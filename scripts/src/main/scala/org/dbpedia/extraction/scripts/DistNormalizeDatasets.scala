package org.dbpedia.extraction.scripts

import java.io.{File, Writer}
import java.net.URL
import java.util.logging.Logger

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.destinations.formatters.Formatter
import org.dbpedia.extraction.destinations.formatters.UriPolicy._
import org.dbpedia.extraction.util.RichFile._
import org.dbpedia.extraction.util._

import scala.collection.mutable.ArrayBuffer


/**
 * Normalize all triples into the new namespace using wikidata identifiers as base using
 * sameAs links between different projects.
 *
 * - read one or more triple files that contain the URI mapping (wikidata sameAs triples):
 * - the predicate is ignored (only read owl:sameAs links and ignore the rest?)
 * - read one or more files that need to be normalized into the new namespace:
 * - the predicate is ignored
 *
 * Example call:
 * ../run NormalizeDatasets /data/dbpedia wikidata wikidata-sameas .ttl.bz2 mappingbased-properties,page-links,... -normalized
 *
 * The output name extension for triples, that are rejected because the subject does not exist, becomes
 * \-normalized-subject-rejected, * and similarly for triples with non-existant objects it becomes -normalized-object-rejected,
 * where -normalized is the provided * output extension. Triples that have both subject and object missing from the mapping
 * are sent to the extension -normalized-rejected. No changes are made to rejected triples.
 *
 * @author Nilesh Chakraborty <nilesh@nileshc.com>
 */
object DistNormalizeDatasets {
  private val logger = Logger.getLogger(classOf[NormalizationJob].getName)

  private def split(arg: String): Array[String] = {
    arg.split(",").map(_.trim).filter(_.nonEmpty)
  }

  // URI prefix for wikidata entities
  private val WikidataResource = "http://wikidata.dbpedia.org/resource/Q"

  def main(args: Array[String]): Unit = {
    require(args != null && args.length >= 6,
      "need at least six args: " +
        /*0*/ "extraction config file" +
        /*1*/ "prefix of mapping file (eg. wikidata)" +
        /*2*/ "comma-separated names of datasets mapping URIs to wikidata identifiers (eg. wikidata-sameas)" +
        /*3*/ "map  ping file suffix (e.g. '.nt.gz', '.ttl', '.ttl.bz2'), " +
        /*4*/ "comma-separated names of input datasets (e.g. 'mappingbased-properties,page-links'), " +
        /*5*/ "output dataset name extension (e.g. '-normalized'), ")

    val sc = getSparkContext

    val config = ConfigUtils.loadConfig(args(0), "UTF-8")

    val baseDir = ConfigUtils.getValue(config, "base-dir", true)(new File(_))
    if (!baseDir.exists)
      throw new IllegalArgumentException("dir " + baseDir + " does not exist")
    val langConfString = ConfigUtils.getString(config, "languages", false)
    val languages = ConfigUtils.parseLanguages(baseDir, Seq(langConfString))
    require(languages.nonEmpty, "no languages")

    val formats = parseFormats(config, "uri-policy", "format")

    val mappingPrefix = args(1)

    val mappings = split(args(2))
    require(mappings.nonEmpty, "no mapping datasets")

    // Suffix of mapping files, for example ".nt", ".ttl.gz", ".nt.bz2" and so on.
    // This script works with .nt, .ttl, .nq or .tql files, using IRIs or URIs.
    val mappingSuffix = args(3)
    require(mappingSuffix.nonEmpty, "no mapping file suffix")

    val inputs = split(args(4)).map(_.replace("_", "-"))
    require(inputs.nonEmpty, "no input datasets")

    val outputExtension = args(5)
    require(outputExtension.nonEmpty, "no output name extension")

    val extensions = List(outputExtension, outputExtension + "-subject-rejected", outputExtension + "-object-rejected", outputExtension + "-rejected")

    // Suffixes of input/output files, for example ".nt", ".ttl.gz", ".nt.bz2" and so on.
    // This script works with .nt, .ttl, .nq or .tql files, using IRIs or URIs.
    val suffixes = formats.keys.map("." + _)
    require(suffixes.nonEmpty, "no input/output file suffixes")

    val mappingFinder = new DateFinder(baseDir, Language(mappingPrefix)) // Finds wikidata mapping dataset

    val mappingsRDD: RDD[(String, String)] = (for (mapping <- mappings)
      yield sc.textFile(mappingFinder.find(mapping + mappingSuffix, auto = true).getAbsolutePath))
      .reduce(_ union _)
      .map {
      case Quad(quad) =>
        (quad.value, quad.subject)
      }.cache()

    val inputOutputs = for(language <- languages) yield {
      val wikiFinder = new Finder(baseDir, language, "wiki") // Finds Wikipedia extracted datasets
      val date = wikiFinder.dates().last
      val datasets = for (input <- inputs; extension <- extensions) yield new Dataset(input + extension)
      val destination = createDestination(wikiFinder, date, formats, datasets)

      val files = for (input <- inputs; suffix <- suffixes) yield {
        val date = wikiFinder.dates().last

        // Check for -correct version first, then -redirected, if not present, fall back to original name
        val inFile = {
          val correct = wikiFinder.file(date, input + "-correct" + suffix)
          if (correct.exists) {
            correct
          }
          else {
            val redirected = wikiFinder.file(date, input + "-redirected" + suffix)
            if (redirected.exists) {
              redirected
            } else {
              wikiFinder.file(date, input + suffix)
            }
          }
        }

        inFile.getAbsolutePath
      }

      (language, destination, files)
    }

    val destinations = inputOutputs.map(x => (x._1.isoCode, x._2)).toMap

    val subjectGroupedQuads: RDD[(String, (Quad, String))] = inputOutputs.map {
      case (language, destination, files) =>
      val langBC = sc.broadcast(language.isoCode)

      val subjectGroupedQuads: RDD[(String, (Quad, String))] = (for (file <- files)
        yield sc.textFile(file.getAbsolutePath))
        .reduce(_ union _)
        .map {
          case Quad(quad) =>
            (quad.subject, (quad, langBC.value))
        }

      subjectGroupedQuads
    }.reduce(_ union _)

    val joinedBySubject = subjectGroupedQuads.leftOuterJoin(mappingsRDD)
    val subjectRejectedQuads: RDD[(Quad, String)] = joinedBySubject.filter(_._2._2.isEmpty).map(_._2._1)
    val subjectMappedQuads: RDD[(Quad, String)] = joinedBySubject.filter(_._2._2.isDefined).map{
      case (subject: String, triple: ((Quad, String), Option[String])) =>
        (triple._1._1.copy(subject = triple._2.get), triple._1._2)
    }


    val objectGroupedQuads = subjectGroupedQuads.map{
      case (subject: String, triple: (Quad, String)) =>
        (triple._1.value, triple)
    }
    val literalNonDBpediaTriples = objectGroupedQuads.filter{
      case (objectt: String, triple: (Quad, String)) =>
        triple._1.datatype != null || !new URL(objectt).getHost.contains("dbpedia.org")
    }
    val joinedBySubjectObject = objectGroupedQuads.filter{
      case (objectt: String, triple: (Quad, String)) =>
        !(triple._1.datatype != null || !new URL(objectt).getHost.contains("dbpedia.org"))
    }.leftOuterJoin(mappingsRDD)
    val objectRejectedQuads: RDD[(Quad, String)] = joinedBySubjectObject.filter(_._2._2.isEmpty).map(_._2._1)
    val subjectObjectMappedQuads: RDD[(Quad, String)] = joinedBySubjectObject.filter(_._2._2.isDefined).map{
      case (objectt: String, triple: ((Quad, String), Option[String])) =>
        (triple._1._1.copy(value = triple._2.get), triple._1._2)
    }


    subjectObjectMappedQuads.foreachPartition{
      case allLangs: Iterator[(Quad, String)] =>
        for((lang, quads) <- allLangs.toSeq.groupBy(_._2)) {
          destinations(lang).write(quads.map(_._1))
        }
    }

  }

  private def createDestination[T <% FileLike[T]](finder: Finder[T], date: String, formats: scala.collection.Map[String, Formatter], datasets: Seq[Dataset]) : Destination = {
    val destination = new ArrayBuffer[Destination]()
    for ((suffix, format) <- formats) {
      val datasetDestinations = new scala.collection.mutable.HashMap[String, Destination]()
      for (dataset <- datasets) {
        val file = finder.file(date, dataset.name.replace('_', '-')+'.'+suffix)
        val sortedFile = finder.file(date, dataset.name.replace('_', '-')+"-sorted."+suffix)
        datasetDestinations(dataset.name) = new WriterDestination(writer(file), format)
      }

      destination += new DatasetDestination(datasetDestinations)
    }
    new CompositeDestination(destination.toSeq: _*)
  }

  private def writer[T <% FileLike[T]](file: T): () => Writer = {
    () => IOUtils.writer(file)
  }

  def getSparkContext: SparkContext
}
