package org.dbpedia.extraction.scripts

import java.util.logging.Logger
import org.dbpedia.extraction.util._
import java.io.{Writer, File}
import org.dbpedia.extraction.destinations.formatters.UriPolicy._
import org.dbpedia.extraction.util.RichFile._
import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.destinations.formatters.Formatter
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
 * Created by nilesh on 23/4/15.
 */
object FuseDatasets {
  private val logger = Logger.getLogger(FuseDatasets.getClass.getName)

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
        /*2*/ "Sorted dataset mapping URIs to wikidata identifiers (eg. wikidata-sameas-sorted)" +
        /*3*/ "mapping file suffix (e.g. '.nt.gz', '.ttl', '.ttl.bz2'), " +
        /*4*/ "comma-separated names of input datasets (e.g. 'infobox-properties-normalized-sorted,mappingbased-properties-normalized-sorted'), " +
        /*5*/ "output date in YYYYMMDD format")


    val config = ConfigUtils.loadConfig(args(0), "UTF-8")

    val baseDir = ConfigUtils.getValue(config, "base-dir", true)(new File(_))
    if (!baseDir.exists)
      throw new IllegalArgumentException("dir " + baseDir + " does not exist")
    val langConfString = ConfigUtils.getString(config, "languages", false)
    val languages = ConfigUtils.parseLanguages(baseDir, Seq(langConfString))
    require(languages.nonEmpty, "no languages")

    val formats = parseFormats(config, "uri-policy", "format")

    val mappingPrefix = args(1)

    val mappingDataset = args(2)
    require(mappingDataset.nonEmpty, "no mapping dataset")

    // Suffix of mapping files, for example ".nt", ".ttl.gz", ".nt.bz2" and so on.
    // This script works with .nt, .ttl, .nq or .tql files, using IRIs or URIs.
    val mappingSuffix = args(3)
    require(mappingSuffix.nonEmpty, "no mapping file suffix")

    val inputs = split(args(4))
    require(inputs.nonEmpty, "no input datasets")

    val outputDate = args(5)

    val outputPrefix = "data"

    // Suffixes of input/output files, for example ".nt", ".ttl.gz", ".nt.bz2" and so on.
    // This script works with .nt, .ttl, .nq or .tql files, using IRIs or URIs.
    val suffixes = formats.keys.map("." + _)
    require(suffixes.nonEmpty, "no input/output file suffixes")

    val mappingFinder = new DateFinder(baseDir, Language(mappingPrefix)) // Finds wikidata mapping dataset
    val outputFinder = new Finder(baseDir, Language(outputPrefix), "wiki")

    // Create output directories if they don't exist
    val dateDir = outputFinder.directory(outputDate)
    if(!dateDir.exists()) {
      dateDir.mkdirs()
    }

    val datasets = for(input <- inputs) yield new Dataset(input)
    val destination = createDestination(outputFinder, outputDate, formats, datasets)

    for (input <- inputs; suffix <- suffixes) {
      val wikidataQuadIterator = QuadReader.iterateQuads(mappingFinder, mappingDataset + mappingSuffix, auto = true).iterator.buffered
      val dbpediaQuadIterators: Array[(String, BufferedIterator[Quad])] = for(language <- languages) yield {
        val wikiFinder = new DateFinder(baseDir, language) // Finds normalized datasets
        (language.wikiCode, QuadReader.iterateQuads(wikiFinder, input + suffix, auto = true).iterator.buffered)
      }

      val iter = wikidataQuadIterator.toIterator.sliding(2).filterNot{case x :: y :: l => x == y}
      val distinctWikidataQuads = (Iterator(List(null, iter.next()(0))) ++ iter).map{case x :: y :: l => y}
      for (wikidataQuad <- distinctWikidataQuads) {
        val resource = wikidataQuad.subject

        val matchingTriples = dbpediaQuadIterators.flatMap {
          case (lang: String, quads: BufferedIterator[Quad]) =>
            quads.takeWhile(_.subject == resource).map((lang, _))
        }

        for((predicate, options) <- matchingTriples.groupBy(_._2.predicate)) {
          destination.write(Seq())
        }

      }


    }
  }

  private def fuse(options: Array[(String, Quad)]): Array[(String, Quad)] = {
    // Dummy fusion function that selects the English triple if present, else the first one from the list
    val selectedQuad = options.find(_._1 == "en") match {
      case Some(quad) => quad
      case _ => options.head
    }

    
  }

  private def createDestination[T <% FileLike[T]](finder: Finder[T], date: String, formats: scala.collection.Map[String, Formatter], datasets: Seq[Dataset]) : Destination = {
    val destination = new ArrayBuffer[Destination]()
    for ((suffix, format) <- formats) {
      val datasetDestinations = new mutable.HashMap[String, Destination]()
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
}