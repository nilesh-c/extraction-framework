package org.dbpedia.extraction.scripts

import java.util.logging.{Level, Logger}
import org.dbpedia.extraction.util._
import java.io._
import org.dbpedia.extraction.destinations.formatters.UriPolicy._
import org.dbpedia.extraction.util.RichFile._
import org.dbpedia.extraction.destinations._
import org.dbpedia.extraction.destinations.formatters.Formatter
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import org.apache.http.client.utils.URIBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.http.NameValuePair
import java.util
import java.net.URL
import scala.io.{Source, Codec}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.language.implicitConversions
import org.dbpedia.extraction.sources.{XMLSource, WikiSource}
import org.dbpedia.extraction.ontology.io.OntologyReader
import org.dbpedia.extraction.wikiparser.Namespace
import org.dbpedia.extraction.ontology.OntologyClass

/**
 * Created by nilesh on 23/4/15.
 */
object FuseDatasets {
  private val logger = Logger.getLogger(FuseDatasets.getClass.getName)
  private var ontologyClasses: Map[String, OntologyClass] = null

  private def split(arg: String): Array[String] = {
    arg.split(",").map(_.trim).filter(_.nonEmpty)
  }

  // URI prefix for wikidata entities
  private val WikidataResource = "http://wikidata.dbpedia.org/resource/Q"

  def main(arg: Array[String]): Unit = {
    val args = "/data/aksw/dbpedia/extraction-framework2/extraction-framework/scripts/extraction.default.properties wikidata wikidata-sameas-sorted .ttl.bz2 mappingbased-properties,labels -normalized-sorted 20150430".split(" ")
    require(args != null && args.length >= 6,
      "need at least six args: " +
        /*0*/ "extraction config file" +
        /*1*/ "prefix of mapping file (eg. wikidata)" +
        /*2*/ "Sorted dataset mapping URIs to wikidata identifiers (eg. wikidata-sameas-sorted)" +
        /*3*/ "mapping file suffix (e.g. '.nt.gz', '.ttl', '.ttl.bz2'), " +
        /*4*/ "comma-separated names of input datasets (e.g. 'infobox-properties,mappingbased-properties'), " +
        /*5*/ "normalized datasets suffix (e.g. '-normalized-sorted'), " +
        /*6*/ "output date in YYYYMMDD format")


    val config = ConfigUtils.loadConfig(args(0), "UTF-8")

    val ontology = {
      val ontologyFile = ConfigUtils.getValue(config, "ontology", false)(new File(_))
      val ontologySource = if (ontologyFile != null && ontologyFile.isFile) {
        XMLSource.fromFile(ontologyFile, Language.Mappings)
      }
      else {
        val namespaces = Set(Namespace.OntologyClass, Namespace.OntologyProperty)
        val url = new URL(Language.Mappings.apiUri)
        WikiSource.fromNamespaces(namespaces, url, Language.Mappings)
      }

      new OntologyReader().read(ontologySource)
    }

    ontologyClasses = ontology.classes.map{
      case (key: String, clazz: OntologyClass) =>
        (clazz.uri, clazz)
    }

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

    val normalizedSuffix = args(5)
    require(normalizedSuffix.nonEmpty, "normalized datasets suffix not given")

    val inputs = split(args(4)).map(_ + normalizedSuffix)
    require(inputs.nonEmpty, "no input datasets")

    val outputDate = args(6)
    require(outputDate.nonEmpty, "no output date")

    val outputPrefix = "data"

    // Suffixes of input/output files, for example ".nt", ".ttl.gz", ".nt.bz2" and so on.
    // This script works with .nt, .ttl, .nq or .tql files, using IRIs or URIs.
    val suffixes = formats.keys.map("." + _)
    require(suffixes.nonEmpty, "no input/output file suffixes")

    val mappingFinder = new DateFinder(baseDir, Language(mappingPrefix)) // Finds wikidata mapping dataset
    val outputFinder = new Finder(baseDir, Language(outputPrefix), "wiki")
    val wikidataResourcePrefix = "http://wikidata.dbpedia.org/resource/Q"
    // Create output directories if they don't exist
    val dateDir = outputFinder.directory(outputDate)
    if(!dateDir.exists()) {
      dateDir.mkdirs()
    }

    val datasets = for(input <- inputs) yield new Dataset(input.replace(normalizedSuffix, ""))
    val destination = createDestination(outputFinder, outputDate, formats, datasets)

    val cache = outputFinder.file(outputDate, "wikidata-resources.obj")
    val wikidataQuadIterator = QuadReader.iterateQuads(mappingFinder, mappingDataset, mappingSuffix, auto = true).iterator

    val dbpediaQuadIterators: Array[(String, EnrichedIterator[Quad])] = for(language <- languages; input <- inputs; suffix <- suffixes) yield {
      val wikiFinder = new DateFinder(baseDir, language) // Finds normalized datasets
      (language.wikiCode, new EnrichedIterator(QuadReader.iterateQuads(wikiFinder, input, suffix, auto = true).iterator.buffered))
    }

    val workers = SimpleWorkers(1.5, 1.0) {
      matchingTriples: Array[(String, Quad)] =>
        for ((predicate, options) <- matchingTriples.groupBy(_._2.predicate)) {
          val (accept, selected, others) = fuse(options)
          if(selected.isEmpty && others.nonEmpty) {
            val context = buildContext(accept, selected, others)("http://data.dbpedia.org/rejected")
            logger.warning("All triples rejected. Dumping context...\n" + context)
          }

          if(selected.nonEmpty || others.nonEmpty) {
            val context = buildContext(accept, selected, others) _
            destination.write(selected.view.map(_._2).distinct.map{
              case quad: Quad =>
                quad.copy(context = context(quad.subject))
            })
          }
        }
    }

    destination.open()
    workers.start()

    var currentResource = ""
    for (wikidataQuad <- wikidataQuadIterator) {
      val resource = if(wikidataQuad == null) null else wikidataQuad.subject
      if (currentResource != resource && null != resource) {
        currentResource = resource

        val matchingTriples = dbpediaQuadIterators.flatMap {
          // iterate over sorted datasets
          case (lang: String, quads: EnrichedIterator[Quad]) =>
            if(quads.it.head == null || quads.it.head.subject != resource)
              Nil
            else
              quads.takeWhileOriginal(_.subject == resource).map((lang, _))
        }

        if(matchingTriples.length > 0) {
          workers.process(matchingTriples.map{
            case (lang: String, quad: Quad) =>
              // hack to replace the -normalized-sorted part from dataset names.
              (lang, quad.copy(dataset = quad.dataset.replace(normalizedSuffix, "")))
          })
        }
      }
    }

    workers.stop()
    destination.close()
  }

  private def loadWikidataResourcesFromCache(cache: File): List[Long] = {
    val inputStream = new ObjectInputStream(new FileInputStream(cache))
    try
    {
      val quads = inputStream.readObject().asInstanceOf[List[Long]]

      logger.info(quads.size + " wikidata resources loaded from cache file "+cache)
      quads
    }
    finally
    {
      inputStream.close()
    }
  }

  // enrich wrapper to give original functionality
  class EnrichedIterator[T](val it: BufferedIterator[T]) {
    def takeWhileOriginal(p: T=>Boolean) = {
      val self = it
      new Iterator[T] {
        def hasNext = { self.hasNext && p(self.head) }
        def next() = (if (hasNext) self else Iterator.empty).next()
      }
    }
  }
  implicit def enrichIterator[T](it: BufferedIterator[T]) = new EnrichedIterator(it)

  private def buildContext(howMany: AcceptCount, accepted: Array[(String, Quad)], others: Array[(String, Quad)])(resourceUri: String) : String = {
    val nameValuePairs = new util.ArrayList[NameValuePair]()
    nameValuePairs.add(new BasicNameValuePair("accept",
      howMany match {
        case AcceptAll() => ""
        case Accept(count) => count.toString
      }))

    val acceptedString = jsonMapper.writeValueAsString(accepted.map{
      case (lang: String, quad: Quad) =>
        ContextAccepted(lang, quad.value, quad.datatype, quad.context, quad.dataset)
    })
    nameValuePairs.add(new BasicNameValuePair("accepted", acceptedString))

    val otherString = jsonMapper.writeValueAsString(others.map{
      case (lang: String, quad: Quad) =>
        ContextOther(lang, quad.value, quad.datatype, quad.context, quad.dataset)
    })
    nameValuePairs.add(new BasicNameValuePair("others", otherString))

    val builder = new URIBuilder(resourceUri).setParameters(nameValuePairs)
    builder.toString
  }

  private class AcceptCount()
  private case class Accept(count: Int) extends AcceptCount
  private case class AcceptAll() extends AcceptCount

  val wikiScores = WikiInfoExt.fromURL(WikiInfo.URL, Codec.UTF8).map(x => (x.language.wikiCode, x.editsPerUser)).toMap

  /**
   * Dummy fusion function that selects the first English triple if present, else the first one from the whole list
   * @param options Array[(language code, quad)] - list of all available quads for a particular resource and predicate.
   * @return Tuple2 of list of selected quads and list of the rest of the quads (to keep in context)
   */
  private def fuse(options: Array[(String, Quad)]): (AcceptCount, Array[(String, Quad)], Array[(String, Quad)]) = {
    val isLangString = options.head._2.datatype == "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
    val isRdfType = options.head._2.predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    val isAcceptAll = acceptAllProperties.contains(options.head._2.predicate)
    val isFunctional = functionalProperties.contains(options.head._2.predicate)

    if(isFunctional && !isLangString) {
      // If property is functional and it's not a langString, use voting
      val scores = options.groupBy(x => x._2.value).mapValues {
        case quads: Array[(String, Quad)] =>
          (quads, quads.foldLeft(0.0){
            case (score: Double, quad: (String, Quad)) => score + wikiScores(quad._1)
          }) // accumulate total score/vote for each value
      }

      val selected = scores.maxBy {
        case (value: String, quadsAndScores: (Array[(String, Quad)], Double)) =>
          quadsAndScores._2
      }

      val selectedQuads = selected._2._1.take(1)

      val otherQuads = scores.filterNot(_._1 == selected._1).flatMap {
        case (value: String, quadsAndScores: (Array[(String, Quad)], Double)) =>
          quadsAndScores._1
      }

      (Accept(selectedQuads.length), selectedQuads, otherQuads.toArray)
    } else if (isAcceptAll || isLangString) {
      // If it's a langString, accept all
      (AcceptAll(), options, Array())
    } else if (isRdfType) {
      val (selected, others) = chooseByType(options)
      (Accept(selected.length), selected, others)
    } else {
      // For the rest and non-functional properties, choose the median language (n/2'th element of sorted array).
      val languageToQuads = options.groupBy(_._1)
      val quadsPerLanguage = languageToQuads.mapValues(_.length).toArray.sortBy(_._2)
      val medianLanguage = quadsPerLanguage(quadsPerLanguage.length / 2)._1

      val (selectedQuads, otherQuads) = options.partition(_._1 == medianLanguage)
      (Accept(selectedQuads.length), selectedQuads, otherQuads)
    }
  }

  def chooseByType(options: Array[(String, Quad)]): (Array[(String, Quad)], Array[(String, Quad)]) = {
    try {
      val types = options.map{
        case (wikiCode: String, quad: Quad) =>
          if(!quad.value.startsWith("http://dbpedia.org/ontology"))
            throw NonDbpediaTypeEncountered(wikiCode, quad.dataset, quad.value)
          (wikiCode, quad, ontologyClasses(quad.value))
      } // map to language, quad, class
      val typeScores = types.map{
          case (wikiCode: String, _, ontologyClass: OntologyClass) =>
            (wikiCode, ontologyClass, types.view.map(x => if(ontologyClass.relatedClasses.contains(x._2)) 1 else 0).sum)
        }.sortBy(_._3) // more specific class gets more score

      val selectedType = typeScores.head._2 // choose the type with the best score (choose the top-most one if it's a tie)
      val (selected, others) = types.partition(_._3.equals(selectedType)) // select quads (each from a different language) with that type
      (selected.map(x => (x._1, x._2)), others.map(x => (x._1, x._2)))
    } catch {
      case NonDbpediaTypeEncountered(wikiCode, dataset, typeUri) =>
        logger.warning("%swiki: encountered non-DBpedia type URI in %s: %s".format(wikiCode, dataset, typeUri))
        (Array(), Array())
      case ex: Exception =>
        logger.info("Exception occured while fusing types: " + ex)
        (Array(), Array())
    }
  }

  val acceptAllProperties = Set("http://dbpedia.org/ontology/wikiPageWikiLink",
    "http://www.w3.org/2004/02/skos/core#broader",
    "http://www.w3.org/2004/02/skos/core#prefLabel",
    "http://www.w3.org/2004/02/skos/core#related",
    "http://purl.org/dc/terms/subject",
    "http://dbpedia.org/ontology/wikiPageExternalLink",
    "http://dbpedia.org/ontology/wikiPageRedirects",
    "http://dbpedia.org/ontology/wikiPageDisambiguates")

  val functionalProperties = Set("http://dbpedia.org/ontology/weight",
    "http://dbpedia.org/ontology/acceleration",
    "http://dbpedia.org/ontology/populationTotal",
    "http://dbpedia.org/ontology/wheelbase",
    "http://dbpedia.org/ontology/co2Emission",
    "http://dbpedia.org/ontology/retirementDate",
    "http://dbpedia.org/ontology/averageAnnualGeneration",
    "http://dbpedia.org/ontology/height",
    "http://dbpedia.org/ontology/topSpeed",
    "http://dbpedia.org/ontology/birthYear",
    "http://dbpedia.org/ontology/restingDate",
    "http://dbpedia.org/ontology/zipCode",
    "http://dbpedia.org/ontology/deathDate",
    "http://dbpedia.org/ontology/fuelCapacity",
    "http://dbpedia.org/ontology/latestReleaseDate",
    "http://dbpedia.org/ontology/netIncome",
    "http://dbpedia.org/ontology/deathYear",
    "http://dbpedia.org/ontology/birthDate",
    "http://dbpedia.org/ontology/installedCapacity",
    "http://dbpedia.org/ontology/foalDate",
    "http://dbpedia.org/ontology/redline",
    "http://dbpedia.org/ontology/diameter",
    "http://dbpedia.org/ontology/length",
    "http://dbpedia.org/ontology/operatingIncome",
    "http://dbpedia.org/ontology/torqueOutput",
    "http://dbpedia.org/ontology/width",
    "http://dbpedia.org/ontology/marketCapitalisation",
    "http://dbpedia.org/ontology/fuelConsumption",
    "http://dbpedia.org/ontology/displacement",
    "http://dbpedia.org/ontology/powerOutput",
    "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
    "http://www.w3.org/2003/01/geo/wgs84_pos#long",
    "http://www.georss.org/georss/point",
    "http://dbpedia.org/ontology/iso31661Code",
    "http://dbpedia.org/ontology/iso6391Code",
    "http://dbpedia.org/ontology/iso6392Code",
    "http://dbpedia.org/ontology/iso6393Code",
    "http://dbpedia.org/ontology/totalPopulation",
    "http://xmlns.com/foaf/0.1/homepage")

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

  val jsonMapper = new ObjectMapper() with ScalaObjectMapper
  jsonMapper.registerModule(DefaultScalaModule)
  jsonMapper.registerSubtypes(classOf[ContextOther])
  jsonMapper.registerSubtypes(classOf[ContextAccepted])
}

class Context
case class ContextOther(language: String, value: String, datatype: String, context: String, dataset: String) extends Context
case class ContextAccepted(language: String, value: String, datatype: String, context: String, dataset: String) extends Context
case class NonDbpediaTypeEncountered(wikiCode: String, dataset: String, typeUri: String) extends Exception