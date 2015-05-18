package org.dbpedia.extraction.scripts

import java.io.File
import java.net.URL
import org.dbpedia.extraction.util.{ConfigUtils, WikiInfo, Language}
import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, Codec}

/**
 * Created by Chile on 5/7/2015.
 * copy of WikiInfo extended with additional information
 */
class WikiInfoExt(language: Language, pages: Int, val editsPerUser: Double) extends WikiInfo(language, pages)
object WikiInfoExt
{
  def fromFile(file: File, codec: Codec): Seq[WikiInfoExt] = {
    val source = Source.fromFile(file)(codec)
    try fromSource(source) finally source.close
  }

  def fromURL(url: URL, codec: Codec): Seq[WikiInfoExt] = {
    val source = Source.fromURL(url)(codec)
    try fromSource(source) finally source.close
  }

  def fromSource(source: Source): Seq[WikiInfoExt] = {
    fromLines(source.getLines)
  }

  /**
   * Retrieves a list of all available Wikipedias from a CSV file like http://s23.org/wikistats/wikipedias_csv.php
   *
   */
  def fromLines(lines: Iterator[String]): Seq[WikiInfoExt] = {
    val info = new ArrayBuffer[WikiInfoExt]

    if (! lines.hasNext) throw new Exception("empty file")
    lines.next // skip first line (headers)

    for (line <- lines) if (line.nonEmpty) info += fromLine(line)

    info
  }
  /**
   * Reads a WikiInfo object from a single CSV line.
   */

  def fromLine(line: String): WikiInfoExt = {
    val fields = line.split(",", -1)

    if (fields.length != 15) throw new Exception("expected [15] fields, found ["+fields.length+"] in line ["+line+"]")

    val pages = try fields(5).toInt
    catch { case nfe: NumberFormatException => throw new Exception("expected page count in field with index [5], found line ["+line+"]") }

    val wikiCode = fields(2)
    if (! ConfigUtils.LanguageRegex.pattern.matcher(fields(2)).matches) throw new Exception("expected language code in field with index [2], found line ["+line+"]")

    val editsPerPage = fields(7).toDouble / fields(5).toDouble
    new WikiInfoExt(Language(wikiCode), pages, editsPerPage)
  }
}