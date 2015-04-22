package org.dbpedia.extraction.destinations

import org.dbpedia.extraction.util.FileLike
import sys.process._
import java.util.logging.Logger
import java.io.File

/**
 * Created by nilesh on 22/4/15.
 */
class SortedDestination(destination: Destination, originalFile: FileLike[_], sortedFile: FileLike[_])
  extends WrapperDestination(destination) {
  private val logger = Logger.getLogger(classOf[SortedDestination].getName)

  override def close(): Unit = {
    super.close()
    logger.info(s"Sorting $originalFile into $sortedFile...")
    (s"bzcat $originalFile" #| "sort" #| "bzip2" #> new File(sortedFile.toString)) #||
    (s"zcat $originalFile" #| "sort" #| "gzip" #> new File(sortedFile.toString)) #||
    (s"cat $originalFile" #| "sort" #> new File(sortedFile.toString)) !!
  }
}
