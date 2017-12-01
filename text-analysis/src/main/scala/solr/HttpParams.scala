package org.apache.solr.handler.tesserae

object TesseraeCompareParams {
  val TESS = "tess"
  val PREFIX = TESS + "."

  // source query
  val SQ = PREFIX + "sq"

  // source text field
  val SF = PREFIX + "sf"

  // source field list
  val SFL = PREFIX + "sfl"

  // target query
  val TQ = PREFIX + "tq"

  // target text field
  val TF = PREFIX + "tf"

  // target field list
  val TFL = PREFIX + "tfl"

  // max distance
  val MD = PREFIX + "md"

  // score cutoff
  val CUT = PREFIX + "cut"

  // minimum common terms
  val MCT = PREFIX + "mct"

  // distance metric
  val METRIC = PREFIX + "metric"

  // read cache
  val RC = PREFIX + "rc"

  // write cache
  val WC = PREFIX + "wc"

  // stopword count
  val SW = PREFIX + "sw"

  // stopword list
  val SL = PREFIX + "sl"

  // stopword list basis
  val SB = PREFIX + "sb"

  // frequency basis
  val FB = PREFIX + "fb"

  // score basis
  val SCB = PREFIX + "scb"

  // include highlight info?
  val HIGHLIGHT = PREFIX + "highlight"
}
