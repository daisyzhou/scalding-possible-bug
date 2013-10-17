package com.wibidata.testing.express

import com.twitter.scalding.{Tsv, TextLine, Args, Job}
import java.text.{ParseException, SimpleDateFormat}

class WorkingJob(args: Args) extends Job(args) {
  val output = args("output")
  val input = args("input")

   val pipe = TextLine(input)
    .map('line -> ('orderId, 'orderItem)) { parseLine }
    .write(Tsv(output))

  /**
   * Parses a line of input
   *
   * @param line Line of text to parse.
   * @return stuff.
   */
  def parseLine(line: String): (String, Tuple3[String, String, String]) = {
    val fields = splitAndTrim(line, " ")
    val orderId = fields(0)
    val item = (fields(1), fields(2), fields(3))
    (orderId, item)
  }

  /**
   * Splits a String representing a line of text in an input file and trims each resulting field.
   * @param line Line of text from input file
   * @param delimiter The delimiter used to split the line into fields (default is tab)
   * @return Array of Strings representing each field in the input.
   */
  def splitAndTrim(line: String, delimiter: String = "\t"): Array[String] = {
    line.split(delimiter).map { _.trim }
  }
}

class BrokenJob(args: Args) extends Job(args) {
  val output = args("output")
  val input = args("input")

   val pipe = TextLine(input)
    .mapTo('line -> ('orderId, 'orderItem)) { parseLine }
    .write(Tsv(output))

  /**
   * Parses a line of input
   *
   * @param line Line of text to parse.
   * @return stuff.
   */
  def parseLine(line: String): (String, Tuple3[String, String, String]) = {
    val fields = splitAndTrim(line, " ")
    val orderId = fields(0)
    val item = (fields(1), fields(2), fields(3))
    (orderId, item)
  }

  /**
   * Splits a String representing a line of text in an input file and trims each resulting field.
   * @param line Line of text from input file
   * @param delimiter The delimiter used to split the line into fields (default is tab)
   * @return Array of Strings representing each field in the input.
   */
  def splitAndTrim(line: String, delimiter: String = "\t"): Array[String] = {
    line.split(delimiter).map { _.trim }
  }
}
