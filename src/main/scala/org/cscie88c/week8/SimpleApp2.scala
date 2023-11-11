package org.cscie88c.week8

import com.typesafe.scalalogging.LazyLogging
import scala.util.Try
import scala.io.Source

// write case class here

// run with: sbt "runMain org.cscie88c.week8.SimpleApp2"
object SimpleApp2 extends LazyLogging {

  def lineStreamFromFile(fileName: String): Option[LazyList[String]] =
    Try {
      LazyList.from(Source.fromResource(fileName).getLines())
    }.toOption

  def monthLines(month: String)(lines: LazyList[String]): LazyList[String] = ???

  def main(args: Array[String]): Unit = ???
}
