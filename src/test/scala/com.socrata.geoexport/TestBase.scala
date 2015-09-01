package com.socrata.geoexport

import java.io._

import com.socrata.soql.SoQLPackWriter
import com.socrata.soql.types.{SoQLValue, SoQLType}
import scala.xml.{NodeSeq, XML, Node}
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.io.IOUtils
import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.velvia.MsgPack
import scala.util.{Try, Success, Failure}

trait TestBase
    extends FunSuite
    with org.scalatest.MustMatchers
    with PropertyChecks
    with BeforeAndAfterAll {

  def wkt(w: String): Geometry = {
    val pm = new PrecisionModel(PrecisionModel.FIXED)
    val fact: GeometryFactory = new GeometryFactory(pm)
    val wktRdr: WKTReader = new WKTReader(fact)
    wktRdr.read(w)
  }

  def fixture(name: String): InputStream = {
    getClass.getResourceAsStream(s"/fixtures/${name}.json")
  }

  def kmlFixture(name: String): Node = {
    val path = s"/fixtures/${name}"
    XML.load(getClass.getResourceAsStream(s"${path}.kml"))
  }

  def fixtures(name: String): (InputStream, Node) = {
    val path = s"/fixtures/${name}"
    val js = getClass.getResourceAsStream(s"${path}.json")
    (js, kmlFixture(name))
  }

  def pack(header: Seq[(String, SoQLType)], rows: Seq[Array[SoQLValue]]): DataInputStream = {
    val is = new PipedInputStream(2048)
    val os = new PipedOutputStream(is)
    val dis = new DataInputStream(is)

    val writer = new SoQLPackWriter(header)
    writer.write(os, rows.iterator)
    dis
  }

  protected def pluckPlacemark(node: Node) = node \ "Document" \ "Folder" \ "Placemark"

  protected def placemarkSimpleData(node: NodeSeq, name: String) = {
    (node \ "ExtendedData" \ "SchemaData" \ "SimpleData").find { datum =>
      datum.attribute("name") match {
        case Some(attName) => attName.toString.equals(name)
        case _ => false
      }
    }
  }

  protected def placemarkPolyOuter(placemark: NodeSeq): Seq[Seq[Seq[Double]]] = {
    (placemark \ "Polygon" \ "outerBoundaryIs" \  "LinearRing" \ "coordinates").map {
      n => pluckCoordinates(n.text)
    }.toList
  }

  protected def pluckPolyOuter(node: Node) = placemarkPolyOuter(node \ "Placemark")

  protected def pluckCoordinates(text: String): Seq[Seq[Double]] = {
    text.trim.split("\\s").toList.map { c => c.trim.split(",").toList.map(_.toDouble) }
  }

  protected def print(node: Node): Unit = {
    val printer = new scala.xml.PrettyPrinter(80, 2)
    println(printer.format(node))
  }



}
