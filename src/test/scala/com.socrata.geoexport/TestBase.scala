package com.socrata.geoexport

import java.io._

import com.socrata.geoexport.conversions.Converter
import com.socrata.geoexport.encoders.KMLEncoder
import com.socrata.soql.SoQLPackWriter
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.ByteArrayOutputStream
import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.velvia.MsgPack

import scala.xml.{NodeSeq, XML, Node}

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

  protected def removeIds(kml: ByteArrayOutputStream): String = {
    "id=\"fid-[a-zA-Z0-9\\-_]+\"".r replaceAllIn(kml.toString("UTF-8"), "none")
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

  protected def placemarkSchemaNames(node: NodeSeq) = {
    (node \ "ExtendedData" \ "SchemaData" \ "SimpleData").map {
      n => (n \ "@name").toString
    }.toSet
  }

  protected def placemarkPolyOuter(placemark: NodeSeq): Seq[Seq[Seq[Double]]] = {
    (placemark \ "Polygon" \ "outerBoundaryIs" \  "LinearRing" \ "coordinates").map {
      n => pluckCoordinates(n.text)
    }.toList
  }

  protected def pluckSimpleData(node: Node, name: String) = placemarkSimpleData(pluckPlacemark(node), name)
  protected def pluckSchemaNames(node: Node) = placemarkSchemaNames(pluckPlacemark(node))
  protected def pluckPolyOuter(node: Node) = placemarkPolyOuter(node \ "Placemark")

  protected def pluckCoordinates(text: String): Seq[Seq[Double]] = {
    text.trim.split("\\s").toList.map { c => c.trim.split(",").toList.map(_.toDouble) }
  }

  protected def convert(layers: List[InputStream]): Node = {
    val outStream = new ByteArrayOutputStream()
    val result = Converter.execute(layers, List(), new KMLEncoder(), outStream) match {
      case Right(outstream) =>
        outStream.flush()
        outStream.toString("UTF-8")
      case Left(err) => err
    }
    // println(result)
    XML.loadString(result)
  }

  protected def print(node: Node): Unit = {
    val printer = new scala.xml.PrettyPrinter(80, 2)
    println(printer.format(node))
  }

  protected def compareAttrs(actual: Option[Node], expected: Option[Node]): Unit = {
    (actual, expected) match {
      case (Some(actualAttr), Some(expectedAttr)) => actualAttr must be(expectedAttr)
      case _ => true must be(false)
    }
  }

  protected def compareNodeTextIgnoreSpaces(actual: Option[Node], expected: Option[Node]): Unit = {
    (actual, expected) match {
      case (Some(actualAttr), Some(expectedAttr)) =>
        actualAttr.text.replaceAll("\\s", "") must be(expectedAttr.text.replaceAll("\\s", ""))
      case _ => true must be(false)
    }
  }


  protected def compareSimpleDatums(actual: Node, expected: Node, attName: String): Unit = {
    compareAttrs(pluckSimpleData(actual, attName), pluckSimpleData(expected, attName))
  }

}
