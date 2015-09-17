package com.socrata.geoexport.encoders

import java.io.{OutputStream, OutputStreamWriter, Writer}

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.socrata.geoexport.encoders.KMLMapper._
import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.slf4j.LoggerFactory
import com.socrata.geoexport.intermediates._
import com.socrata.geoexport.intermediates.kml._

import scala.language.implicitConversions
import scala.xml.{Node, XML}
import com.rojoma.simplearm.util._
import scala.util.{Try, Success, Failure}
import scala.language.existentials


// Convert to XML nodes: https://developers.google.com/kml/documentation/kmlreference
// functions in here should take a value or seq of values and return a single Node
object KMLMapper {
 lazy val log = LoggerFactory.getLogger(getClass)

  case class MultipleGeometriesFoundException(message: String) extends Exception
  case class UnknownGeometryException(message: String) extends Exception
  type Attributes = Seq[AttributeDescriptor]
  type Layers = Iterable[SoQLPackIterator]

  def genKML(layers: Layers, writer: OutputStreamWriter): Unit = {
    kml(layers, writer)
  }

  val defaultStyle = <Style id="defaultStyle">
      <LineStyle>
        <width>1.5</width>
      </LineStyle>
      <PolyStyle>
        <color>7dff0000</color>
      </PolyStyle>
    </Style>


  private def kml(layers: Layers, writer: OutputStreamWriter): Unit = {

    writer.write("""<?xml version='1.0' encoding='UTF-8'?>
      |<kml xmlns:kml="http://earth.google.com/kml/2.2">
      |  <Document id="featureCollection">""".stripMargin)
    // no scalastyle. how about avoid using null in the scala XML API
    // scalastyle:off
    XML.write(writer, defaultStyle, "UTF-8", false, null)
    // scalastyle:on
    layers.foreach(kml(_, writer))
    writer.write("""  </Document>
    </kml>""".stripMargin)
  }

  private def kml(collection: SoQLPackIterator, writer: OutputStreamWriter): Unit = {
    writer.write("<Folder>")
    collection.foreach { feature: Array[SoQLValue] =>
      val featureXML = kml(collection.schema, feature)
      // scalastyle:off
      XML.write(writer, featureXML, "UTF-8", false, null)
      // scalastyle:on
    }
    writer.write("</Folder>")
  }

  private def kml(schema: Seq[(String, SoQLType)], fields: Array[_ <: SoQLValue]): Node = {

    val (geoms, attrs) = schema
      .zip(fields)
      .map { case (column, value) => (value, ShapeRep.repFor(column, KMLRepMapper)) }
      .partition { case (_value, intermediate) => intermediate.isGeometry }

    val geomAttr = geoms match {
      case Seq(g) => g
      case _ => throw new MultipleGeometriesFoundException("Too many geometry columns!")
    }

    <Placemark>
      <styleUrl>#defaultStyle</styleUrl>
      <ExtendedData>
        <SchemaData>
          { attrs.flatMap(KMLRepMapper.toAttr(_)) }
        </SchemaData>
      </ExtendedData>
      { KMLRepMapper.toAttr(geomAttr).head }
    </Placemark>
  }
}

object KMLEncoder extends GeoEncoder {
  def encode(layers: Layers, outStream: OutputStream) : Try[OutputStream] = {
    val writer = new OutputStreamWriter(outStream)
    try {
      KMLMapper.genKML(layers, writer)
      Success(outStream)
    } catch {
      case e: Exception => Failure(e)
    } finally {
      writer.close()
    }
  }

  def encodes: Set[String] = Set("kml")
  def encodedMIME: String  = "application/vnd.google-earth.kml+xml"
}


