package com.socrata.geoexport.encoders

import java.io.{OutputStream, OutputStreamWriter, Writer}

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.socrata.geoexport.encoders.KMLMapper._
import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.xml.{Node, XML}
import com.rojoma.simplearm.util._
import scala.util.{Try, Success, Failure}

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
    // no scalastyle. how about avoid using null scala XML
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

  private def isGeometry(kind: SoQLType) = {
    List(SoQLPoint, SoQLLine, SoQLPolygon, SoQLMultiPoint, SoQLMultiLine, SoQLMultiPolygon).contains(kind)
  }

  private def kml(schema: Seq[(String, SoQLType)], fields: Array[SoQLValue]): Node = {

    val (geoms, attrs) = schema
      .zip(fields)
      .map { case ((name, kind), value) => (name, kind, value) }
      .partition { case (_name, kind, _value) => isGeometry(kind)}

    val geom = geoms match {
      case Seq(g) => g
      case _ => throw new MultipleGeometriesFoundException("Too many geometry columns!")
    }

    <Placemark>
      <styleUrl>#defaultStyle</styleUrl>
      <ExtendedData>
        <SchemaData>
          {attrs.map(kml(_))}
        </SchemaData>
      </ExtendedData>
      { geomToKML(geom) }
    </Placemark>
  }

  private def geomToKML(simpleData: (String, SoQLType, SoQLValue)): Node = {
    val (_name, _kind, value) = simpleData
    value match {
      // ugh this is goofy..have to force everything into its canonical type
      case v: SoQLPoint => kml(v.value)
      case l: SoQLLine => kml(l.value)
      case p: SoQLPolygon => kml(p.value)
      case mp: SoQLMultiPoint => kml(mp.value)
      case ml: SoQLMultiLine => kml(ml.value)
      case mp: SoQLMultiPolygon => kml(mp.value)
      case other: Any => throw UnknownGeometryException(s"${other} is not a serializable geometry")
    }
  }

  // scalastyle:off
  private def kml(simpleData: (String, SoQLType, Any)): Node = {
    val (name, kind, value) = simpleData
    val xmlVal = value match {
      case SoQLText(t) => t
      case SoQLDouble(d) => d
      case SoQLNumber(n) => n
      case SoQLMoney(m) => m
      case SoQLBoolean(b) => b
      case SoQLFixedTimestamp(ts) => SoQLFixedTimestamp.StringRep(ts)
      case SoQLFloatingTimestamp(fts) => SoQLFloatingTimestamp.StringRep(fts)
      case SoQLTime(dt) => SoQLTime.StringRep(dt)
      case SoQLDate(d) => SoQLDate.StringRep(d)
      case other: Any => other.toString
    }
    <SimpleData name={name}>{xmlVal}</SimpleData>
  }
  // scalastyle:on

  // Shape to KML transforms
  private def kml(coordinates: Array[Coordinate]): Node = {
    def toStr(c: Coordinate): String = {
      (c.x, c.y, c.z) match {
        case (x, y, z) if z.isNaN => s"${x},${y}"
        case (x, y, z) => s"${x},${y},${z}"
        case _ => ""
      }
    }
    <coordinates>
      {coordinates.map(toStr(_)).mkString(" \n")}
    </coordinates>
  }

  private def kml(point: Point): Node = <Point>{kml(point.getCoordinates)}</Point>
  private def kml(line: LineString): Node = <LineString>{kml(line.getCoordinates)}</LineString>


  private def kml(polygon: Polygon): Node = {
    <Polygon>
      <outerBoundaryIs>
        <LinearRing>{kml(polygon.getExteriorRing.getCoordinates)}</LinearRing>
      </outerBoundaryIs>
      {
        polygon.getNumInteriorRing match {
          case 0 => ""
          case count: Int =>
            <innerBoundaryIs>
              {
                Range(0, count).map { i =>
                  <LinearRing>{kml(polygon.getInteriorRingN(i).getCoordinates)}</LinearRing>
                }
              }
            </innerBoundaryIs>
        }
      }
    </Polygon>
  }

  private def kml(shapes: GeometryCollection): Node = {
    <MultiGeometry>
      {
        Range(0, shapes.getNumGeometries).map { i =>
          shapes.getGeometryN(i) match {
            case p: Point => kml(p)
            case l: LineString => kml(l)
            case p: Polygon => kml(p)
          }
        }
      }
    </MultiGeometry>
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


