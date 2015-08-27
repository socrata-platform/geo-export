package com.socrata.geoexport.encoders

import java.io.{OutputStream, OutputStreamWriter, Writer}

import com.vividsolutions.jts.geom._
import org.apache.commons.io.output.{ByteArrayOutputStream, WriterOutputStream}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import KMLMapper._
import scala.xml.{Node, XML}

//Convert to XML nodes: https://developers.google.com/kml/documentation/kmlreference
//functions in here should take a value or seq of values and return a single Node
object KMLMapper {
  type Attributes = Seq[AttributeDescriptor]
  type Collection = Iterator[SimpleFeature]
  type Layers = Iterable[Collection]

  def genKML(layers: Layers): Node = {
    <kml xmlns:kml="http://earth.google.com/kml/2.2">
      <Document id="featureCollection">
        { layers.map(kml(_)) }
      </Document>
    </kml>
  }

  private def kml(collection: Collection): Node = {
    <Folder>
      { collection.map(kml(_)) }
    </Folder>
  }

  private def kml(feature: SimpleFeature): Node = {
    val attrs = feature.getType.getAttributeDescriptors.toList
    .map(_.getLocalName)
    .filter(name => name != feature.getType.getGeometryDescriptor.getLocalName)
    .map(name => (name, feature.getAttribute(name)))

    <Placemark>
      <ExtendedData>
        <SchemaData>
          {attrs.map(kml(_))}
        </SchemaData>
      </ExtendedData>
      { feature.getDefaultGeometry match { case geom: Geometry => geomToKML(geom) } }
    </Placemark>
  }

  private def geomToKML(geo: Geometry): Node = {
    geo match {
      //ugh this is goofy..have to force everything into its canonical type
      case point: Point => kml(point)
      case line: LineString => kml(line)
      case poly: Polygon => kml(poly)
      //kml composes collections within a MultiGeometry tag, which is nice
      //because it simplifies everything
      case many: GeometryCollection => kml(many)
    }
  }

  private def kml(simpleData: (String, AnyRef)): Node = {
    val (name, value) = simpleData
    <SimpleData name={name}>{value}</SimpleData>
  }

  ///Shape to KML transforms
  private def kml(coordinates: Array[Coordinate]): Node = {
    def toStr(c: Coordinate): String = {
      (c.x, c.y, c.z) match {
        case (x, y, z) if z.isNaN => s"${x},${y}"
        case (x, y, z) => s"${x},${y},${z}"
        case _ => ""
      }
    }
    <coordinates>
      {coordinates.map(toStr(_)).mkString(",\n")}
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
          case count => Range(0, count).map { i =>
            <LinearRing>{kml(polygon.getInteriorRingN(i).getCoordinates)}</LinearRing>
          }
        }
      }
    </Polygon>
  }

  private def kml(shapes: GeometryCollection): Node = {
    <MultiGeometry>
      {
        Range(0, shapes.getNumGeometries).map { i =>
          geomToKML(shapes.getGeometryN(i))
        }
      }
    </MultiGeometry>
  }
}

class KMLEncoder extends GeoEncoder {

  def writeKML(kml: Node, writer: Writer): Unit = {
    XML.write(writer, kml, "UTF-8", true, null)
  }

  def encode(layers: Layers, outStream: OutputStream) : Either[String, OutputStream] = {

    val writer = new OutputStreamWriter(outStream)

    try {
      val kml = KMLMapper.genKML(layers)
      writeKML(kml, writer)
    } finally {
      writer.close()
    }

    return Right(outStream)
  }
}


