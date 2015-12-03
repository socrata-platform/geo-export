package com.socrata.geoexport.encoders

import java.io.{OutputStream, OutputStreamWriter}

import com.socrata.geoexport.intermediates.kml._
import com.rojoma.simplearm.v2.ResourceScope
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import scala.xml.{Node, XML}
import geotypes._

// Convert to XML nodes: https://developers.google.com/kml/documentation/kmlreference
// functions in here should take a value or seq of values and return a single Node
object KMLMapper extends RowMapper[Node] {

  protected def prefix = """<?xml version='1.0' encoding='UTF-8'?>
    |<kml xmlns:kml="http://earth.google.com/kml/2.2">
    |  <Document id="featureCollection">
    |<Style id="defaultStyle">
    |  <LineStyle>
    |    <width>1.5</width>
    |  </LineStyle>
    |  <PolyStyle>
    |    <color>7d8a30c4</color>
    |  </PolyStyle>
    |</Style>
  """.stripMargin

  protected def suffix = """  </Document>
  </kml>""".stripMargin

  override protected def layerPrefix = "<Folder>"
  override protected def layerSuffix(_moreLayers: Boolean) = "</Folder>"

  protected def writeRow(node: Node, writer: OutputStreamWriter, _hasNext: Boolean) = {
    XML.write(writer, node, "UTF-8", false, null) //scalastyle:ignore
  }

  protected def toRow(schema: Schema, fields: Fields): Node = {

    val (geomAttr, attrs) = splitOnGeo(KMLRepMapper, schema, fields)

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
  def encode(rs: ResourceScope, layers: Layers, outStream: OutputStream) : Try[OutputStream] = {
    val writer = new OutputStreamWriter(outStream)
    try {
      KMLMapper.serialize(layers, writer)
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
