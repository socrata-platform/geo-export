package com.socrata.geoexport.encoders

import java.io.{OutputStream, OutputStreamWriter}

import com.rojoma.json.v3.ast.{JObject, JString, JValue}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.geoexport.intermediates.geojson.GeoJSONRepMapper
import com.rojoma.simplearm.v2.ResourceScope
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}
import geotypes._

object GeoJSONMapper extends RowMapper[JValue] {

  protected def prefix = """{
      |  "type": "FeatureCollection",
      |  "features": [
    """.stripMargin

  protected def suffix: String = """]
    |}""".stripMargin

  override protected def layerSuffix(moreLayers: Boolean) = {
    if(moreLayers) {
      // sigh...
      "," // scalastyle:ignore multiple.string.literals
    } else {
      ""
    }
  }

  protected def toRow(schema: Schema, fields: Fields): JValue = {

    val (geomAttr, attrs) = splitOnGeo(GeoJSONRepMapper, schema, fields)

    JObject(
      Map(
        "type" -> JString("Feature"),
        "properties" -> JObject(attrs.flatMap { case (value, intermediary) =>
          intermediary.toAttrNames.zip(GeoJSONRepMapper.toAttr((value, intermediary)))
        }.toMap),
        "geometry" -> GeoJSONRepMapper.toAttr(geomAttr).head
      )
    )
  }

  protected def writeRow(js: JValue, writer: OutputStreamWriter, moreFeatures: Boolean): Unit = {
    var jsWriter = new CompactJsonWriter(writer)
    jsWriter.write(js)
    if(moreFeatures) writer.write(",")
  }

}



object GeoJSONEncoder extends GeoEncoder {

  def encode(rs: ResourceScope, layers: Layers, outStream: OutputStream) : Try[OutputStream] = {
    val writer = new OutputStreamWriter(outStream)
    try {
      GeoJSONMapper.serialize(layers, writer)
      Success(outStream)
    } catch {
      case e: Exception => Failure(e)
    } finally {
      writer.close()
    }
  }

  def encodes: Set[String] = Set("geojson")
  def encodedMIME: String  = "application/vnd.geo+json"
}


