package com.socrata.geoexport

import java.io._
import java.math.BigDecimal
import java.net.URL
import java.util.{Date, UUID}
import java.util.zip.{ZipEntry, ZipFile}
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.apache.commons.io.IOUtils
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.{SimpleFeatureIterator, SimpleFeatureSource, SimpleFeatureCollection}
import org.joda.time.{DateTimeZone, LocalTime, LocalDateTime, DateTime}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.util.{Try, Success, Failure}
import scala.xml.Utility.{trim => xmltrim}
import com.socrata.geoexport.conversions.Converter
import org.apache.commons.io.output.ByteArrayOutputStream
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.geoexport.encoders.GeoJSONEncoder
import com.socrata.thirdparty.geojson.JtsCodecs.geoCodec
import com.rojoma.simplearm.v2._

class GeoJSONTest extends TestBase {
  val ldt = LocalDateTime.parse("2015-03-22T01:23")
  val dt = DateTime.parse("2015-03-22T12:00:00-08:00")
  val THE_TIME_UTC = 1427025600000L
  val AN_HOUR = 60 * 60 * 1000

  val simpleSchema = List(
    ("a_name", SoQLText),
    ("a_number", SoQLNumber),
    ("a_bool", SoQLBoolean),
    (":a_ts", SoQLFixedTimestamp),
    (":a_fts", SoQLFloatingTimestamp),
    ("a_time", SoQLTime),
    (":a_date", SoQLDate),
    (":a_money", SoQLMoney),
    (":an_id", SoQLID),
    (":a_version", SoQLVersion),
    ("a_double", SoQLDouble),
    ("a_num_arr", SoQLArray),
    ("a_str_arr", SoQLArray),
    ("a_json", SoQLJson),
    ("an_object", SoQLObject)
  )

  val simpleRows = List(
    SoQLText("this is a name"),
    SoQLNumber(new BigDecimal(42.00)),
    SoQLBoolean(true),
    SoQLFixedTimestamp(dt),
    SoQLFloatingTimestamp(ldt.plusHours(1)),
    SoQLTime(ldt.toLocalTime),
    SoQLDate(ldt.toLocalDate),
    SoQLMoney((new BigDecimal(42.00))),
    SoQLID(42),
    SoQLVersion(32),
    SoQLDouble(42.00),
    SoQLArray(JArray(Seq(JNumber(1), JNumber(2), JNumber(3)))),
    SoQLArray(JArray(Seq(JString("a"), JString("b"), JString("c")))),
    SoQLJson(JsonReader.fromString("""{"something": "else", "a_json_number": 1, "nested": {"child": "hello"}}""")),
    SoQLObject(JsonReader.fromString("""{"something": "wow", "an_object_number": 7, "nested": {"child": "hi"}}""").asInstanceOf[JObject])

  )


  private def convertGeoJSON(layers: List[InputStream]): String = {
    using(new ResourceScope) { rs =>
      val outStream = new ByteArrayOutputStream()
      val result = Converter.execute(rs, layers, GeoJSONEncoder, outStream) match {
        case Success(outstream) =>
          outStream.flush()
          outStream.toString("UTF-8")
        case Failure(err) => throw err
      }
      result.replaceAll("\\s", "")
    }
  }

  private def getExpected(shape: String): String = {
    s"""
    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {
            "a_money": "42",
            "a_str_arr": [
              "a",
              "b",
              "c"
            ],
            "a_name": "this is a name",
            "a_date": "2015-03-22",
            "an_object": {
              "something": "wow",
              "an_object_number": 7,
              "nested": {
                "child": "hi"
              }
            },
            "an_id": "42",
            "a_num_arr": [
              1,
              2,
              3
            ],
            "a_version": "32",
            "a_json": {
              "something": "else",
              "a_json_number": 1,
              "nested": {
                "child": "hello"
              }
            },
            "a_number": "42",
            "a_ts": "2015-03-22T20:00:00.000Z",
            "a_fts": "2015-03-22T02:23:00.000",
            "a_double": "42.0",
            "a_bool": "true",
            "a_time": "01:23:00.000"
          },
          "geometry": ${shape}
        }
      ]
    }
    """.replaceAll("\\s", "")
  }

  private def serializeSingleLayer(name: String, geom: Geometry, soqlValue: SoQLValue, kind: SoQLType) : String = {

    val soqlSchema = simpleSchema :+ ((name, kind))
    val items = simpleRows :+ soqlValue
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    convertGeoJSON(layers)
  }

  test("can convert a stream of a point soqlpack to geoJSON") {
    val p = wkt("POINT (0 1)").asInstanceOf[Point]
    val actual = serializeSingleLayer("a_point", p, SoQLPoint(p), SoQLPoint)

    val expectedJs = CompactJsonWriter.toString(geoCodec.encode(p))
    actual must be(getExpected(expectedJs))
  }

  test("can convert a stream of line soqlpack to geoJSON") {
    val p = wkt("LINESTRING (30 10, 10 30, 40 40)").asInstanceOf[LineString]
    val actual = serializeSingleLayer("a_line", p, SoQLLine(p), SoQLLine)

    val expectedJs = CompactJsonWriter.toString(geoCodec.encode(p))
    actual must be(getExpected(expectedJs))
  }

  test("can convert a stream of polygon soqlpack to geoJSON") {
    val p = wkt("POLYGON ((30.0 10, 40 40, 20 40, 10 20, 30 10))").asInstanceOf[Polygon]
    val actual = serializeSingleLayer("a_poly", p, SoQLPolygon(p), SoQLPolygon)

    val expectedJs = CompactJsonWriter.toString(geoCodec.encode(p))
    actual must be(getExpected(expectedJs))

    //with donuts (yum)
    val donut = wkt("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))").asInstanceOf[Polygon]
    val actualDonut = serializeSingleLayer("a_poly", donut, SoQLPolygon(donut), SoQLPolygon)
    val expectedDonut = CompactJsonWriter.toString(geoCodec.encode(donut))
    actualDonut must be(getExpected(expectedDonut))

  }

  test("can convert a stream of multipoint soqlpack to geoJSON") {
    val p = wkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").asInstanceOf[MultiPoint]
    val actual = serializeSingleLayer("a_multipoint", p, SoQLMultiPoint(p), SoQLMultiPoint)

    val expectedJs = CompactJsonWriter.toString(geoCodec.encode(p))
    actual must be(getExpected(expectedJs))
  }

  test("can convert a stream of multiline soqlpack to geoJSON") {
    val p = wkt("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))").asInstanceOf[MultiLineString]
    val actual = serializeSingleLayer("a_multiline", p, SoQLMultiLine(p), SoQLMultiLine)

    val expectedJs = CompactJsonWriter.toString(geoCodec.encode(p))
    actual must be(getExpected(expectedJs))
  }


  test("can convert a stream of multipolygon soqlpack to geoJSON") {
    val p = wkt("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))").asInstanceOf[MultiPolygon]
    val actual = serializeSingleLayer("a_multipoly", p, SoQLMultiPolygon(p), SoQLMultiPolygon)

    val expectedJs = CompactJsonWriter.toString(geoCodec.encode(p))
    actual must be(getExpected(expectedJs))
  }

  test("can convert a stream of heterogenous layers to geoJSON") {
    val line = wkt("LINESTRING (30 10, 10 30, 40 40)").asInstanceOf[LineString]
    val poly = wkt("POLYGON ((30.0 10, 40 40, 20 40, 10 20, 30 10))").asInstanceOf[Polygon]

    val lineSchema = simpleSchema :+ (("a_line", SoQLLine))
    val polySchema = simpleSchema :+ (("a_poly", SoQLPolygon))
    val lineItems = simpleRows :+ SoQLLine(line)
    val polyItems = simpleRows :+ SoQLPolygon(poly)

    val layerOnePack = pack(lineSchema, List(lineItems.toArray))
    val layerTwoPack = pack(polySchema, List(polyItems.toArray))

    val layers = List(layerOnePack, layerTwoPack)
    val actual = convertGeoJSON(layers)

    val expectedLine = geoCodec.encode(line)
    val expectedPoly = geoCodec.encode(poly)

    val expectedJs = s"""
    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {
            "a_money": "42",
            "a_str_arr": [
              "a",
              "b",
              "c"
            ],
            "a_name": "this is a name",
            "a_date": "2015-03-22",
            "an_object": {
              "something": "wow",
              "an_object_number": 7,
              "nested": {
                "child": "hi"
              }
            },
            "an_id": "42",
            "a_num_arr": [
              1,
              2,
              3
            ],
            "a_version": "32",
            "a_json": {
              "something": "else",
              "a_json_number": 1,
              "nested": {
                "child": "hello"
              }
            },
            "a_number": "42",
            "a_ts": "2015-03-22T20:00:00.000Z",
            "a_fts": "2015-03-22T02:23:00.000",
            "a_double": "42.0",
            "a_bool": "true",
            "a_time": "01:23:00.000"
          },
          "geometry": ${expectedLine}
        },
        {
          "type": "Feature",
          "properties": {
            "a_money": "42",
            "a_str_arr": [
              "a",
              "b",
              "c"
            ],
            "a_name": "this is a name",
            "a_date": "2015-03-22",
            "an_object": {
              "something": "wow",
              "an_object_number": 7,
              "nested": {
                "child": "hi"
              }
            },
            "an_id": "42",
            "a_num_arr": [
              1,
              2,
              3
            ],
            "a_version": "32",
            "a_json": {
              "something": "else",
              "a_json_number": 1,
              "nested": {
                "child": "hello"
              }
            },
            "a_number": "42",
            "a_ts": "2015-03-22T20:00:00.000Z",
            "a_fts": "2015-03-22T02:23:00.000",
            "a_double": "42.0",
            "a_bool": "true",
            "a_time": "01:23:00.000"
          },
          "geometry": ${expectedPoly}
        }
      ]
    }
    """.replaceAll("\\s", "")

    actual must be(expectedJs)

  }

}
