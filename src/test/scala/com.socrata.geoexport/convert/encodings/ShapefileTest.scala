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
import org.locationtech.jts.geom._
import com.vividsolutions.jts.{geom => vgeom}
import org.apache.commons.io.IOUtils
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.{SimpleFeatureIterator, SimpleFeatureSource, SimpleFeatureCollection}
import org.joda.time.{DateTimeZone, LocalTime, LocalDateTime, DateTime}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.util.{Try, Success, Failure}
import scala.xml.Utility.{trim => xmltrim}
import com.socrata.geoexport.conversions.Converter
import org.apache.commons.io.output.ByteArrayOutputStream
import scala.xml.{NodeSeq, XML, Node}
import com.socrata.geoexport.encoders.ShapefileEncoder
import com.socrata.soql.SoQLPackIterator
import com.rojoma.simplearm.v2._

import com.socrata.geoexport.intermediates.shapefile.GeoDatum

class GeoIterator(sfi: SimpleFeatureIterator) extends Iterator [SimpleFeature] with AutoCloseable {
  def hasNext: Boolean = sfi.hasNext()
  def next: SimpleFeature = sfi.next()
  def remove(): Unit = ???
  def close: Unit = sfi.close()
}

class ShapefileTest extends TestBase {
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
    ("a_null_fts", SoQLFloatingTimestamp),
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
    SoQLText("this is a name¾"),
    SoQLNumber(new BigDecimal(42.00)),
    SoQLBoolean(true),
    SoQLFixedTimestamp(dt),
    SoQLFloatingTimestamp(ldt.plusHours(1)),
    SoQLNull,
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

  private def convertShapefile(layers: List[InputStream]): File = {
    using(new ResourceScope) { rs =>
      val archive = new File(s"/tmp/test_geo_export_${UUID.randomUUID()}.zip")
      val outStream = new FileOutputStream(archive)

      val layerStream = layers.map { is => new SoQLPackIterator(new DataInputStream(is)) }
      val files = ShapefileEncoder.buildFiles(rs, layerStream)
      ShapefileEncoder.streamZip(files, outStream)
      outStream.flush()
      archive
    }
  }

  private def multiPointCoords(mp: MultiPoint) = {
    Range(0, mp.getNumGeometries)
          .map(mp.getGeometryN(_).asInstanceOf[Point])
          .map { p => (p.getX, p.getY)}.toList
  }



  private def verifyFeature(feature: SimpleFeature) = {
    feature.getAttribute("a_name").toString must be("this is a name¾")
    feature.getAttribute("a_number") must be(42.00)
    feature.getAttribute("a_bool") must be(java.lang.Boolean.TRUE)

    feature.getAttribute("date_a_ts").asInstanceOf[Date] must be(ldt.withTime(0, 0, 0, 0).toDate)
    feature.getAttribute("time_a_ts") must be("20:00:00.000")

    feature.getAttribute("date_a_nul") must be(null)
    feature.getAttribute("time_a_nul") must be(null)

    feature.getAttribute("date_a_fts").asInstanceOf[Date] must be(ldt.withTime(0, 0, 0, 0).toDate)
    feature.getAttribute("time_a_fts") must be("02:23:00.000")

    feature.getAttribute("a_time") must be("01:23:00.000")
    feature.getAttribute("a_date") must be(ldt.withTime(0, 0, 0, 0).toDate)

    feature.getAttribute("a_money") must be(42.00)

    //this is a string because it's too big to represent in a DBF
    feature.getAttribute("a_version") must be("32")

    feature.getAttribute("a_double") must be(42.0)

    feature.getAttribute("a_num_arr") must be("[ 1, 2, 3 ]")
    feature.getAttribute("a_str_arr") must be("""[ "a", "b", "c" ]""")

    val actualJs = feature.getAttribute("a_json").asInstanceOf[String].replaceAll("\\s", "")
    val expectedJs = """{"something": "else", "a_json_number": 1, "nested": {"child": "hello"}}""".replaceAll("\\s", "")
    actualJs must be(expectedJs)

    val actualObj = feature.getAttribute("an_object").asInstanceOf[String].replaceAll("\\s", "")
    val expectedObj = """{"something": "wow", "an_object_number": 7, "nested": {"child": "hi"}}""".replaceAll("\\s", "")
    actualObj must be(expectedObj)
  }


  test("can convert non geometry soql values to shp") {
    val p = wkt("POINT (0 1)").asInstanceOf[vgeom.Point]

    val soqlSchema = simpleSchema :+ (("a_point", SoQLPoint))
    val items = simpleRows :+ SoQLPoint(p)
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((schema, features)) =>

        features.size must be(1)
        val feature = features(0)

        verifyFeature(feature)
    }
  }


  test("can convert a stream of a point soqlpack to shp") {
    val p = wkt("POINT (0 1)").asInstanceOf[vgeom.Point]

    val soqlSchema = simpleSchema :+ (("a_point", SoQLPoint))
    val items = simpleRows :+ SoQLPoint(p)
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((schema, features)) =>
        features.size must be(1)
        val feature = features(0)
        val point = feature.getDefaultGeometry.asInstanceOf[Point]
        point.getX must be(0)
        point.getY must be(1)
    }

  }

  test("can convert a stream of a line soqlpack to shp") {
    val line = wkt("LINESTRING (30 10, 10 30, 40 40)").asInstanceOf[vgeom.LineString]

    val soqlSchema = simpleSchema :+ (("a_line", SoQLLine))
    val items = simpleRows :+ SoQLLine(line)
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((schema, features)) =>

        features.size must be(1)
        val feature = features(0)
        val shpLine = feature.getDefaultGeometry.asInstanceOf[MultiLineString]
        shpLine.getGeometryN(0).getCoordinates must be(GeoDatum.vgeom2geom(line).getCoordinates)
    }
  }

  test("can convert a stream of a polygon soqlpack to shp") {
    val poly = wkt("POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))").asInstanceOf[vgeom.Polygon]

    val soqlSchema = simpleSchema :+ (("a_poly", SoQLPolygon))
    val items = simpleRows :+ SoQLPolygon(poly)
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((schema, features)) =>
        features.size must be(1)

        val feature = features(0)
        val shpPoly = feature.getDefaultGeometry.asInstanceOf[MultiPolygon]

        shpPoly.getGeometryN(0).getCoordinates must be(GeoDatum.vgeom2geom(poly).getCoordinates)
    }
  }

  test("can convert a stream of a multipoint soqlpack to shp") {
    val points = wkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").asInstanceOf[vgeom.MultiPoint]

    val soqlSchema = simpleSchema :+ (("a_multipoint", SoQLMultiPoint))
    val items = simpleRows :+ SoQLMultiPoint(points)
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((schema, features)) =>
        features.size must be(1)

        val feature = features(0)
        val shpPoints = feature.getDefaultGeometry.asInstanceOf[MultiPoint]

        val coords = multiPointCoords(shpPoints)

        coords must be(List((10.0,40.0), (40.0,30.0), (20.0,20.0), (30.0,10.0)))
    }
  }

  test("can convert a stream of a MultiLine soqlpack to shp") {
    val lines = wkt("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))").asInstanceOf[vgeom.MultiLineString]

    val soqlSchema = simpleSchema :+ (("a_multipoly", SoQLMultiLine))
    val items = simpleRows :+ SoQLMultiLine(lines)
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((schema, features)) =>
        features.size must be(1)
        val feature = features(0)
        val shpLines = feature.getDefaultGeometry.asInstanceOf[MultiLineString]

        shpLines.getGeometryN(0).getCoordinates must be(GeoDatum.vgeom2geom(lines).getGeometryN(0).getCoordinates)
        shpLines.getGeometryN(1).getCoordinates must be(GeoDatum.vgeom2geom(lines).getGeometryN(1).getCoordinates)

    }
  }

  test("can convert a stream of a MultiPolygon soqlpack to shp") {
    val expectedPolys = wkt("MULTIPOLYGON (((30 20, 10 40, 45 40, 30 20)), ((15 5, 5 10, 10 20, 40 10, 15 5)))").asInstanceOf[vgeom.MultiPolygon]
    val soqlSchema = simpleSchema :+ (("a_multipoly", SoQLMultiPolygon))
    val items = simpleRows :+ SoQLMultiPolygon(expectedPolys)
    val packed = pack(soqlSchema, List(items.toArray))

    val layers = List(packed)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((schema, features)) =>
        features.size must be(1)

        val feature = features(0)
        val actualPolys = feature.getDefaultGeometry.asInstanceOf[MultiPolygon]

        actualPolys.getGeometryN(0).getCoordinates must be(GeoDatum.vgeom2geom(expectedPolys).getGeometryN(0).getCoordinates)
        actualPolys.getGeometryN(1).getCoordinates must be(GeoDatum.vgeom2geom(expectedPolys).getGeometryN(1).getCoordinates)

    }
  }


  test("can convert multiple streams of soqlpack a multilayer to shp") {
    val expectedPoints = wkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").asInstanceOf[vgeom.MultiPoint]
    val soqlPointSchema = simpleSchema :+ (("a_multipoint", SoQLMultiPoint))
    val pointRows = simpleRows :+ SoQLMultiPoint(expectedPoints)
    val packedMultipoints = pack(soqlPointSchema, List(pointRows.toArray))

    val expectedPolys = wkt("MULTIPOLYGON (((30 20, 10 40, 45 40, 30 20)), ((15 5, 5 10, 10 20, 40 10, 15 5)))").asInstanceOf[vgeom.MultiPolygon]
    val soqlPolySchema = simpleSchema :+ (("a_multipoly", SoQLMultiPolygon))
    val polyRows = simpleRows :+ SoQLMultiPolygon(expectedPolys)
    val packedPolys = pack(soqlPolySchema, List(polyRows.toArray))

    val layers = List(packedMultipoints, packedPolys)
    val archive = convertShapefile(layers)
    readShapeArchive(archive) match {
      case Seq((pointSchema, pointFeatures), (polySchema, polyFeatures)) =>
        pointFeatures.size must be(1)
        polyFeatures.size must be(1)

        val actualPointFeature = pointFeatures(0)
        val actualMultiPoint = actualPointFeature.getDefaultGeometry.asInstanceOf[MultiPoint]
        multiPointCoords(actualMultiPoint) must be(multiPointCoords(GeoDatum.vgeom2geom(expectedPoints)))

        val actualPolyFeature = polyFeatures(0)
        val actualPoly = actualPolyFeature.getDefaultGeometry.asInstanceOf[MultiPolygon]
        actualPoly.getGeometryN(0).getCoordinates must be(GeoDatum.vgeom2geom(expectedPolys).getGeometryN(0).getCoordinates)
        actualPoly.getGeometryN(1).getCoordinates must be(GeoDatum.vgeom2geom(expectedPolys).getGeometryN(1).getCoordinates)

        verifyFeature(actualPointFeature)
        verifyFeature(actualPolyFeature)
    }
  }


}
