package com.socrata.geoexport


import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import encoders.geoexceptions._

import scala.xml.Utility.{trim => xmltrim}

class KMLTest extends KMLIshTest {


  test("cannot convert multi column geo to kml") {
    val line = wkt("LINESTRING (30 10, 10 30, 40 40)").asInstanceOf[LineString]
    val poly = wkt("POLYGON ((30.0 10, 40 40, 20 40, 10 20, 30 10))").asInstanceOf[Polygon]

    val invalidSchema = simpleSchema :+ (("a_line", SoQLLine)) :+ (("a_poly", SoQLPolygon))
    val invalidRows = simpleRows :+ SoQLLine(line) :+ SoQLPolygon(poly)

    val layer = pack(invalidSchema, List(invalidRows.toArray))

    // convert is a TestBase function that loads the outputstream back into a string and
    // then into a DOM. It also raises exceptions on matching a Try Failure.
    an[MultipleGeometriesFoundException] should be thrownBy convertKML(List(layer))
  }

  test("can convert a stream of a point soqlpack to kml") {
    testPoints(convertKML(_))
  }

  test("can convert a stream of a linestring soqlpack to kml") {
    testLinestring(convertKML(_))
  }

  test("can convert a stream of a polygons soqlpack to kml") {
    testPolygonsSingleRing(convertKML(_))
  }

  test("can convert a stream of polygons with donut shapes (mmm donuts) to kml") {
    testPolygonsWithDonuts(convertKML(_))
  }

  test("can convert a stream of a multipoint soqlpack to kml") {
    testMultiPoints(convertKML(_))
  }

  test("can convert a stream of a multilines soqlpack to kml") {
    testMultiLines(convertKML(_))
  }

  test("can convert a stream of a multipolygon soqlpack to kml") {
    testMultiPolygons(convertKML(_))
  }

  test("can merge two layers with different geoms to one kml in folders") {
    testMultiLayerHeterogenous(convertKML(_))
  }




}
