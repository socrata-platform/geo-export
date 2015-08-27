package com.socrata.geoexport
package convert.tasks

import java.io.InputStream
import com.socrata.geoexport.conversions.Converter
import com.socrata.geoexport.encoders._
import org.apache.commons.io.output.ByteArrayOutputStream

import scala.xml.{XML, NodeSeq, Node}

class KMLTest extends TestBase {

  test("can convert a stream of point geojson to kml") {
    val (js, expected) = fixtures("points")
    val layers = List(js)

    val actual = convert(layers)

    pluckSchemaNames(actual) must be(pluckSchemaNames(expected))
    pluckSchemaNames(actual).map { attrName =>
      compareSimpleDatums(actual, expected, attrName)
    }

    val expectedCoords = (pluckPlacemark(expected)(0) \ "Point" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }
    val actualCoords = (pluckPlacemark(actual)(0) \ "Point" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }

    actualCoords must be(expectedCoords)
  }

  test("can convert a stream of lines geojson to kml") {
    val (js, expected) = fixtures("lines")
    val layers = List(js)

    val actual = convert(layers)

    pluckSchemaNames(actual) must be(pluckSchemaNames(expected))

    pluckSchemaNames(actual).map { attrName =>
      compareSimpleDatums(actual, expected, attrName)
    }

    val actualLineCoords   = (pluckPlacemark(actual) \ "LineString" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }
    val expectedLineCoords = (pluckPlacemark(expected) \ "LineString" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }

    expectedLineCoords must be(actualLineCoords)
  }

  test("can convert a stream of polygons geojson to kml") {
    val (js, expected) = fixtures("polygons")
    val layers = List(js)

    val actual = convert(layers)

    pluckSchemaNames(actual) must be(pluckSchemaNames(expected))

    pluckSchemaNames(actual).map { attrName =>
      compareSimpleDatums(actual, expected, attrName)
    }

    pluckPolyOuter(actual) must be(pluckPolyOuter(expected))
  }

  test("can convert a stream of multipoints geojson to kml") {
    val (js, expected) = fixtures("multipoints")
    val layers = List(js)

    val actual = convert(layers)

    pluckSchemaNames(actual) must be(pluckSchemaNames(expected))

    pluckSchemaNames(actual).map { attrName =>
      compareSimpleDatums(actual, expected, attrName)
    }

    val actualPoints   = (pluckPlacemark(actual) \ "MultiGeometry" \ "Point" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }
    val expectedPoints = (pluckPlacemark(expected) \ "MultiGeometry" \ "Point" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }

    expectedPoints must be(actualPoints)
  }

  test("can convert a stream of multilines geojson to kml") {
    val (js, expected) = fixtures("multilines")
    val layers = List(js)

    val actual = convert(layers)

    pluckSchemaNames(actual) must be(pluckSchemaNames(expected))

    pluckSchemaNames(actual).map { attrName =>
      compareSimpleDatums(actual, expected, attrName)
    }

    val actualLines   = (pluckPlacemark(actual) \ "MultiGeometry" \ "LineString" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }
    val expectedLines = (pluckPlacemark(expected) \ "MultiGeometry" \ "LineString" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }

    expectedLines must be(actualLines)
  }

  test("can convert a stream of multipolygons geojson to kml") {
    val (js, expected) = fixtures("multipolygons")
    val layers = List(js)

    val actual = convert(layers)

    pluckSchemaNames(actual) must be(pluckSchemaNames(expected))

    pluckSchemaNames(actual).map { attrName =>
      compareSimpleDatums(actual, expected, attrName)
    }

    val actualLines   = (pluckPlacemark(actual) \ "MultiGeometry" \ "Polygon" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }
    val expectedLines = (pluckPlacemark(expected) \ "MultiGeometry" \ "Polygon" \ "coordinates").map {
      c => pluckCoordinates(c.text)
    }

    expectedLines must be(actualLines)
  }

  test("can convert a multiple streams of json to nested kml") {
    //this tests merging two datasets into a single kml file
    val layers = List(fixture("freshzoning_layer1"), fixture("freshzoning_layer2"))

    val expectedLayer1 = kmlFixture("freshzoning_layer1")
    val expectedLayer2 = kmlFixture("freshzoning_layer2")


    val actual = convert(layers)

    actual \ "Document" \ "Folder" match {
      case Seq(actualLayer1, actualLayer2) =>

        List(
          (actualLayer1, expectedLayer1),
          (actualLayer2, expectedLayer2)
        ).map { case (actual, expected) =>

          val expectedColNames = pluckSchemaNames(expected)

          placemarkSchemaNames(actual \ "Placemark") must be(expectedColNames)

          expectedColNames.map { attrName =>
            val actualAttr = placemarkSimpleData(actual \ "Placemark", attrName)
            val expectedAttr = pluckSimpleData(expected, attrName)
            compareNodeTextIgnoreSpaces(actualAttr, expectedAttr)
          }

          placemarkPolyOuter(actual) must be(pluckPolyOuter(expected))
        }
    }
  }


}
