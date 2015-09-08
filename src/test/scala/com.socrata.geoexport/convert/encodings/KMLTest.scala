package com.socrata.geoexport


import java.io._
import java.math.BigDecimal

import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.joda.time.{LocalTime, LocalDateTime, DateTime}
import scala.util.{Try, Success, Failure}
import scala.xml.Utility.{trim => xmltrim}
import com.socrata.geoexport.encoders.KMLMapper._
import com.socrata.geoexport.conversions.Converter
import org.apache.commons.io.output.ByteArrayOutputStream
import scala.xml.{NodeSeq, XML, Node}
import com.socrata.geoexport.encoders.KMLEncoder

class KMLTest extends TestBase {
  val ldt = LocalDateTime.parse("2015-03-22T01:23")
  val simpleSchema = List(
    ("a_name", SoQLText),
    ("a_number", SoQLNumber),
    ("a_bool", SoQLBoolean),
    ("a_ts", SoQLFixedTimestamp),
    ("a_floating_ts", SoQLFloatingTimestamp),
    ("a_time", SoQLTime),
    ("a_date", SoQLDate),
    ("a_money", SoQLMoney)
  )

  val simpleRows = List(
    SoQLText("this is a name"),
    SoQLNumber(new BigDecimal(42.00)),
    SoQLBoolean(true),
    SoQLFixedTimestamp(DateTime.parse("2015-03-22T12:00:00-08:00")),
    SoQLFloatingTimestamp(ldt.plusHours(1)),
    SoQLTime(ldt.toLocalTime),
    SoQLDate(ldt.toLocalDate),
    SoQLMoney((new BigDecimal(42.00)))
  )

  private def convertKML(layers: List[InputStream]): Node = {
    val outStream = new ByteArrayOutputStream()
    val result = Converter.execute(layers, KMLEncoder, outStream) match {
      case Success(outstream) =>
        outStream.flush()
        outStream.toString("UTF-8")
      case Failure(err) => throw err
    }
    XML.loadString(result)
  }

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
    val p = wkt("POINT (0 1)").asInstanceOf[Point]


    val schema = simpleSchema :+ (("a_point", SoQLPoint))
    val items = simpleRows :+ SoQLPoint(p)
    val packed = pack(schema, List(items.toArray))

    val layers = List(packed)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <Point>
                <coordinates> 0.0,1.0 </coordinates>
              </Point>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }

  test("can convert a stream of a linestring soqlpack to kml") {
    val p = wkt("LINESTRING (30 10, 10 30, 40 40)").asInstanceOf[LineString]

    val schema = simpleSchema :+ (("a_line", SoQLLine))
    val items = simpleRows :+ SoQLLine(p)
    val packed = pack(schema, List(items.toArray))

    val layers = List(packed)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <LineString>
                <coordinates>
                  30.0,10.0
                  10.0,30.0
                  40.0,40.0
                </coordinates>
              </LineString>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }

  test("can convert a stream of a polygons soqlpack to kml") {
    val p = wkt("POLYGON ((30.0 10, 40 40, 20 40, 10 20, 30 10))").asInstanceOf[Polygon]

    val schema = simpleSchema :+ (("a_poly", SoQLPolygon))
    val items = simpleRows :+ SoQLPolygon(p)
    val packed = pack(schema, List(items.toArray))

    val layers = List(packed)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <Polygon>
                <outerBoundaryIs>
                  <LinearRing>
                    <coordinates>
                      30.0,10.0
                      40.0,40.0
                      20.0,40.0
                      10.0,20.0
                      30.0,10.0
                    </coordinates>
                  </LinearRing>
                </outerBoundaryIs>
              </Polygon>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }

  test("can convert a stream of polygons with donut shapes (mmm donuts) to kml") {
    val p = wkt("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))").asInstanceOf[Polygon]

    val schema = simpleSchema :+ (("a_poly", SoQLPolygon))
    val items = simpleRows :+ SoQLPolygon(p)
    val packed = pack(schema, List(items.toArray))

    val layers = List(packed)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <Polygon>
                <outerBoundaryIs>
                  <LinearRing>
                    <coordinates>
                      35.0,10.0
                      45.0,45.0
                      15.0,40.0
                      10.0,20.0
                      35.0,10.0
                    </coordinates>
                  </LinearRing>
                </outerBoundaryIs>
                <innerBoundaryIs>
                  <LinearRing><coordinates>20.0,30.0 35.0,35.0 30.0,20.0 20.0,30.0</coordinates></LinearRing>
                </innerBoundaryIs>
              </Polygon>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }

  test("can convert a stream of a multipoint soqlpack to kml") {
    val p = wkt("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").asInstanceOf[MultiPoint]

    val schema = simpleSchema :+ (("a_multipoint", SoQLMultiPoint))
    val items = simpleRows :+ SoQLMultiPoint(p)
    val packed = pack(schema, List(items.toArray))

    val layers = List(packed)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <MultiGeometry>
                <Point><coordinates> 10.0,40.0 </coordinates></Point>
                <Point><coordinates> 40.0,30.0 </coordinates></Point>
                <Point><coordinates> 20.0,20.0 </coordinates></Point>
                <Point><coordinates> 30.0,10.0 </coordinates></Point>

              </MultiGeometry>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }

  test("can convert a stream of a multilines soqlpack to kml") {
    val p = wkt("MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))").asInstanceOf[MultiLineString]

    val schema = simpleSchema :+ (("a_multipoint", SoQLMultiLine))
    val items = simpleRows :+ SoQLMultiLine(p)
    val packed = pack(schema, List(items.toArray))

    val layers = List(packed)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <MultiGeometry>
                <LineString><coordinates> 10.0,10.0 20.0,20.0 10.0,40.0 </coordinates></LineString>
                <LineString><coordinates> 40.0,40.0 30.0,30.0 40.0,20.0 30.0,10.0 </coordinates></LineString>
              </MultiGeometry>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }

  test("can convert a stream of a multipolygon soqlpack to kml") {
    val p = wkt("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))").asInstanceOf[MultiPolygon]

    val schema = simpleSchema :+ (("a_multipoint", SoQLMultiPolygon))
    val items = simpleRows :+ SoQLMultiPolygon(p)
    val packed = pack(schema, List(items.toArray))

    val layers = List(packed)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <MultiGeometry>
                <Polygon>
                  <outerBoundaryIs>
                    <LinearRing>
                      <coordinates>30.0,20.0 45.0,40.0 10.0,40.0 30.0,20.0</coordinates>
                    </LinearRing>
                  </outerBoundaryIs>
                </Polygon>
                <Polygon>
                  <outerBoundaryIs>
                    <LinearRing>
                      <coordinates>15.0,5.0 40.0,10.0 10.0,20.0 5.0,10.0 15.0,5.0</coordinates>
                    </LinearRing>
                  </outerBoundaryIs>
                </Polygon>
              </MultiGeometry>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }

  test("can merge two layers with different geoms to one kml in folders") {
    val line = wkt("LINESTRING (30 10, 10 30, 40 40)").asInstanceOf[LineString]
    val poly = wkt("POLYGON ((30.0 10, 40 40, 20 40, 10 20, 30 10))").asInstanceOf[Polygon]

    val lineSchema = simpleSchema :+ (("a_line", SoQLLine))
    val polySchema = simpleSchema :+ (("a_poly", SoQLPolygon))
    val lineItems = simpleRows :+ SoQLLine(line)
    val polyItems = simpleRows :+ SoQLPolygon(poly)

    val layerOnePack = pack(lineSchema, List(lineItems.toArray))
    val layerTwoPack = pack(polySchema, List(polyItems.toArray))

    val layers = List(layerOnePack, layerTwoPack)
    val actual = convertKML(layers)

    xmltrim(actual) must be(xmltrim(
      <kml xmlns:kml="http://earth.google.com/kml/2.2">
        <Document id="featureCollection">
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <LineString>
                <coordinates>
                  30.0,10.0
                  10.0,30.0
                  40.0,40.0
                </coordinates>
              </LineString>
            </Placemark>
          </Folder>
          <Folder>
            <Placemark>
              <ExtendedData>
                <SchemaData>
                  <SimpleData name="a_name">this is a name</SimpleData>
                  <SimpleData name="a_number">42</SimpleData>
                  <SimpleData name="a_bool">true</SimpleData>
                  <SimpleData name="a_ts">2015-03-22T20:00:00.000Z</SimpleData>
                  <SimpleData name="a_floating_ts">2015-03-22T02:23:00.000</SimpleData>
                  <SimpleData name="a_time">01:23:00.000</SimpleData>
                  <SimpleData name="a_date">2015-03-22</SimpleData>
                  <SimpleData name="a_money">42</SimpleData>
                </SchemaData>
              </ExtendedData>
              <Polygon>
                <outerBoundaryIs>
                  <LinearRing>
                    <coordinates>
                      30.0,10.0
                      40.0,40.0
                      20.0,40.0
                      10.0,20.0
                      30.0,10.0
                    </coordinates>
                  </LinearRing>
                </outerBoundaryIs>
              </Polygon>
            </Placemark>
          </Folder>
        </Document>
      </kml>
    ))
  }




}
