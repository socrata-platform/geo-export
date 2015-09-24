package com.socrata.geoexport.intermediates
package kml

import com.rojoma.json.v3.ast._
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.slf4j.LoggerFactory
import scala.xml.Node

case class KMLTranslationException(message: String) extends Exception

abstract class KMLRep[T](soqlName: String) extends ShapeRep[T] {
  protected def normalizeName(name: String) = {
    name
  }

  def toAttrBindings: Seq[Class[_]] = throw new KMLTranslationException("KML doesn't use bindings.")
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
}

trait GeoDatum {
  protected def encodeCoordinates(coordinates: Array[Coordinate]): Node = {
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

  protected def encodePoint(point: Point): Node = <Point>{encodeCoordinates(point.getCoordinates)}</Point>
  protected def encodeLine(line: LineString): Node = <LineString>{encodeCoordinates(line.getCoordinates)}</LineString>

  protected def encodePolygon(polygon: Polygon): Node = {
    <Polygon>
      <outerBoundaryIs>
        <LinearRing>{encodeCoordinates(polygon.getExteriorRing.getCoordinates)}</LinearRing>
      </outerBoundaryIs>
      {
        polygon.getNumInteriorRing match {
          case 0 => ""
          case count: Int =>
            <innerBoundaryIs>
              {
                Range(0, count).map { i =>
                  <LinearRing>{encodeCoordinates(polygon.getInteriorRingN(i).getCoordinates)}</LinearRing>
                }
              }
            </innerBoundaryIs>
        }
      }
    </Polygon>
  }

  protected def encodeMulti(shapes: GeometryCollection): Node = {
    <MultiGeometry>
      {
        Range(0, shapes.getNumGeometries).map { i =>
          shapes.getGeometryN(i) match {
            case p: Point => encodePoint(p)
            case l: LineString => encodeLine(l)
            case p: Polygon => encodePolygon(p)
          }
        }
      }
    </MultiGeometry>
  }

  def isGeometry: Boolean = true
}

trait SimpleDatum {
  def asSimpleData(item: String): Seq[Node] = {
    toAttrNames.zip(List(item)).map { case (name, value) =>
      <SimpleData name={name}>{value}</SimpleData>
    }
  }
  def toAttrNames: Seq[String]
  def isGeometry: Boolean = false
}

trait ComplexDatum extends SimpleDatum {
  def fromJValue(soqlName: String, v: JValue): Seq[Node] = v match {
    case n: JNull => asSimpleData(null) // scalastyle:ignore
    case JBoolean(b) => asSimpleData(b.booleanValue.toString)
    case JString(s) => asSimpleData(s)
    case n: JNumber => asSimpleData(n.toString)
    case JArray(arr) =>
      arr.zipWithIndex.flatMap { case (elem, index) =>
        val childName = s"${soqlName}.${index}"
        new JSONRep(childName).fromJValue(childName, elem)
      }
    case JObject(fields) =>
      fields.flatMap { case (name, value) =>
        val childName = s"${soqlName}.${name}"
        new JSONRep(childName).fromJValue(childName, value)
      }.toSeq
  }
}

class PointRep(soqlName: String) extends KMLRep[SoQLPoint](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLPoint): Seq[Any] = encodePoint(soql.value)
}

class MultiPointRep(soqlName: String) extends KMLRep[SoQLMultiPoint](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLMultiPoint): Seq[Any] = encodeMulti(soql.value)
}

class LineRep(soqlName: String) extends KMLRep[SoQLLine](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLLine): Seq[Any] = encodeLine(soql.value)
}

class MultiLineRep(soqlName: String) extends KMLRep[SoQLMultiLine](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLMultiLine): Seq[Any] = encodeMulti(soql.value)
}

class PolygonRep(soqlName: String) extends KMLRep[SoQLPolygon](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLPolygon): Seq[Any] = encodePolygon(soql.value)
}

class MultiPolygonRep(soqlName: String) extends KMLRep[SoQLMultiPolygon](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLMultiPolygon): Seq[Any] = encodeMulti(soql.value)
}

class DateRep(soqlName: String) extends KMLRep[SoQLDate](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLDate): Seq[Any] = asSimpleData(SoQLDate.StringRep(soql.value))
}

class TimeRep(soqlName: String) extends KMLRep[SoQLTime](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLTime): Seq[Any] = asSimpleData(SoQLTime.StringRep(soql.value))
}

class FloatingTimestampRep(soqlName: String) extends KMLRep[SoQLFloatingTimestamp](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLFloatingTimestamp): Seq[Any] = asSimpleData(SoQLFloatingTimestamp.StringRep(soql.value))
}

class FixedTimestampRep(soqlName: String) extends KMLRep[SoQLFixedTimestamp](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLFixedTimestamp): Seq[Any] = asSimpleData(SoQLFixedTimestamp.StringRep(soql.value))
}

class NumberRep(soqlName: String) extends KMLRep[SoQLNumber](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLNumber): Seq[Any] = asSimpleData(soql.value.toString)
}

// KML hardcodes name and description columns as distinct from simpldata columns
class TextRep(soqlName: String) extends KMLRep[SoQLText](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLText): Seq[Any] = {
    toAttrNames.head.toLowerCase match {
      case "description" => <description>{soql.value.toString}</description>
      case "name" => <name>{soql.value.toString}</name>
      case _ => asSimpleData(soql.value.toString)
    }
  }
}

class MoneyRep(soqlName: String) extends KMLRep[SoQLMoney](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLMoney): Seq[Any] = asSimpleData(soql.value.toString)
}

class BooleanRep(soqlName: String) extends KMLRep[SoQLBoolean](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLBoolean): Seq[Any] = asSimpleData(soql.value.toString)
}

class VersionRep(soqlName: String) extends KMLRep[SoQLVersion](soqlName) with SimpleDatum with SocrataMetadataRep {
  def toAttrValues(soql: SoQLVersion): Seq[Any] = asSimpleData(soql.value.toString)
  override protected def normalizeName(name: String) = normalizeIdLike(name)
}

class IDRep(soqlName: String) extends KMLRep[SoQLID](soqlName) with SimpleDatum with SocrataMetadataRep {
  def toAttrValues(soql: SoQLID): Seq[Any] = asSimpleData(soql.value.toString)
  override protected def normalizeName(name: String) = normalizeIdLike(name)
}

class DoubleRep(soqlName: String) extends KMLRep[SoQLDouble](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLDouble): Seq[Any] = asSimpleData(soql.value.toString)
}

class ArrayRep(soqlName: String) extends KMLRep[SoQLArray](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLArray): Seq[Any] = {
    asSimpleData(soql.value.toArray.map {
      case JString(s) => s
      case other: Any => other
    }.mkString(", "))
  }
}

/**
  Complex intermediate mappings.

  If we have
  ```
    {"i": "love", {"xml": "jk"}}
  ```

  That would get mapped and flattened to
  ```
    <SimpleData name="i">love</SimpleData>
    <SimpleData name="i.xml">jk</SimpleData>
  ```
*/
class JSONRep(soqlName: String) extends KMLRep[SoQLJson](soqlName) with ComplexDatum {
  def toAttrValues(soql: SoQLJson): Seq[Any] = fromJValue(soqlName, soql.value)
}

class ObjectRep(soqlName: String) extends KMLRep[SoQLObject](soqlName) with ComplexDatum {
  def toAttrValues(soql: SoQLObject): Seq[Any] = fromJValue(soqlName, soql.value)
}

/**
  Maps all the SoQLColumns to intermediate columns
*/
object KMLRepMapper extends RepMapper {
  lazy val log = LoggerFactory.getLogger(getClass)

  def forPoint(name: String): PointRep =                          new PointRep(name)
  def forMultiPoint(name: String): MultiPointRep =                new MultiPointRep(name)
  def forLine(name: String): LineRep =                            new LineRep(name)
  def forMultiLine(name: String): MultiLineRep =                  new MultiLineRep(name)
  def forPolygon(name: String): PolygonRep =                      new PolygonRep(name)
  def forMultiPolygon(name: String): MultiPolygonRep =            new MultiPolygonRep(name)
  def forDate(name: String): DateRep =                            new DateRep(name)
  def forTime(name: String): TimeRep =                            new TimeRep(name)
  def forFloatingTimestamp(name: String): FloatingTimestampRep =  new FloatingTimestampRep(name)
  def forFixedTimestamp(name: String): FixedTimestampRep =        new FixedTimestampRep(name)
  def forNumber(name: String): NumberRep =                        new NumberRep(name)
  def forText(name: String): TextRep =                            new TextRep(name)
  def forMoney(name: String): MoneyRep =                          new MoneyRep(name)
  def forBoolean(name: String): BooleanRep =                      new BooleanRep(name)
  def forVersion(name: String): VersionRep =                      new VersionRep(name)
  def forID(name: String): IDRep =                                new IDRep(name)
  def forArray(name: String): ArrayRep =                          new ArrayRep(name)
  def forDouble(name: String): DoubleRep =                        new DoubleRep(name)
  def forJson(name: String): JSONRep =                            new JSONRep(name)
  def forObject(name: String): ObjectRep =                        new ObjectRep(name)
  // scalastyle:off cyclomatic.complexity
  def toAttr(thing: (SoQLValue, ShapeRep[_ <: SoQLValue])) : Seq[Any] = thing match {
    case (value: SoQLPoint, intermediary: PointRep) => intermediary.toAttrValues(value)
    case (value: SoQLMultiPoint, intermediary: MultiPointRep) => intermediary.toAttrValues(value)
    case (value: SoQLLine, intermediary: LineRep) => intermediary.toAttrValues(value)
    case (value: SoQLMultiLine, intermediary: MultiLineRep) => intermediary.toAttrValues(value)
    case (value: SoQLPolygon, intermediary: PolygonRep) => intermediary.toAttrValues(value)
    case (value: SoQLMultiPolygon, intermediary: MultiPolygonRep) => intermediary.toAttrValues(value)
    case (value: SoQLDate, intermediary: DateRep) => intermediary.toAttrValues(value)
    case (value: SoQLTime, intermediary: TimeRep) => intermediary.toAttrValues(value)
    case (value: SoQLFloatingTimestamp, intermediary: FloatingTimestampRep) => intermediary.toAttrValues(value)
    case (value: SoQLFixedTimestamp, intermediary: FixedTimestampRep) => intermediary.toAttrValues(value)
    case (value: SoQLNumber, intermediary: NumberRep) => intermediary.toAttrValues(value)
    case (value: SoQLText, intermediary: TextRep) => intermediary.toAttrValues(value)
    case (value: SoQLMoney, intermediary: MoneyRep) => intermediary.toAttrValues(value)
    case (value: SoQLBoolean, intermediary: BooleanRep) => intermediary.toAttrValues(value)
    case (value: SoQLVersion, intermediary: VersionRep) => intermediary.toAttrValues(value)
    case (value: SoQLID, intermediary: IDRep) => intermediary.toAttrValues(value)
    case (value: SoQLArray, intermediary: ArrayRep) => intermediary.toAttrValues(value)
    case (value: SoQLDouble, intermediary: DoubleRep) => intermediary.toAttrValues(value)
    case (value: SoQLJson, intermediary: JSONRep) => intermediary.toAttrValues(value)
    case (value: SoQLObject, intermediary: ObjectRep) => intermediary.toAttrValues(value)
    case (SoQLNull, _) => Seq(null) // scalstyle:ignore null
    case (value: SoQLValue, _) =>
      log.error(s"Unknown SoQLValue: ${value.getClass()} - coercing toString but you should fix this!")
      Seq(value.toString)
  }
  // scalastyle:on cyclomatic.complexity
}

