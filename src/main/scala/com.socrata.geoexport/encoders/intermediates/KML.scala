package com.socrata.geoexport.intermediates
package kml

import java.util.Date

import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import scala.xml.Node


case class KMLTranslationException(message: String) extends Exception

abstract class KMLRep[T](soqlName: String) extends ShapeRep[T] {
  protected def normalizeName(name: String) = {
    name
  }

  def toAttrBindings = throw new KMLTranslationException("KML doesn't use bindings.")
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

  def isGeometry = true
}

//At the moment, none of the KML elements split data so..
trait SimpleDatum {
  def asSimpleData(item: String): Seq[Node] = {
    toAttrNames.zip(List(item)).map { case (name, value) =>
      <SimpleData name={name}>{value}</SimpleData>
    }
  }
  def toAttrNames: Seq[String]
  def isGeometry = false
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

class TextRep(soqlName: String) extends KMLRep[SoQLText](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLText): Seq[Any] = asSimpleData(soql.value.toString)
}

class MoneyRep(soqlName: String) extends KMLRep[SoQLMoney](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLMoney): Seq[Any] = asSimpleData(soql.value.toString)
}

class BooleanRep(soqlName: String) extends KMLRep[SoQLBoolean](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLBoolean): Seq[Any] = asSimpleData(soql.value.toString)
}

class VersionRep(soqlName: String) extends KMLRep[SoQLVersion](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLVersion): Seq[Any] = asSimpleData(soql.value.toString)
}

class IDRep(soqlName: String) extends KMLRep[SoQLID](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLID): Seq[Any] = asSimpleData(soql.value.toString)
}

object KMLRepMapper extends RepMapper {
  def forPoint(name: String) =              new PointRep(name)
  def forMultiPoint(name: String) =         new MultiPointRep(name)
  def forLine(name: String) =               new LineRep(name)
  def forMultiLine(name: String) =          new MultiLineRep(name)
  def forPolygon(name: String) =            new PolygonRep(name)
  def forMultiPolygon(name: String) =       new MultiPolygonRep(name)
  def forDate(name: String) =               new DateRep(name)
  def forTime(name: String) =               new TimeRep(name)
  def forFloatingTimestamp(name: String) =  new FloatingTimestampRep(name)
  def forFixedTimestamp(name: String) =     new FixedTimestampRep(name)
  def forNumber(name: String) =             new NumberRep(name)
  def forText(name: String) =               new TextRep(name)
  def forMoney(name: String) =              new MoneyRep(name)
  def forBoolean(name: String) =            new BooleanRep(name)
  def forVersion(name: String) =            new VersionRep(name)
  def forID(name: String) =                 new IDRep(name)
  def forArray(name: String) = ???
  def forDouble(name: String) = ???
  def forJson(name: String) = ???
  def forObject(name: String) = ???


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
    case _ => ???
  }
}

