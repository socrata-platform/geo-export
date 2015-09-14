package com.socrata.geoexport.intermediates.shapefile

import java.util.Date
import com.socrata.geoexport.intermediates.ShapeRep
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import com.socrata.geoexport.intermediates._

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat


abstract class ShapefileRep[T](name: String) extends ShapeRep[T] {
  val maxDBFColumnLength = 10

  protected def normalizeName(name: String) = {
    if (name.length > maxDBFColumnLength) name.substring(0, maxDBFColumnLength) else name
  }

  def toAttrNames: Seq[String] = Seq(normalizeName(name))

}

trait SplitDatetime {
  def splitNames(soqlName: String, normalizer: String => String): Seq[String] = {
    val dateName = s"date_${soqlName}"
    val timeName = s"time_${soqlName}"
    Seq(normalizer(dateName), normalizer(timeName))
  }
}

class PointRep(soqlName: String) extends ShapefileRep[SoQLPoint](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Point])
  def toAttrValues(soql: SoQLPoint): Seq[Object] = Seq(soql.value)
  def isGeometry = true
}

class MultiPointRep(soqlName: String) extends ShapefileRep[SoQLMultiPoint](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPoint])
  def toAttrValues(soql: SoQLMultiPoint): Seq[Object] = Seq(soql.value)
  def isGeometry = true
}

class LineRep(soqlName: String) extends ShapefileRep[SoQLLine](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[LineString])
  def toAttrValues(soql: SoQLLine): Seq[Object] = Seq(soql.value)
  def isGeometry = true
}

class MultiLineRep(soqlName: String) extends ShapefileRep[SoQLMultiLine](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiLineString])
  def toAttrValues(soql: SoQLMultiLine): Seq[Object] = Seq(soql.value)
  def isGeometry = true
}

class PolygonRep(soqlName: String) extends ShapefileRep[SoQLPolygon](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Polygon])
  def toAttrValues(soql: SoQLPolygon): Seq[Object] = Seq(soql.value)
  def isGeometry = true
}

class MultiPolygonRep(soqlName: String) extends ShapefileRep[SoQLMultiPolygon](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPolygon])
  def toAttrValues(soql: SoQLMultiPolygon): Seq[Object] = Seq(soql.value)
  def isGeometry = true
}

class DateRep(soqlName: String) extends ShapefileRep[SoQLDate](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date])
  def toAttrValues(soql: SoQLDate): Seq[Object] = Seq(soql.value.toDate)
  def isGeometry = false
}

class TimeRep(soqlName: String) extends ShapefileRep[SoQLTime](soqlName: String) with StringTimeRep {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
  def isGeometry = false
}


class FloatingTimestampRep(soqlName: String) extends ShapefileRep[SoQLFloatingTimestamp](soqlName: String) with SplitDatetime {
  override def toAttrNames: Seq[String] = this.splitNames(soqlName, normalizeName)
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date], classOf[String])
  def toAttrValues(soql: SoQLFloatingTimestamp): Seq[Object] = {
    val date = soql.value.toDate()
    val fmt = DateTimeFormat.forPattern(this.timeFormat)
    val time = fmt.print(soql.value)

    Seq(date, time)
  }
  def isGeometry = false
}

class FixedTimestampRep(soqlName: String) extends ShapefileRep[SoQLFixedTimestamp](soqlName: String) with SplitDatetime {
  override def toAttrNames: Seq[String] = this.splitNames(soqlName, normalizeName)
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date], classOf[String])
  def toAttrValues(soql: SoQLFixedTimestamp): Seq[Object] = {
    val utc = new DateTime(soql.value, DateTimeZone.UTC)
    val date = utc.toDate()
    val fmt = DateTimeFormat.forPattern(this.timeFormat)
    val time = fmt.print(utc)
    Seq(date, time)
  }
  def isGeometry = false
}

class NumberRep(soqlName: String) extends ShapefileRep[SoQLNumber](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[BigDecimal])
  def toAttrValues(soql: SoQLNumber): Seq[Object] = Seq(soql.value)
  def isGeometry = false
}

class TextRep(soqlName: String) extends ShapefileRep[SoQLText](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
  def toAttrValues(soql: SoQLText): Seq[Object] = Seq(soql.value)
  def isGeometry = false
}

class MoneyRep(soqlName: String) extends ShapefileRep[SoQLMoney](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[BigDecimal])
  def toAttrValues(soql: SoQLMoney): Seq[Object] = Seq(soql.value)
  def isGeometry = false
}

class BooleanRep(soqlName: String) extends ShapefileRep[SoQLBoolean](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.Boolean])
  def toAttrValues(soql: SoQLBoolean): Seq[Object] = Seq(soql.value: java.lang.Boolean)
  def isGeometry = false
}

// These are both Longs, but you can't represent a Long this large in a DBF,
// so they'll need to be strings
class VersionRep(soqlName: String) extends ShapefileRep[SoQLVersion](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.String])
  def toAttrValues(soql: SoQLVersion): Seq[Object] = Seq(soql.value.toString)
  def isGeometry = false
}

class IDRep(soqlName: String) extends ShapefileRep[SoQLID](soqlName: String) {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.String])
  def toAttrValues(soql: SoQLID): Seq[Object] = Seq(soql.value.toString)
  def isGeometry = false
}



object ShapefileRepMapper extends RepMapper {
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

  def toAttr(thing: (SoQLValue, ShapeRep[_ <: SoQLValue])) : Seq[Object] = thing match {
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

