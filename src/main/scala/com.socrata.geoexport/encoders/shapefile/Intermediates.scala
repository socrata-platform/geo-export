package com.socrata.geoexport.shapefile.intermediates

import java.util.Date

import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat


abstract class ShapeRep[T] {
  val timeFormat = "HH:mm:ss.SSS"
  val maxDBFColumnLength = 10

  protected def normalizeName(name: String) = {
    if (name.length > maxDBFColumnLength) name.substring(0, maxDBFColumnLength) else name
  }
  def toAttrNames: Seq[String]
  def toAttrBindings: Seq[Class[_]]
  def toAttrValues(soql: T): Seq[Object]
}

class PointRep(soqlName: String) extends ShapeRep[SoQLPoint] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Point])
  def toAttrValues(soql: SoQLPoint): Seq[Object] = Seq(soql.value)
}

class MultiPointRep(soqlName: String) extends ShapeRep[SoQLMultiPoint] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPoint])
  def toAttrValues(soql: SoQLMultiPoint): Seq[Object] = Seq(soql.value)
}

class LineRep(soqlName: String) extends ShapeRep[SoQLLine] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[LineString])
  def toAttrValues(soql: SoQLLine): Seq[Object] = Seq(soql.value)
}

class MultiLineRep(soqlName: String) extends ShapeRep[SoQLMultiLine] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiLineString])
  def toAttrValues(soql: SoQLMultiLine): Seq[Object] = Seq(soql.value)
}

class PolygonRep(soqlName: String) extends ShapeRep[SoQLPolygon] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Polygon])
  def toAttrValues(soql: SoQLPolygon): Seq[Object] = Seq(soql.value)
}

class MultiPolygonRep(soqlName: String) extends ShapeRep[SoQLMultiPolygon] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPolygon])
  def toAttrValues(soql: SoQLMultiPolygon): Seq[Object] = Seq(soql.value)
}

class DateRep(soqlName: String) extends ShapeRep[SoQLDate] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date])
  def toAttrValues(soql: SoQLDate): Seq[Object] = Seq(soql.value.toDate)
}

class TimeRep(soqlName: String) extends ShapeRep[SoQLTime] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
  def toAttrValues(soql: SoQLTime): Seq[Object] = {
    val utc = soql.value.toDateTimeToday(DateTimeZone.UTC)
    val fmt = DateTimeFormat.forPattern(this.timeFormat)
    val time = fmt.print(utc)
    Seq(time)
  }
}


class FloatingTimestampRep(soqlName: String) extends ShapeRep[SoQLFloatingTimestamp] {
  def toAttrNames: Seq[String] = {
    val dateName = s"date_${soqlName}"
    val timeName = s"time_${soqlName}"
    Seq(normalizeName(dateName), normalizeName(timeName))
  }
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date], classOf[String])
  def toAttrValues(soql: SoQLFloatingTimestamp): Seq[Object] = {
    val date = soql.value.toDate()
    val fmt = DateTimeFormat.forPattern(this.timeFormat)
    val time = fmt.print(soql.value)

    Seq(date, time)
  }
}

class FixedTimestampRep(soqlName: String) extends ShapeRep[SoQLFixedTimestamp] {
  def toAttrNames: Seq[String] = {
    val dateName = s"date_${soqlName}"
    val timeName = s"time_${soqlName}"
    Seq(normalizeName(dateName), normalizeName(timeName))
  }
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date], classOf[String])
  def toAttrValues(soql: SoQLFixedTimestamp): Seq[Object] = {
    val utc = new DateTime(soql.value, DateTimeZone.UTC)
    val date = utc.toDate()
    val fmt = DateTimeFormat.forPattern(this.timeFormat)
    val time = fmt.print(utc)
    Seq(date, time)
  }
}

class NumberRep(soqlName: String) extends ShapeRep[SoQLNumber] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[BigDecimal])
  def toAttrValues(soql: SoQLNumber): Seq[Object] = Seq(soql.value)
}

class TextRep(soqlName: String) extends ShapeRep[SoQLText] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
  def toAttrValues(soql: SoQLText): Seq[Object] = Seq(soql.value)
}

class MoneyRep(soqlName: String) extends ShapeRep[SoQLMoney] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[BigDecimal])
  def toAttrValues(soql: SoQLMoney): Seq[Object] = Seq(soql.value)
}

class BooleanRep(soqlName: String) extends ShapeRep[SoQLBoolean] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.Boolean])
  def toAttrValues(soql: SoQLBoolean): Seq[Object] = Seq(soql.value: java.lang.Boolean)
}

// These are both Longs, but you can't represent a Long this large in a DBF,
// so they'll need to be strings
class VersionRep(soqlName: String) extends ShapeRep[SoQLVersion] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.String])
  def toAttrValues(soql: SoQLVersion): Seq[Object] = Seq(soql.value.toString)
}

class IDRep(soqlName: String) extends ShapeRep[SoQLID] {
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.String])
  def toAttrValues(soql: SoQLID): Seq[Object] = Seq(soql.value.toString)
}
