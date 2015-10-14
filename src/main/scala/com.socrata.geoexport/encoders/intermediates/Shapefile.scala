package com.socrata.geoexport.intermediates.shapefile

import java.util.Date
import com.rojoma.json.v3.ast._
import com.socrata.geoexport.intermediates.ShapeRep
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import com.socrata.geoexport.intermediates._
import org.slf4j.LoggerFactory
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

/**
  Here be the intermediate representations used to map SoQLColumns and SoQLValues
  into things that we can store in a shapefile/dbf by way of geotools

  See the note above ShapefileRepMapper for why this stuff needs to exist
*/
abstract class ShapefileRep[T](name: String) extends ShapeRep[T] {
  val maxDBFColumnLength = 10

  protected def normalizeName(name: String) = {
    val n = name.replaceAll(":", "")
    if (n.length > maxDBFColumnLength) n.substring(0, maxDBFColumnLength) else n
  }

  def toAttrNames: Seq[String] = Seq(normalizeName(name))
}

trait GeoDatum {
  def isGeometry: Boolean = true
}

trait DBFDatum {
  def isGeometry: Boolean = false
}

trait SplitDatetime {
  def splitNames(soqlName: String, normalizer: String => String): Seq[String] = {
    val dateName = s"date_${soqlName}"
    val timeName = s"time_${soqlName}"
    Seq(normalizer(dateName), normalizer(timeName))
  }
}

class PointRep(soqlName: String) extends ShapefileRep[SoQLPoint](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Point])
  def toAttrValues(soql: SoQLPoint): Seq[AnyRef] = Seq(soql.value)
}

class MultiPointRep(soqlName: String) extends ShapefileRep[SoQLMultiPoint](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPoint])
  def toAttrValues(soql: SoQLMultiPoint): Seq[AnyRef] = Seq(soql.value)
}

class LineRep(soqlName: String) extends ShapefileRep[SoQLLine](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[LineString])
  def toAttrValues(soql: SoQLLine): Seq[AnyRef] = Seq(soql.value)
}

class MultiLineRep(soqlName: String) extends ShapefileRep[SoQLMultiLine](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiLineString])
  def toAttrValues(soql: SoQLMultiLine): Seq[AnyRef] = Seq(soql.value)
}

class PolygonRep(soqlName: String) extends ShapefileRep[SoQLPolygon](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Polygon])
  def toAttrValues(soql: SoQLPolygon): Seq[AnyRef] = Seq(soql.value)
}

class MultiPolygonRep(soqlName: String) extends ShapefileRep[SoQLMultiPolygon](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPolygon])
  def toAttrValues(soql: SoQLMultiPolygon): Seq[AnyRef] = Seq(soql.value)
}

class DateRep(soqlName: String) extends ShapefileRep[SoQLDate](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date])
  def toAttrValues(soql: SoQLDate): Seq[AnyRef] = Seq(soql.value.toDate)
}

class TimeRep(soqlName: String) extends ShapefileRep[SoQLTime](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
  def toAttrValues(soql: SoQLTime): Seq[AnyRef] = Seq(SoQLTime.StringRep(soql.value))
}

class FloatingTimestampRep(soqlName: String)
  extends ShapefileRep[SoQLFloatingTimestamp](soqlName: String) with SplitDatetime with DBFDatum {

  override def toAttrNames: Seq[String] = this.splitNames(soqlName, normalizeName)
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date], classOf[String])
  def toAttrValues(soql: SoQLFloatingTimestamp): Seq[AnyRef] = {
    val date = soql.value.toDate()
    val time = SoQLTime.StringRep(soql.value.toLocalTime)
    Seq(date, time)
  }
}

class FixedTimestampRep(soqlName: String)
  extends ShapefileRep[SoQLFixedTimestamp](soqlName: String) with SplitDatetime with DBFDatum {

  override def toAttrNames: Seq[String] = this.splitNames(soqlName, normalizeName)
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Date], classOf[String])
  def toAttrValues(soql: SoQLFixedTimestamp): Seq[AnyRef] = {
    val utc = new DateTime(soql.value, DateTimeZone.UTC)
    val date = utc.toDate()
    val time = SoQLTime.StringRep(utc.toLocalTime)
    Seq(date, time)
  }
}

class NumberRep(soqlName: String) extends ShapefileRep[SoQLNumber](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[BigDecimal])
  def toAttrValues(soql: SoQLNumber): Seq[AnyRef] = Seq(soql.value)
}

class TextRep(soqlName: String) extends ShapefileRep[SoQLText](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
  def toAttrValues(soql: SoQLText): Seq[AnyRef] = Seq(soql.value)
}

class MoneyRep(soqlName: String) extends ShapefileRep[SoQLMoney](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[BigDecimal])
  def toAttrValues(soql: SoQLMoney): Seq[AnyRef] = Seq(soql.value)
}

class BooleanRep(soqlName: String) extends ShapefileRep[SoQLBoolean](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.Boolean])
  def toAttrValues(soql: SoQLBoolean): Seq[AnyRef] = Seq(soql.value: java.lang.Boolean)
}

// These are both Longs, but you can't represent a Long this large in a DBF,
// so they'll need to be strings
class VersionRep(soqlName: String)
  extends ShapefileRep[SoQLVersion](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.String])
  def toAttrValues(soql: SoQLVersion): Seq[AnyRef] = Seq(soql.value.toString)
}

class IDRep(soqlName: String) extends ShapefileRep[SoQLID](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.String])
  def toAttrValues(soql: SoQLID): Seq[AnyRef] = Seq(soql.value.toString)
}
class DoubleRep(soqlName: String) extends ShapefileRep[SoQLDouble](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.Double])
  def toAttrValues(soql: SoQLDouble): Seq[AnyRef] = Seq(soql.value: java.lang.Double)
}

// Because there is no way to encode arbitrary types with a rigid schema that DBF enforces,
// just serialize complex types to strings in JSON form. KML gets away with this because SimpleDatums
// can be different
class ArrayRep(soqlName: String) extends ShapefileRep[SoQLArray](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.String])
  def toAttrValues(soql: SoQLArray): Seq[AnyRef] = Seq(soql.value.toString)
}
class JSONRep(soqlName: String) extends ShapefileRep[SoQLJson](soqlName: String) with DBFDatum {
  def toAttrValues(soql: SoQLJson): Seq[AnyRef] = Seq(soql.value.toString)
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
}
class ObjectRep(soqlName: String) extends ShapefileRep[SoQLObject](soqlName: String) with DBFDatum {
  def toAttrValues(soql: SoQLObject): Seq[AnyRef] = Seq(soql.value.toString)
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
}

/**
  This is a thing to map all the SoQLColumns to an intermediate column which will facilitate
  the translation into something that will map onto the constraints of a DBF file easily.
  Everything returns a Seq() because a single SoQLValue must be mapped (split) into
  multiple DBF columns. DBF only allows you to represent...

    {:code, :name, :size}

    {"C", "Character", 255},
    {"N", "Number", 18},
    {"L", "Logical", 1},
    {"D", "Date",  8},
    {"F", "Float",  20},
    {"O", "Double",  8}

    Ah, May of 1984. Things were simpler back then.


  For example, since DBF only allows you to store a Date, SoQLDateTime* columns need to
  be mapped onto two columns, Date and Time, where Time is a Character column. That's why
  everything here returns Sequences. The encoder will then map the SoQLSchema to the intermediate
  schema, and then flatten the result into something that we can put in a DBF.
*/
object ShapefileRepMapper extends RepMapper {
  lazy val log = LoggerFactory.getLogger(getClass)

  def forPoint(name: String): PointRep =                            new PointRep(name)
  def forMultiPoint(name: String): MultiPointRep =                  new MultiPointRep(name)
  def forLine(name: String): LineRep =                              new LineRep(name)
  def forMultiLine(name: String): MultiLineRep =                    new MultiLineRep(name)
  def forPolygon(name: String): PolygonRep =                        new PolygonRep(name)
  def forMultiPolygon(name: String): MultiPolygonRep =              new MultiPolygonRep(name)
  def forDate(name: String): DateRep =                              new DateRep(name)
  def forTime(name: String): TimeRep =                              new TimeRep(name)
  def forFloatingTimestamp(name: String): FloatingTimestampRep =    new FloatingTimestampRep(name)
  def forFixedTimestamp(name: String): FixedTimestampRep =          new FixedTimestampRep(name)
  def forNumber(name: String): NumberRep =                          new NumberRep(name)
  def forText(name: String): TextRep =                              new TextRep(name)
  def forMoney(name: String): MoneyRep =                            new MoneyRep(name)
  def forBoolean(name: String): BooleanRep =                        new BooleanRep(name)
  def forVersion(name: String): VersionRep =                        new VersionRep(name)
  def forID(name: String): IDRep =                                  new IDRep(name)
  def forArray(name: String): ArrayRep =                            new ArrayRep(name)
  def forDouble(name: String): DoubleRep =                          new DoubleRep(name)
  def forJson(name: String): JSONRep =                              new JSONRep(name)
  def forObject(name: String): ObjectRep =                          new ObjectRep(name)
  // scalastyle:off
  def toAttr(thing: (SoQLValue, ShapeRep[_ <: SoQLValue])) : Seq[AnyRef] = thing match {
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
    case (SoQLNull, _) => Seq(null) // scalastyle:ignore null
    case (value: SoQLValue, _) =>
      log.error(s"Unknown SoQLValue: ${value.getClass()} - coercing toString but you should fix this!")
      Seq(value.toString: java.lang.String)
    // scalastyle:on
  }
}

