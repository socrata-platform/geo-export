package com.socrata.geoexport.intermediates.shapefile

import java.nio.charset.CodingErrorAction
import java.util.Date
import com.socrata.geoexport.intermediates.ShapeRep
import com.socrata.soql.types._
import org.locationtech.jts.geom._
import org.locationtech.jts.io.{WKBReader, WKBWriter}
import com.vividsolutions.jts.{geom => vgeom}
import com.vividsolutions.jts.{io => vio}
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
  protected def normalizeName(name: String) = {
    name.replaceAll(":", "") // we'll truncate to 10 chars later
  }

  def toAttrNames: Seq[String] = Seq(normalizeName(name))
}

trait GeoDatum {
  def isGeometry: Boolean = true
}
object GeoDatum {
  private def threadLocal[T](init: => T) = new java.lang.ThreadLocal[T] {
    override def initialValue(): T = init
  }

  private val gf = threadLocal { new GeometryFactory }
  private val wkbReader_ = threadLocal { new WKBReader(gf.get) }
  private val wkbWriter_ = threadLocal { new WKBWriter }

  private val vGf = threadLocal { new vgeom.GeometryFactory }
  private val vWkbReader_ = threadLocal { new vio.WKBReader(vGf.get) }
  private val vWkbWriter_ = threadLocal { new vio.WKBWriter }

  private def wkbReader = wkbReader_.get
  private def wkbWriter = wkbWriter_.get
  private def vWkbReader = vWkbReader_.get
  private def vWkbWriter = vWkbWriter_.get

  // Typesafe conversion between locationtech geo types and
  // vividsolutions geo types (in geotools 20, they switched to
  // locationtech but we still use vividsolutions everywhere else -
  // specifically, in the soql geospatial types.  So this bundles up
  // the conversion in a typesafe way where if you put a static Point
  // (or whatever) in, you get a static Point (or whatever out).
  // Internally it's convert-to-wkb followed by read-from-wkb followed
  // by a cast, but that's all encapsulated here.
  trait GeoConvert[G <: Geometry] {
    type VG <: com.vividsolutions.jts.geom.Geometry
    def geom2vgeom(g: G): VG =
      vWkbReader.read(wkbWriter.write(g)).asInstanceOf[VG]
  }

  trait VGeoConvert[VG <: com.vividsolutions.jts.geom.Geometry] {
    type G <: Geometry
    def vgeom2geom(vg: VG): G =
      wkbReader.read(vWkbWriter.write(vg)).asInstanceOf[G]
  }

  // Oh, for a non-painful macro system
  implicit object GeometryConvert extends GeoConvert[Geometry] with VGeoConvert[vgeom.Geometry] {
    type G = Geometry
    type VG = vgeom.Geometry
  }
  implicit object PointConvert extends GeoConvert[Point] with VGeoConvert[vgeom.Point] {
    type G = Point
    type VG = vgeom.Point
  }
  implicit object MultiPointConvert extends GeoConvert[MultiPoint] with VGeoConvert[vgeom.MultiPoint] {
    type G = MultiPoint
    type VG = vgeom.MultiPoint
  }
  implicit object LineStringConvert extends GeoConvert[LineString] with VGeoConvert[vgeom.LineString] {
    type G = LineString
    type VG = vgeom.LineString
  }
  implicit object MultiLineStringConvert extends GeoConvert[MultiLineString] with VGeoConvert[vgeom.MultiLineString] {
    type G = MultiLineString
    type VG = vgeom.MultiLineString
  }
  implicit object PolygonConvert extends GeoConvert[Polygon] with VGeoConvert[vgeom.Polygon] {
    type G = Polygon
    type VG = vgeom.Polygon
  }
  implicit object MultiPolygonConvert extends GeoConvert[MultiPolygon] with VGeoConvert[vgeom.MultiPolygon] {
    type G = MultiPolygon
    type VG = vgeom.MultiPolygon
  }

  // These are the things you actually call to do the conversions.
  def vgeom2geom[VG <: vgeom.Geometry](vg: VG)(implicit ev: VGeoConvert[VG]): ev.G =
    ev.vgeom2geom(vg)

  def geom2vgeom[G <: Geometry](g: G)(implicit ev: GeoConvert[G]): ev.VG =
    ev.geom2vgeom(g)
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
  def toAttrValues(soql: SoQLPoint): Seq[AnyRef] = Seq(GeoDatum.vgeom2geom(soql.value))
}

class MultiPointRep(soqlName: String) extends ShapefileRep[SoQLMultiPoint](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPoint])
  def toAttrValues(soql: SoQLMultiPoint): Seq[AnyRef] = Seq(GeoDatum.vgeom2geom(soql.value))
}

class LineRep(soqlName: String) extends ShapefileRep[SoQLLine](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[LineString])
  def toAttrValues(soql: SoQLLine): Seq[AnyRef] = Seq(GeoDatum.vgeom2geom(soql.value))
}

class MultiLineRep(soqlName: String) extends ShapefileRep[SoQLMultiLine](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiLineString])
  def toAttrValues(soql: SoQLMultiLine): Seq[AnyRef] = Seq(GeoDatum.vgeom2geom(soql.value))
}

class PolygonRep(soqlName: String) extends ShapefileRep[SoQLPolygon](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[Polygon])
  def toAttrValues(soql: SoQLPolygon): Seq[AnyRef] = Seq(GeoDatum.vgeom2geom(soql.value))
}

class MultiPolygonRep(soqlName: String) extends ShapefileRep[SoQLMultiPolygon](soqlName: String) with GeoDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[MultiPolygon])
  def toAttrValues(soql: SoQLMultiPolygon): Seq[AnyRef] = Seq(GeoDatum.vgeom2geom(soql.value))
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
  def toAttrValues(soql: SoQLText): Seq[AnyRef] = {
    Seq(soql.value)
  }
}

class MoneyRep(soqlName: String) extends ShapefileRep[SoQLMoney](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[BigDecimal])
  def toAttrValues(soql: SoQLMoney): Seq[AnyRef] = Seq(soql.value)
}

class BooleanRep(soqlName: String) extends ShapefileRep[SoQLBoolean](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[java.lang.Boolean])
  def toAttrValues(soql: SoQLBoolean): Seq[AnyRef] = Seq(soql.value: java.lang.Boolean)
}

class UrlRep(soqlName: String) extends ShapefileRep[SoQLUrl](soqlName: String) with DBFDatum {
  def toAttrBindings: Seq[Class[_]] = Seq(classOf[String])
  def toAttrValues(soql: SoQLUrl): Seq[AnyRef] = {
    soql match {
      case SoQLUrl(None, None) => Seq("")
      case SoQLUrl(Some(url), None) => Seq(url.toString)
      case SoQLUrl(Some(url), Some(label)) => Seq(s"${label} (${url})")
    }
  }
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
  def forUrl(name: String): UrlRep =                                new UrlRep(name)
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
    case (value: SoQLUrl, intermediary: UrlRep) => intermediary.toAttrValues(value)
    case (SoQLNull, intermediary) => intermediary.toAttrNames.map{ _name => null} // scalastyle:ignore null
    case (value: SoQLValue, _) =>
      log.error(s"Unknown SoQLValue: ${value.getClass()} - coercing toString but you should fix this!")
      Seq(value.toString: java.lang.String)
    // scalastyle:on
  }
}

