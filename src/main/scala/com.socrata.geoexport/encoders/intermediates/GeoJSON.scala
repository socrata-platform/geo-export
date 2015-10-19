package com.socrata.geoexport.intermediates.geojson

import com.rojoma.json.v3.ast._
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.slf4j.LoggerFactory
import com.socrata.geoexport.intermediates.{ShapeRep, RepMapper}
import com.socrata.thirdparty.geojson.JtsCodecs.geoCodec


case class GeoJSONTranslationException(message: String) extends Exception


abstract class GeoJSONRep[T](soqlName: String) extends ShapeRep[T] {
  protected def normalizeName(name: String) = name.replaceAll(":", "")

  def toAttrBindings: Seq[Class[_]] = throw new GeoJSONTranslationException("geoJSON doesn't use bindings.")
  def toAttrNames: Seq[String] = Seq(normalizeName(soqlName))
}




trait SimpleDatum {
  def asSimpleData(item: String): Seq[JValue] = Seq(JString(item))
  def toAttrNames: Seq[String]
  def isGeometry: Boolean = false
}


trait GeoDatum {
  def isGeometry: Boolean = true
}


class PointRep(soqlName: String) extends GeoJSONRep[SoQLPoint](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLPoint): Seq[JValue] = soql match {
    case SoQLPoint(p) => Seq(geoCodec.encode(p))
  }
}

class MultiPointRep(soqlName: String) extends GeoJSONRep[SoQLMultiPoint](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLMultiPoint): Seq[JValue] = soql match {
    case SoQLMultiPoint(p) => Seq(geoCodec.encode(p))
  }
}

class LineRep(soqlName: String) extends GeoJSONRep[SoQLLine](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLLine): Seq[JValue] = soql match {
    case SoQLLine(p) => Seq(geoCodec.encode(p))
  }
}

class MultiLineRep(soqlName: String) extends GeoJSONRep[SoQLMultiLine](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLMultiLine): Seq[JValue] = soql match {
    case SoQLMultiLine(p) => Seq(geoCodec.encode(p))
  }
}

class PolygonRep(soqlName: String) extends GeoJSONRep[SoQLPolygon](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLPolygon): Seq[JValue] = soql match {
    case SoQLPolygon(p) => Seq(geoCodec.encode(p))
  }
}

class MultiPolygonRep(soqlName: String) extends GeoJSONRep[SoQLMultiPolygon](soqlName) with GeoDatum {
  def toAttrValues(soql: SoQLMultiPolygon): Seq[JValue] = soql match {
    case SoQLMultiPolygon(p) => Seq(geoCodec.encode(p))
  }
}

class DateRep(soqlName: String) extends GeoJSONRep[SoQLDate](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLDate): Seq[JValue] = asSimpleData(SoQLDate.StringRep(soql.value))
}

class TimeRep(soqlName: String) extends GeoJSONRep[SoQLTime](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLTime): Seq[JValue] = asSimpleData(SoQLTime.StringRep(soql.value))
}

class FloatingTimestampRep(soqlName: String) extends GeoJSONRep[SoQLFloatingTimestamp](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLFloatingTimestamp): Seq[JValue] = asSimpleData(SoQLFloatingTimestamp.StringRep(soql.value))
}

class FixedTimestampRep(soqlName: String) extends GeoJSONRep[SoQLFixedTimestamp](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLFixedTimestamp): Seq[JValue] = asSimpleData(SoQLFixedTimestamp.StringRep(soql.value))
}

class NumberRep(soqlName: String) extends GeoJSONRep[SoQLNumber](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLNumber): Seq[JValue] = asSimpleData(soql.value.toString)
}

class TextRep(soqlName: String) extends GeoJSONRep[SoQLText](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLText): Seq[JValue] = asSimpleData(soql.value.toString)
}

class MoneyRep(soqlName: String) extends GeoJSONRep[SoQLMoney](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLMoney): Seq[JValue] = asSimpleData(soql.value.toString)
}

class BooleanRep(soqlName: String) extends GeoJSONRep[SoQLBoolean](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLBoolean): Seq[JValue] = asSimpleData(soql.value.toString)
}

class VersionRep(soqlName: String) extends GeoJSONRep[SoQLVersion](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLVersion): Seq[JValue] = asSimpleData(soql.value.toString)
}

class IDRep(soqlName: String) extends GeoJSONRep[SoQLID](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLID): Seq[JValue] = asSimpleData(soql.value.toString)
}

class DoubleRep(soqlName: String) extends GeoJSONRep[SoQLDouble](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLDouble): Seq[JValue] = asSimpleData(soql.value.toString)
}

class ArrayRep(soqlName: String) extends GeoJSONRep[SoQLArray](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLArray): Seq[JValue] = Seq(soql.value)
}

class JSONRep(soqlName: String) extends GeoJSONRep[SoQLJson](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLJson): Seq[JValue] = Seq(soql.value)
}

class ObjectRep(soqlName: String) extends GeoJSONRep[SoQLObject](soqlName) with SimpleDatum {
  def toAttrValues(soql: SoQLObject): Seq[JValue] = Seq(soql.value)
}

/**
  Maps all the SoQLColumns to intermediate columns
*/
object GeoJSONRepMapper extends RepMapper {
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
  def toAttr(thing: (SoQLValue, ShapeRep[_ <: SoQLValue])) : Seq[JValue] = thing match {
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
      Seq(JString(value.toString))
  }
  // scalastyle:on cyclomatic.complexity
}

