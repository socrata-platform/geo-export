package com.socrata.geoexport.intermediates

import com.socrata.soql.types._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

/**
  the object ShapeRep is what's used by the encoder to translate formats.

  ShapeRep the abstract class is implemented for a specific export format
  for each SoQLType which will translate names/values to those representable
  by the export format

  RepMapper is a gross workaround for the lack of dependent typing i guess.
*/

case class UnknownSoQLTypeException(message: String) extends Exception

abstract class ShapeRep[T] {
  def toAttrNames: Seq[String]
  def toAttrBindings: Seq[Class[_]]
  def toAttrValues(soql: T): Seq[Any]
  def isGeometry: Boolean
}

trait RepMapper {
  def forPoint(name: String): ShapeRep[SoQLPoint]
  def forMultiPoint(name: String): ShapeRep[SoQLMultiPoint]
  def forLine(name: String): ShapeRep[SoQLLine]
  def forMultiLine(name: String): ShapeRep[SoQLMultiLine]
  def forPolygon(name: String): ShapeRep[SoQLPolygon]
  def forMultiPolygon(name: String): ShapeRep[SoQLMultiPolygon]
  def forDate(name: String): ShapeRep[SoQLDate]
  def forTime(name: String): ShapeRep[SoQLTime]
  def forFloatingTimestamp(name: String): ShapeRep[SoQLFloatingTimestamp]
  def forFixedTimestamp(name: String): ShapeRep[SoQLFixedTimestamp]
  def forNumber(name: String): ShapeRep[SoQLNumber]
  def forText(name: String): ShapeRep[SoQLText]
  def forMoney(name: String): ShapeRep[SoQLMoney]
  def forBoolean(name: String): ShapeRep[SoQLBoolean]
  def forVersion(name: String): ShapeRep[SoQLVersion]
  def forID(name: String): ShapeRep[SoQLID]
  def forArray(name: String): ShapeRep[SoQLArray]
  def forDouble(name: String): ShapeRep[SoQLDouble]
  def forJson(name: String): ShapeRep[SoQLJson]
  def forObject(name: String): ShapeRep[SoQLObject]

  def toAttr(thing: (SoQLValue, ShapeRep[_ <: SoQLValue])) : Seq[Any]
}


object ShapeRep {
  // scalastyle:off
  def repFor(col: (String, SoQLType), repMapper: RepMapper): ShapeRep[_ <: SoQLValue] = col match {
    case (name, SoQLPoint) => repMapper.forPoint(name)
    case (name, SoQLMultiPoint) => repMapper.forMultiPoint(name)
    case (name, SoQLLine) => repMapper.forLine(name)
    case (name, SoQLMultiLine) => repMapper.forMultiLine(name)
    case (name, SoQLPolygon) => repMapper.forPolygon(name)
    case (name, SoQLMultiPolygon) => repMapper.forMultiPolygon(name)
    case (name, SoQLDate) => repMapper.forDate(name)
    case (name, SoQLTime) => repMapper.forTime(name)
    case (name, SoQLFloatingTimestamp) => repMapper.forFloatingTimestamp(name)
    case (name, SoQLFixedTimestamp) => repMapper.forFixedTimestamp(name)
    case (name, SoQLNumber) => repMapper.forNumber(name)
    case (name, SoQLText) => repMapper.forText(name)
    case (name, SoQLMoney) => repMapper.forMoney(name)
    case (name, SoQLBoolean) => repMapper.forBoolean(name)
    case (name, SoQLVersion) => repMapper.forVersion(name)
    case (name, SoQLID) => repMapper.forID(name)
    case (name, SoQLArray) => repMapper.forArray(name)
    case (name, SoQLDouble) => repMapper.forDouble(name)
    case (name, SoQLJson) => repMapper.forJson(name)
    case (name, SoQLObject) => repMapper.forObject(name)
    case unknown: Any => throw new UnknownSoQLTypeException(s"Unknown SoQLType ${unknown}")
  }
  // scalastyle:on
}
