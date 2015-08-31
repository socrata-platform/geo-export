package com.socrata.geoexport

import com.rojoma.json.v3.ast.{JArray, JValue}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.{Strategy, JsonKeyStrategy, AutomaticJsonCodecBuilder}

import scala.runtime.AbstractFunction1

object CJson {

  case class Field(c: String, t: JValue)
  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  @JsonKeyStrategy(Strategy.Underscore)
  case class Schema(
                     approximateRowCount: Option[Long],
                     dataVersion: Option[Long],
                     lastModified: Option[String],
                     locale: String,
                     rowCount: Option[Long],
                     schema: Seq[Field]
                     )
  private implicit val schemaCodec = AutomaticJsonCodecBuilder[Schema]

  def decode(data: Iterator[JValue]): Result = {
    if(!data.hasNext) return NoSchemaPresent
    val schemaish = data.next()
    JsonDecode.fromJValue[Schema](schemaish) match {
      case Right(schema) =>
        class Mapper extends AbstractFunction1[JValue, JArray] {
          def apply(row: JValue) = row match {
            case elems: JArray => elems
            case nope => throw new RowWasNotAnArray(nope)
          }
        }
        Decoded(schema, data.map(new Mapper))
      case Left(_) =>
        CannotDecodeSchema(schemaish)
    }
  }

  sealed abstract class Result
  case class Decoded(schema: Schema, rows: Iterator[JArray]) extends Result
  case object NoSchemaPresent extends Result
  case class CannotDecodeSchema(value: JValue) extends Result

  case class RowWasNotAnArray(value: JValue) extends Exception(s"Row was not an array: $value")
  case class RowIncorrectLength(got: Int, expected: Int)
    extends Exception(s"Incorrect number of elements in row; expected $expected, got $got")
  case class UndecodableValue(got: JValue, expected: JValue)
    extends Exception(s"Undecodable json value: ${got} (${got.jsonType}), " +
                      s"expected SoQLType: ${expected.toString}")
}