package com.socrata.geoexport.encoders

import java.io.{OutputStream, OutputStreamWriter, Writer}
import com.socrata.soql.SoQLPackIterator
import geoexceptions._
import geotypes._
import com.socrata.geoexport.intermediates.RepMapper
import com.socrata.geoexport.intermediates.ShapeRep
import com.socrata.soql.types._
import scala.language.existentials

trait RowMapper[T] {
  type ValueRep = (_ <: SoQLValue, ShapeRep[_ <: SoQLValue])

  def splitOnGeo(repMapper: RepMapper, schema: Schema, fields: Fields): (ValueRep, Seq[ValueRep]) = {
    val (geoms, attrs) = schema
      .zip(fields)
      .map { case (column, value) => (value, ShapeRep.repFor(column, repMapper)) }
      .partition { case (_value, intermediate) => intermediate.isGeometry }

    val geomAttr = geoms match {
      case Seq(g) => g
      case _ => throw new MultipleGeometriesFoundException("Too many geometry columns!")
    }

    (geomAttr, attrs)
  }

  def serialize(layers: Layers, writer: OutputStreamWriter): Unit = {
    writer.write(prefix)
    // no scala. how about *you* avoid using null in the scala XML API
    layers.zipWithIndex.foreach { case (layer, index) =>
      toLayer(layer, writer, index < layers.size - 1)
    }
    writer.write(suffix)
  }

  def toLayer(layer: SoQLPackIterator, writer: OutputStreamWriter, moreLayers: Boolean): Unit = {
    writer.write(layerPrefix)
    layer.foreach { feature: Array[SoQLValue] =>
      val row = toRow(layer.schema, feature)
      writeRow(row, writer, layer.hasNext)
    }
    writer.write(layerSuffix(moreLayers))
  }

  protected def prefix: String
  protected def suffix: String

  protected def layerPrefix: String = ""
  protected def layerSuffix(moreLayers: Boolean): String = ""

  protected def toRow(schema: Schema, fields: Fields): T
  protected def writeRow(thing: T, writer: OutputStreamWriter, moreFeatures: Boolean): Unit
}
