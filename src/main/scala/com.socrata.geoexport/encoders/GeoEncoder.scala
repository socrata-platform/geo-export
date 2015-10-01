package com.socrata.geoexport.encoders

import java.io.OutputStream

import com.socrata.soql.SoQLPackIterator
import scala.util.Try

trait GeoEncoder {
  def encode(layers: Iterable[SoQLPackIterator], outStream: OutputStream) : Try[OutputStream]
  def encodes: Set[String]
  def encodedMIME: String
}
