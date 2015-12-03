package com.socrata.geoexport.encoders

import java.io.OutputStream

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.soql.SoQLPackIterator
import scala.util.Try

trait GeoEncoder {
  def encode(rs: ResourceScope, layers: Iterable[SoQLPackIterator], outStream: OutputStream) : Try[OutputStream]
  def encodes: Set[String]
  def encodedMIME: String
}
