package com.socrata.geoexport.encoders

import java.io.OutputStream

import com.socrata.soql.SoQLPackIterator
import com.socrata.thirdparty.geojson.FeatureCollectionJson
import org.opengis.feature.simple.SimpleFeature
import scala.util.{Try, Success, Failure}

trait GeoEncoder {
  def encode(layers: Iterable[SoQLPackIterator], outStream: OutputStream) : Try[OutputStream]
  def encodes: Set[String]
  def encodedMIME: String
}