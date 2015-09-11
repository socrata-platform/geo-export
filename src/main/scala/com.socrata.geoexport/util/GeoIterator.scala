package com.socrata.geoexport.util

import org.geotools.feature.FeatureIterator
import org.opengis.feature.simple.SimpleFeature

class GeoIterator(sfi: FeatureIterator[SimpleFeature]) extends Iterator[SimpleFeature] with AutoCloseable {

  def hasNext: Boolean = sfi.hasNext()
  def next(): SimpleFeature = sfi.next()
  def remove(): Unit = ???
  def close(): Unit = ???
}
