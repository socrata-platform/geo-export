package com.socrata.geoexport.encoders

import java.io.OutputStream

import org.opengis.feature.simple.SimpleFeature


trait GeoEncoder {
  def encode(layers: Iterable[Iterator[SimpleFeature]], outStream: OutputStream) : Either[String, OutputStream]
}