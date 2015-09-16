package com.socrata.geoexport.conversions

import org.opengis.feature.simple.SimpleFeature

trait GeoConversion {
  def run(geo: Iterator[SimpleFeature]) : Either[String, Iterator[SimpleFeature]]
}
