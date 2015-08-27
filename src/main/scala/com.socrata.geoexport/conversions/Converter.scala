package com.socrata.geoexport.conversions

import java.io.{InputStream, OutputStream}
import com.socrata.geoexport.encoders.GeoEncoder
import com.socrata.geoexport.util.GeoIterator
import org.geotools.geojson.feature.FeatureJSON


object Converter {
  def execute(layerStreams: Iterable[InputStream], tasks: List[GeoConversion], encoder: GeoEncoder, os: OutputStream) : Either[String, OutputStream] = {

    val featureStreams = layerStreams.map { instream =>
      val deserializer = new FeatureJSON()
      new GeoIterator(deserializer.streamFeatureCollection(instream))
    }

    tasks.foldLeft(Right(featureStreams)) {
      (acc, task) =>
        acc match {
          case Right(collections) => Right(collections)
          case error => error
        }
    } match {
      case Right(newFeatures) => encoder.encode(newFeatures, os)
      case error => Left("FIXME")
    }
  }
}