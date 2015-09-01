package com.socrata.geoexport.conversions

import java.io._
import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.io.JsonReader
import com.socrata.geoexport.CJson
import com.socrata.geoexport.encoders.GeoEncoder
import com.socrata.soql.types.SoQLPoint
import com.socrata.soql.{SoQLPackIterator, SoQLPackDecoder}
import com.socrata.thirdparty.geojson.{FeatureCollectionJson, GeoJson}
import scala.util.{Try, Success, Failure}

object Converter {

  def execute(layerStreams: Iterable[InputStream], tasks: List[GeoConversion], encoder: GeoEncoder, os: OutputStream) : Try[OutputStream] = {

    val featureStreams = layerStreams.map { instream =>
      new SoQLPackIterator(new DataInputStream(instream))
    }

    encoder.encode(featureStreams, os)
//    tasks.foldLeft(Right(featureStreams)) {
//      (acc, task) =>
//        acc match {
//          case Right(collections) => Right(collections)
//          case error => error
//        }
//    } match {
//      case Right(transformedFeatures: Iterable[FeatureCollectionJson]) =>
//      case error => Left("FIXME")
//    }
  }
}