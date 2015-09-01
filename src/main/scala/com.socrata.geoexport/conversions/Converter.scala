package com.socrata.geoexport.conversions

import java.nio.charset.StandardCharsets
import java.io._

import com.rojoma.json.v3.io.JsonReader
import com.socrata.geoexport.CJson
import com.socrata.geoexport.encoders.GeoEncoder
import com.socrata.soql.types.SoQLPoint
import com.socrata.soql.{SoQLPackIterator, SoQLPackDecoder}
import com.socrata.thirdparty.geojson.{FeatureCollectionJson, GeoJson}
import org.apache.commons.io.IOUtils
import scala.util.{Try, Success, Failure}

object Converter {

  def execute(layerStreams: Iterable[InputStream], tasks: List[GeoConversion], encoder: GeoEncoder, os: OutputStream) : Try[OutputStream] = {

    Try(layerStreams.map { instream =>
      new SoQLPackIterator(new DataInputStream(instream))
    }) match {
      case Success(featureStreams) => encoder.encode(featureStreams, os)
      case Failure(f) => Failure(f)
    }




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