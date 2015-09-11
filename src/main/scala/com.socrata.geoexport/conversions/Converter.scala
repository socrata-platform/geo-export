package com.socrata.geoexport.conversions

import java.io._

import com.socrata.geoexport.encoders.GeoEncoder
import com.socrata.soql.SoQLPackIterator

import scala.util.{Failure, Success, Try}

object Converter {

  /**
    Take some layers streams and merge them into the outputstream using the encoder
    This will merge the layers into whatever representation the encoder decides to represent
    layers as.
  */
  def execute(layerStreams: Iterable[InputStream], encoder: GeoEncoder, os: OutputStream) : Try[OutputStream] = {
    Try(layerStreams.map { instream =>
      new SoQLPackIterator(new DataInputStream(instream))
    }) match {
      case Success(featureStreams) => encoder.encode(featureStreams, os)
      case Failure(f) => Failure(f)
    }
  }
}
