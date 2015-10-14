package com.socrata.geoexport.encoders

import java.io.OutputStream
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import geotypes._


object KMZEncoder extends GeoEncoder {

  def encode(layers: Layers, outStream: OutputStream) : Try[OutputStream] = {
    val zipStream = new ZipOutputStream(outStream)
    val entry = new ZipEntry("export.kml")
    zipStream.putNextEntry(entry)
    KMLEncoder.encode(layers, zipStream) match {
      case Success(_) => Success(zipStream)
      case Failure(reason) => Failure(reason)
    }
  }

  def encodes: Set[String] = Set("kmz")
  def encodedMIME: String = "application/vnd.google-earth.kmz"
}


