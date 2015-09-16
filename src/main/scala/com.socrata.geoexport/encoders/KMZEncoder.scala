package com.socrata.geoexport.encoders

import java.io.{OutputStream, OutputStreamWriter, Writer}

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.socrata.geoexport.encoders.KMLMapper._
import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.xml.{Node, XML}
import com.rojoma.simplearm.util._
import scala.util.{Try, Success, Failure}
import java.util.zip.{ZipEntry, ZipOutputStream}


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


