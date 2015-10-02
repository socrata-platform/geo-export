package org.geotools.data.shapefile

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.TimeZone

import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object NastyHack {
  def write(featureType: SimpleFeatureType, file: File, it: Iterator[SimpleFeature]) = {
    val shpFiles = new ShpFiles(file)



    val shpReader = new ShapefileReader(shpFiles, false, false, null)
    val reader = new ShapefileFeatureReader(featureType, shpReader, null, null) {
      override def hasNext(): Boolean = {
        it.hasNext
      }

      override def next(): SimpleFeature = {
        it.next()
      }
    }
    val tz: TimeZone = TimeZone.getDefault
    val writer = new ShapefileFeatureWriter(shpFiles, reader, StandardCharsets.UTF_8, tz)

    while(writer.hasNext) {
      writer.next()
      writer.write()
    }
    writer.close()

  }
}