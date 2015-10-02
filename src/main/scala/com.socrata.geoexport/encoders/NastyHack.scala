package org.geotools.data.shapefile

import com.rojoma.simplearm.util._
import java.io.File
import java.nio.charset.StandardCharsets
import java.util.TimeZone

import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object NastyHack {

  private def getReader(featureType: SimpleFeatureType, shpFiles: ShpFiles, it: Iterator[SimpleFeature]) = {
    // scalastyle:off null
    val shpReader = new ShapefileReader(shpFiles, false, false, null)
    new ShapefileFeatureReader(featureType, shpReader, null, null) {
      override def hasNext(): Boolean = {
        it.hasNext
      }

      override def next(): SimpleFeature = {
        it.next()
      }
    }
    // scalastyle:on null
  }

  private def getWriter(reader: ShapefileFeatureReader, shpFiles: ShpFiles) = {
    val tz: TimeZone = TimeZone.getTimeZone("UTC")
    new ShapefileFeatureWriter(shpFiles, reader, StandardCharsets.UTF_8, tz)
  }

  def write(featureType: SimpleFeatureType, file: File, it: Iterator[SimpleFeature]): Unit = {
    val shpFiles = new ShpFiles(file)

    for {
      reader <- managed(getReader(featureType, shpFiles, it))
      writer <- managed(getWriter(reader, shpFiles))
    } {
      while(writer.hasNext) {
        writer.next()
        writer.write()
      }
    }




  }
}
