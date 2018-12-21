package org.geotools.data.shapefile

import com.rojoma.simplearm.v2._
import java.io.File
import java.nio.charset.StandardCharsets
import java.util.TimeZone

import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object NastyHack {
  implicit object ShpFilesResource extends Resource[ShpFiles] {
    def close(shpFiles: ShpFiles) {
      shpFiles.dispose()
    }
  }

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
    for {
      shpFiles <- managed(new ShpFiles(file))
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
