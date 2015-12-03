// scalastyle:off null
package com.socrata.geoexport.encoders

import java.io._
import java.nio.charset.StandardCharsets
import java.util.{TimeZone, UUID}
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.rojoma.simplearm.util._
import com.socrata.geoexport.config.GeoexportConfig
import com.socrata.geoexport.encoders.KMLMapper._
import com.socrata.geoexport.intermediates.shapefile._
import com.socrata.geoexport.intermediates._
import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.geotools.data.shapefile.shp.{ShapefileReader, ShapefileWriter}
import org.geotools.data.shapefile._
import org.geotools.data.{DataStore, Transaction}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature.`type`._
import org.geotools.feature.simple.{SimpleFeatureImpl, SimpleFeatureTypeBuilder}
import org.geotools.feature.{DefaultFeatureCollection, NameImpl}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.slf4j.LoggerFactory
import com.rojoma.simplearm.v2.ResourceScope
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import geotypes._

object ShapefileEncoder extends GeoEncoder {

  lazy val log = LoggerFactory.getLogger(getClass)

  case class FeatureCollectionException(val message: String) extends Exception

  type SoQLColumn = (String, SoQLType)
  type IntermediarySchema = Seq[ShapeRep[_ <: SoQLValue]]
  type IntermediaryValues = Seq[(_ <: SoQLValue, ShapeRep[_ <: SoQLValue])]

  val shapefileExt = "shp"
  val spatialIndexExt = "shx"
  val dbaseExt = "dbf"
  val projExt = "prj"

  val shapeArchiveExts = Seq(
    shapefileExt,
    spatialIndexExt,
    dbaseExt,
    projExt
  )
  private def getShapefileMinions(shpFile: File, rs: ResourceScope, onFile: (InputStream, String) => Unit): Unit = {
    shpFile.getName.split("\\.") match {
      case Array(name, ext) => shapeArchiveExts.map { other =>
        val file = new File(shpFile.getParent, s"${name}.${other}")
        val in = rs.open(new BufferedInputStream(rs.open(new FileInputStream(file))))
        try {
          onFile(in, file.getName)
        } finally {
          file.delete()
        }
      }
    }
  }

  private def toIntermediaryReps(schema: Seq[SoQLColumn]) = schema.map(ShapeRep.repFor(_, ShapefileRepMapper))

  private def buildFeatureType(schema: IntermediarySchema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()

    builder.setCRS(DefaultGeographicCRS.WGS84)
    builder.setName("FeatureType")

    val addedNames = schema
    .filter(!_.isInstanceOf[IDRep]) // find all the non-id fields
    .flatMap { rep =>
      val restrictions = seqAsJavaList(List[Filter]())

      val attrNames = rep.toAttrNames
      val bindings = rep.toAttrBindings

      if(attrNames.size != bindings.size) {
        throw new Exception("Attribute names:bindings mapping must be 1:1 in the intermediary representation")
      }

      attrNames.zip(bindings).foreach { case (attrName, binding) =>
        // geotools is dumb
        if (classOf[Geometry].isAssignableFrom(binding)) {
          val attrName = new NameImpl("the_geom")
          val geomType = new GeometryTypeImpl(
            attrName, binding, DefaultGeographicCRS.WGS84, true, false, seqAsJavaList(List()), null, null
          )
          val geomDescriptor = new GeometryDescriptorImpl(
            geomType, attrName, Int.MinValue, Int.MaxValue, true, null
          )
          builder.add(geomDescriptor)
        } else {
          val bindingName = new NameImpl(binding.toString())
          val attrType = new AttributeTypeImpl(bindingName, binding, true, false, restrictions, null, null)
          val descriptor = new AttributeDescriptorImpl(
            attrType, new NameImpl(attrName), Int.MinValue, Int.MaxValue, true, null
          )
          builder.add(descriptor)
        }
      }
      attrNames
    }
    log.debug(s"Added names ${addedNames.size}  ${addedNames}")
    builder.buildFeatureType()
  }

  private def buildFeature(
    featureId: Int,
    featureType: SimpleFeatureType,
    attributes: IntermediaryValues): SimpleFeature = {

    val id = new FeatureIdImpl(featureId.toString)

    val nonIds = attributes.filter(!_._1.isInstanceOf[SoQLID])
    // what in the actual .... there has to be a better way to do this...
    // except not because dependent types
    val values = seqAsJavaList(nonIds.flatMap {
      intermediate => ShapefileRepMapper.toAttr(intermediate)
    })
    new SimpleFeatureImpl(values, featureType, id)
  }

  private def addFeatures(layer: SoQLPackIterator, featureType: SimpleFeatureType, file: File,
                          dataStore: DataStore, reps:Seq[ShapeRep[_ <: SoQLValue]]) = {

    var fid = 0
    val it = new Iterator[SimpleFeature] {
      def next(): SimpleFeature = {
        val intermediary = layer.next.zip(reps)
        val feature = buildFeature(fid, featureType, intermediary)
        fid = fid + 1
        feature
      }
      def hasNext: Boolean = layer.hasNext
    }

    NastyHack.write(featureType, file, it)

    file
  }


  def encode(rs: ResourceScope, layers: Layers, outStream: OutputStream) : Try[OutputStream] = {

    try {
      layers.map { layer =>
        val file = new File(s"/tmp/geo_export_${UUID.randomUUID()}.shp")

        // factories, stores, and sources, oh my
        val shapefileFactory = new ShapefileDataStoreFactory()
        val meta = Map[String, AnyRef](
          "create spatial index" -> java.lang.Boolean.TRUE,
          "url" -> file.toURI.toURL
        )
        val dataStore = shapefileFactory.createNewDataStore(mapAsJavaMap(meta.asInstanceOf[Map[String, Serializable]]))

        // split the SoQLSchema into a potentially longer ShapeSchema
        val reps = toIntermediaryReps(layer.schema)
        // build the feature type out of a schema that is representable by a ShapeFile
        val featureType = buildFeatureType(reps)
        dataStore.createSchema(featureType)
        addFeatures(layer, featureType, file, dataStore, reps)

      }.foldLeft(rs.open(new ZipOutputStream(outStream))) { (zipStream, shpFile) =>


        getShapefileMinions(shpFile, rs, { (in, fileName) =>
          zipStream.putNextEntry(new ZipEntry(fileName))
          var b = in.read()
          while(b > -1) {
            zipStream.write(b)
            b = in.read()
          }
          zipStream.closeEntry()
        })

        zipStream
      }

      Success(outStream)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  def encodes: Set[String] = Set(shapefileExt) ++ Set("shapefile")
  def encodedMIME: String  = "application/zip"
}
// scalastyle:on null
