// scalastyle:off null
package com.socrata.geoexport.encoders

import java.io._
import java.nio.charset.StandardCharsets
import java.util.{TimeZone, UUID}
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.commons.io.{IOUtils, FileUtils}
import com.rojoma.simplearm.v2._
import com.socrata.geoexport.config.GeoexportConfig
import com.socrata.geoexport.intermediates.shapefile._
import com.socrata.geoexport.intermediates._
import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types._
import org.locationtech.jts.geom._
import org.geotools.data.shapefile.shp.{ShapefileReader, ShapefileWriter}
import org.geotools.data.shapefile._
import org.geotools.data.{DataStore, Transaction}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature.`type`._
import org.geotools.feature.simple.{SimpleFeatureImpl, SimpleFeatureTypeBuilder}
import org.geotools.feature.NameImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.slf4j.LoggerFactory
import com.rojoma.simplearm.v2.ResourceScope
// scalastyle:off
import scala.collection.JavaConverters._
// scalastyle:on
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}
import java.io.Closeable
import geotypes._

class ShapefileDirManager(shpDir: File) extends Closeable {
  shpDir.mkdir()
  override def close(): Unit = FileUtils.deleteDirectory(shpDir)
}

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
    dbaseExt,
    shapefileExt,
    spatialIndexExt,
    projExt
  )

  /**
   * For any poor souls reading this code:
   * Note that this does not create any files, it simply returns a
   * list of file handles that GeoTools *will* make, when features
   * are written to it.
   */
  private def getShapefileMinions(shpFile: File): Seq[File] = {
    shpFile.getName.split("\\.") match {
      case Array(name, _) => shapeArchiveExts.map {
        ext => new File(shpFile.getParent, s"${name}.${ext}")
      }
    }
  }

  private def toIntermediaryReps(schema: Seq[SoQLColumn]) = schema.map(ShapeRep.repFor(_, ShapefileRepMapper))

  def truncateAndDedup(initialAttrNames: Seq[String]): Seq[String] = {
    val MaxShapefileName = 10
    val shortNamesSet = initialAttrNames.filter(_.length <= MaxShapefileName).toSet
    val (attrNamesReversed, _, _) = initialAttrNames.foldLeft((List.empty[String], Set.empty[String], shortNamesSet)) {
      (accUsedNamesReservedNames, name) =>
        // "acc" is our results
        // "usedNames" is the set of names we've generated
        // "reservedNames" is the set of names we've not yet generated but are unwilling to generate
        // because a future column will have that name without needing to be truncated
        val (acc, usedNames, reservedNames) = accUsedNamesReservedNames
        if(reservedNames.contains(name)) {
          (name :: acc, usedNames + name, reservedNames - name)
        } else {
          val shortName = name.take(MaxShapefileName)
          val freshName =
            (Iterator.single(shortName) ++ Iterator.from(2).map { i =>
               val iStr = i.toString
               shortName.take(MaxShapefileName - iStr.length - 1) + "_" + iStr
             }).dropWhile { n => usedNames.contains(n) || reservedNames.contains(n) }.next()
          (freshName :: acc, usedNames + freshName, reservedNames)
        }
    }
    attrNamesReversed.reverse
  }

  private def buildFeatureType(schema: IntermediarySchema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()

    builder.setCRS(DefaultGeographicCRS.WGS84)
    builder.setName("FeatureType")

    val (rawAttrNames, bindings, restrictions) = schema.
      filterNot(_.isInstanceOf[IDRep]). // find all the non-id fields
      flatMap { rep =>
        val restrictions = seqAsJavaList(List[Filter]())

        val repAttrNames = rep.toAttrNames
        val repBindings = rep.toAttrBindings
        if(repAttrNames.size != repBindings.size) {
          throw new Exception("Attribute names:bindings mapping must be 1:1 in the intermediary representation")
        }

        (repAttrNames, repBindings, Stream.continually(restrictions)).zipped.toSeq
      }.
      unzip3

    val attrNames = truncateAndDedup(rawAttrNames)

    var usedAttrNames = attrNames.toSet

    (attrNames, bindings, restrictions).zipped.foreach { (attrName, binding, restrictions) =>
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
        val realAttrName =
          if(attrName == "the_geom") { // this name is special, so since it's _not_ a geometry, we need to rename it
            Iterator.from(2).map(attrName + "_" + _).find(!usedAttrNames.contains(_)).get
          } else {
            attrName
          }
        usedAttrNames += realAttrName
        val bindingName = new NameImpl(binding.toString())
        val attrType = new AttributeTypeImpl(bindingName, binding, true, false, restrictions, null, null)
        val descriptor = new AttributeDescriptorImpl(
          attrType, new NameImpl(realAttrName), Int.MinValue, Int.MaxValue, true, null
        )
        builder.add(descriptor)
      }
    }
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

  private def addFeatures(
    layer: SoQLPackIterator,
    featureType: SimpleFeatureType,
    file: File,
    dataStore: DataStore,
    reps:Seq[ShapeRep[_ <: SoQLValue]]) = {

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

  def buildFiles(rs: ResourceScope, layers: Layers): Iterable[File] = {
    val tmpDir = new File(s"/tmp/${UUID.randomUUID()}")
    rs.open(new ShapefileDirManager(tmpDir))

    layers.map { layer =>
      val file = new File(tmpDir, s"geo_export_${UUID.randomUUID()}.shp")
      // factories, stores, and sources, oh my
      val shapefileFactory = new ShapefileDataStoreFactory()
      val meta = Map[String, Any](
        "create spatial index" -> true,
        "url" -> file.toURI.toURL,
        "charset" -> StandardCharsets.UTF_8
      )
      val dataStore = shapefileFactory.createNewDataStore(meta.asJava)
      // split the SoQLSchema into a potentially longer ShapeSchema
      val reps = toIntermediaryReps(layer.schema)
      // build the feature type out of a schema that is representable by a ShapeFile
      val featureType = buildFeatureType(reps)
      dataStore.createSchema(featureType)
      addFeatures(layer, featureType, file, dataStore, reps)
    }
  }

  def streamZip(files: Iterable[File], outStream: OutputStream): Unit = {
    using(new ZipOutputStream(outStream)) { zipStream =>
      files.foreach { shpFile =>
        getShapefileMinions(shpFile).foreach { file =>
          using(new FileInputStream(file)) { fis =>
            zipStream.putNextEntry(new ZipEntry(file.getName))
            IOUtils.copy(fis, zipStream)
            zipStream.closeEntry()
            // I cannot for the life of me figure out if it's possible
            // to get geotools to emit a shapefile.  No combination of
            // setting an encoding and/or setting the "try cpg file"
            // setting seems to do it.  So, hackhackhack.
            if(file.getName.endsWith(".dbf")) {
              // The CPG file describes the encoding of text values in
              // the DBF file, which is why we're hooking this in at
              // "just wrote out the dbf".
              zipStream.putNextEntry(new ZipEntry(file.getName.dropRight(3) + "cpg"))
              zipStream.write(StandardCharsets.UTF_8.name.getBytes(StandardCharsets.ISO_8859_1))
              zipStream.closeEntry()
            }
          }
        }
      }
    }
  }

  def encode(rs: ResourceScope, layers: Layers, outStream: OutputStream) : Try[OutputStream] =
    Success(outStream)
  def encodes: Set[String] = Set(shapefileExt) ++ Set("shapefile")
  def encodedMIME: String  = "application/zip"
}
// scalastyle:on null
