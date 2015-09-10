package com.socrata.geoexport.encoders

import java.io._
import java.util.UUID
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.rojoma.simplearm.util._
import com.socrata.geoexport.encoders.KMLMapper._
import com.socrata.geoexport.shapefile.intermediates._
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.geotools.data.DefaultTransaction
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature.`type`._
import org.geotools.feature.simple.{SimpleFeatureImpl, SimpleFeatureTypeBuilder}
import org.geotools.feature.{DefaultFeatureCollection, NameImpl}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}



object ShapefileEncoder extends GeoEncoder {

  case class FeatureCollectionException(val message: String) extends Exception

  type SoQLColumn = (String, SoQLType)
  type IntermediarySchema = Seq[ShapeRep[_ <: SoQLValue]]
  type IntermediaryValues = Seq[(_ <: SoQLValue, ShapeRep[_ <: SoQLValue])]


  val shapefileExts = Seq("shp", "shx", "prj", "fix", "dbf")
  private def getShapefileMinions(shpFile: File): Seq[File] = {
    shpFile.getName.split("\\.") match {
      case Array(name, ext) => shapefileExts.map { other =>
        new File(shpFile.getParent, s"${name}.${other}")
      }
    }
  }

  private def repFor(col: SoQLColumn): ShapeRep[_ <: SoQLValue] = {
    col match {
      case (name, SoQLPoint) => new PointRep(name)
      case (name, SoQLMultiPoint) => new MultiPointRep(name)
      case (name, SoQLLine) => new LineRep(name)
      case (name, SoQLMultiLine) => new MultiLineRep(name)
      case (name, SoQLPolygon) => new PolygonRep(name)
      case (name, SoQLMultiPolygon) => new MultiPolygonRep(name)
      case (name, SoQLDate) => new DateRep(name)
      case (name, SoQLTime) => new TimeRep(name)
      case (name, SoQLFloatingTimestamp) => new FloatingTimestampRep(name)
      case (name, SoQLFixedTimestamp) => new FixedTimestampRep(name)
      case (name, SoQLNumber) => new NumberRep(name)
      case (name, SoQLText) => new TextRep(name)
      case (name, SoQLMoney) => new MoneyRep(name)
      case (name, SoQLBoolean) => new BooleanRep(name)
      case (name, SoQLVersion) => new VersionRep(name)
      case (name, SoQLID) => new IDRep(name)

      case (name, SoQLArray) => ???
      case (name, SoQLDouble) => ???
      case (name, SoQLJson) => ???
      case (name, SoQLNull) => ???
      case (name, SoQLObject) => ???
    }
  }

  private def toIntermediaryReps(schema: Seq[SoQLColumn]) = schema.map(repFor(_))

  private def buildFeatureType(schema: IntermediarySchema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder();

    builder.setCRS(DefaultGeographicCRS.WGS84);
    builder.setName("FeatureType")

    val addedNames = schema
    .filter {
      case r: IDRep => false
      case _ => true
    }.flatMap { rep =>
      val restrictions = seqAsJavaList(List[Filter]())

      val attrNames = rep.toAttrNames
      val bindings = rep.toAttrBindings

      if(attrNames.size != bindings.size) {
        throw new Exception("Attribute names:bindings mapping must be 1:1 in the intermediary representation")
      }

      attrNames.zip(bindings).foreach { case (attrName, binding) =>
        if (classOf[Geometry].isAssignableFrom(binding)) {
          val attrName = new NameImpl("the_geom")
          val geomType = new GeometryTypeImpl(attrName, binding, DefaultGeographicCRS.WGS84, true, false, seqAsJavaList(List()), null, null)
          val geomDescriptor = new GeometryDescriptorImpl(geomType, attrName, Int.MinValue, Int.MaxValue, true, null)
          builder.add(geomDescriptor)
        } else {
          val bindingName = new NameImpl(binding.toString())
          val attrType = new AttributeTypeImpl(bindingName, binding, true, false, restrictions, null, null)
          val descriptor = new AttributeDescriptorImpl(attrType, new NameImpl(attrName), Int.MinValue, Int.MaxValue, true, null)
          builder.add(descriptor)
        }
      }
      attrNames
    }
    log.debug(s"Added names ${addedNames.size}  ${addedNames}")
    builder.buildFeatureType()
  }

  private def buildFeature(featureId: Int, featureType: SimpleFeatureType, attributes: IntermediaryValues): SimpleFeature = {

    val id = new FeatureIdImpl(featureId.toString)

    val nonIds = attributes.filter { v =>
      v match {
        case (_: SoQLID, _) => false
        case _ => true
      }
    }
    //what in the fuck there has to be a better way to do this...
    //except not because dependent types
    val values = seqAsJavaList(nonIds.flatMap {
        case (value: SoQLPoint, intermediary: PointRep) => intermediary.toAttrValues(value)
        case (value: SoQLMultiPoint, intermediary: MultiPointRep) => intermediary.toAttrValues(value)
        case (value: SoQLLine, intermediary: LineRep) => intermediary.toAttrValues(value)
        case (value: SoQLMultiLine, intermediary: MultiLineRep) => intermediary.toAttrValues(value)
        case (value: SoQLPolygon, intermediary: PolygonRep) => intermediary.toAttrValues(value)
        case (value: SoQLMultiPolygon, intermediary: MultiPolygonRep) => intermediary.toAttrValues(value)
        case (value: SoQLDate, intermediary: DateRep) => intermediary.toAttrValues(value)
        case (value: SoQLTime, intermediary: TimeRep) => intermediary.toAttrValues(value)
        case (value: SoQLFloatingTimestamp, intermediary: FloatingTimestampRep) => intermediary.toAttrValues(value)
        case (value: SoQLFixedTimestamp, intermediary: FixedTimestampRep) => intermediary.toAttrValues(value)
        case (value: SoQLNumber, intermediary: NumberRep) => intermediary.toAttrValues(value)
        case (value: SoQLText, intermediary: TextRep) => intermediary.toAttrValues(value)
        case (value: SoQLMoney, intermediary: MoneyRep) => intermediary.toAttrValues(value)
        case (value: SoQLBoolean, intermediary: BooleanRep) => intermediary.toAttrValues(value)
        case (value: SoQLVersion, intermediary: VersionRep) => intermediary.toAttrValues(value)
        case (value: SoQLID, intermediary: IDRep) => intermediary.toAttrValues(value)
        case _ => ???
    })
    new SimpleFeatureImpl(values, featureType, id)
  }

  def encode(layers: Layers, outStream: OutputStream) : Try[OutputStream] = {

    try {
      layers.map { layer =>
        val file = new File(s"/tmp/geo_export_${UUID.randomUUID()}.shp")

        //factories, stores, and sources, oh my
        val shapefileFactory = new ShapefileDataStoreFactory()
        val meta = Map[String, AnyRef](
          "create spatial index" -> java.lang.Boolean.TRUE,
          "url" -> file.toURI.toURL
        )
        val dataStore = shapefileFactory.createNewDataStore(mapAsJavaMap(meta.asInstanceOf[Map[String, Serializable]]))

        //split the SoQLSchema into a potentially longer ShapeSchema
        val reps = toIntermediaryReps(layer.schema)
        //build the feature type out of a schema that is representable by a ShapeFile
        val featureType = buildFeatureType(reps)
        dataStore.createSchema(featureType)

        dataStore.getFeatureSource((dataStore.getTypeNames.toList)(0)) match {
          case featureStore: SimpleFeatureStore =>

            for { trans <- managed(new DefaultTransaction(file.getName)) } {
              try {

                featureStore.setTransaction(trans)
                val collection = new DefaultFeatureCollection()
                layer.foldLeft(0) { (id, attrs) =>

                  val intermediary = attrs.zip(reps)
                  if(!collection.add(buildFeature(id, featureType, intermediary))) {
                    throw new FeatureCollectionException(s"Failed to add feature ${attrs} to Geotools DefaultFeatureCollection")
                  }
                  id + 1
                }
                log.debug(s"Added ${collection.size()} features to feature collection")
                featureStore.addFeatures(collection)
              } finally {
                trans.commit()
                dataStore.dispose()
              }
            }
        }
        file
      }.foldLeft(new ZipOutputStream(outStream)) { (zipStream, shpFile) =>
        getShapefileMinions(shpFile).foreach { file =>
          zipStream.putNextEntry(new ZipEntry(file.getName))
          val in = new BufferedInputStream(new FileInputStream(file))
          var b = in.read()
          while(b > -1) {
            zipStream.write(b)
            b = in.read()
          }
          in.close()
          zipStream.closeEntry()
        }
        zipStream
      }.close()

      Success(outStream)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  def encodes = Set("shp")
  def encodedMIME = "application/zip"
}


