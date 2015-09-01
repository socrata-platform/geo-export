package com.socrata.geoexport.encoders

import java.io._
import java.sql.{Timestamp, Time}
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.{lang, util}
import java.util.{Calendar, UUID, Date}

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.socrata.geoexport.encoders.KMLMapper._
import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types._
import com.vividsolutions.jts.geom._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{Transaction, DefaultTransaction}
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.feature.{DefaultFeatureCollection, GeometryAttributeImpl, NameImpl}
import org.geotools.feature.`type`._
import org.geotools.feature.simple.{SimpleFeatureImpl, SimpleFeatureTypeBuilder, SimpleFeatureBuilder}
import org.geotools.filter.expression.SimpleFeaturePropertyAccessorFactory
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.base.{BaseLocal, BaseDateTime}

import org.joda.time._
import org.joda.time.format.DateTimeFormat

import org.opengis.feature.`type`.{AttributeType, Name, FeatureType, AttributeDescriptor}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.identity.FeatureId
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.slf4j.LoggerFactory

import scala.language.implicitConversions
import scala.xml.{Node, XML}
import com.rojoma.simplearm.util._
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConversions._



class ShapefileEncoder extends GeoEncoder {
  private val TIME_FORMAT = "HH:mm:ss.SSS"

  type SoQLColumn = (String, SoQLType)
  type SoQLSchema = Seq[SoQLColumn]

  type ShapeColumn = (String, Class[_])
  type ShapeSchema = Seq[ShapeColumn]

  type ShapeAttr = (String, Object)
  type ShapeAttrs = Seq[ShapeAttr]


  val shapefileExts = Seq("shp", "shx", "prj", "fix", "dbf")
  private def getShapefileMinions(shpFile: File): Seq[File] = {
    shpFile.getName.split("\\.") match {
      case Array(name, ext) => shapefileExts.map { other =>
        new File(shpFile.getParent, s"${name}.${other}")
      }
    }
  }


  private def getBindings(soqlCol: SoQLColumn): ShapeSchema = {
    val (name, soqlType) = soqlCol
    soqlType match {
      case SoQLPoint => Seq((name, classOf[Point]))
      case SoQLMultiPoint => Seq((name, classOf[MultiPoint]))
      case SoQLLine =>
        Seq((name, classOf[LineString]))
      case SoQLMultiLine => Seq((name, classOf[MultiLineString]))
      case SoQLPolygon => Seq((name, classOf[Polygon]))
      case SoQLMultiPolygon => Seq((name, classOf[MultiPolygon]))
      case SoQLDate =>
        Seq((name, classOf[Date]))
      case SoQLTime =>
        Seq((name, classOf[String]))
      case SoQLFloatingTimestamp =>
        val dateName = s"${name}_date"
        val timeName = s"${name}_time"
        Seq((dateName, classOf[Date]), (timeName, classOf[String]))
      case SoQLFixedTimestamp =>
        val dateName = s"${name}_date"
        val timeName = s"${name}_time"
        Seq((dateName, classOf[Date]), (timeName, classOf[String]))
      case SoQLNumber => Seq((name, classOf[BigDecimal]))
      case SoQLText => Seq((name, classOf[String]))
      case SoQLMoney => Seq((name, classOf[BigDecimal]))
      case SoQLBoolean => Seq((name, classOf[java.lang.Boolean]))
      case v => throw new Exception(s"Unknown type ${v}")
    }
  }


  private def toShapeSchema(schema: SoQLSchema): ShapeSchema = {
    schema.flatMap(getBindings(_))
  }

  private def formatTime(dt: DateTime): String = {
    val fmt = DateTimeFormat.forPattern(TIME_FORMAT);
    fmt.print(dt)
  }

  private def splitTimish(name: String, dt: LocalDate): ShapeAttrs = {
    val date = dt.toDate()
    Seq((s"${name}_date", date))
  }

  private def splitTimish(name: String, dt: LocalDateTime): ShapeAttrs = {
    val date = dt.toDate()
    val fmt = DateTimeFormat.forPattern(TIME_FORMAT);
    val time = fmt.print(dt)
    Seq((s"${name}_date", date), (s"${name}_time", time))
  }

  private def splitTimish(name: String, dt: DateTime): ShapeAttrs = {
    val utc = new DateTime(dt, DateTimeZone.UTC)
    val date = utc.toDate()
    val time = formatTime(utc)
    Seq((s"${name}_date", date), (s"${name}_time", time))
  }

  private def splitTimish(name: String, dt: LocalTime): ShapeAttrs = {
    val utc = dt.toDateTimeToday(DateTimeZone.UTC)
    val time = formatTime(utc)
    Seq((s"${name}_time", time))
  }

  //despite making you jump through 37 layers of indirection, at the end of the day
  //you everything you set/get in geotools is no more specific than `Object`
  private def toGeotoolsTypes(name: String, soql: SoQLValue): ShapeAttrs = {

    soql match {
      case v: SoQLPoint => Seq((name, v.value))
      case v: SoQLMultiPoint => Seq((name, v.value))
      case v: SoQLLine => Seq((name, v.value))
      case v: SoQLMultiLine => Seq((name, v.value))
      case v: SoQLPolygon => Seq((name, v.value))
      case v: SoQLMultiPolygon => Seq((name, v.value))
      case v: SoQLDate => splitTimish(name, v.value)
      case v: SoQLFloatingTimestamp => splitTimish(name, v.value)
      case v: SoQLFixedTimestamp => splitTimish(name, v.value)
      case v: SoQLTime => splitTimish(name, v.value)
      case v: SoQLMoney => Seq((name, v.value))
      case v: SoQLBoolean => Seq((name, v.value: java.lang.Boolean))
      case v: SoQLNumber => Seq((name, v.value))
      case v: SoQLText => Seq((name, v.value))
      case v: SoQLValue => Seq((name, v.toString))
    }
  }

  private def buildFeatureType(schema: ShapeSchema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder();

    builder.setCRS(DefaultGeographicCRS.WGS84);
    builder.setName("FeatureType")

    schema.foreach { case (bindingName, binding) =>
      val restrictions = seqAsJavaList(List[Filter]())

      if (classOf[Geometry].isAssignableFrom(binding)) {
        val attrName = new NameImpl("the_geom")
        val geomType = new GeometryTypeImpl(attrName, binding, DefaultGeographicCRS.WGS84, true, false, seqAsJavaList(List()), null, null)
        val geomDescriptor = new GeometryDescriptorImpl(geomType, attrName, Int.MinValue, Int.MaxValue, true, null)
        builder.add(geomDescriptor)
      } else {
        val attrName = new NameImpl(binding.toString())
        val attrType = new AttributeTypeImpl(attrName, binding, true, false, restrictions, null, null)
        val descriptor = new AttributeDescriptorImpl(attrType, new NameImpl(bindingName), Int.MinValue, Int.MaxValue, true, null)
        builder.add(descriptor)
      }
    }
    builder.buildFeatureType()
  }


  private def buildFeature(featureType: SimpleFeatureType, soqlRep: SoQLSchema,
    shapeRep: ShapeSchema, attributes: Array[SoQLValue]): SimpleFeature = {

    val id = new FeatureIdImpl(":id")

    val values = seqAsJavaList(soqlRep.zip(attributes).flatMap { case ((soqlName, soqlType), soqlValue) =>
      toGeotoolsTypes(soqlName, soqlValue)
    }).map { case (_newName, geoValue) => geoValue}

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
        val shapeSchema = toShapeSchema(layer.schema)
        //build the feature type out of a schema that is representable by a ShapeFile
        val featureType = buildFeatureType(shapeSchema)
        dataStore.createSchema(featureType)

        dataStore.getFeatureSource((dataStore.getTypeNames.toList)(0)) match {
          case featureStore: SimpleFeatureStore =>

            for { trans <- managed(new DefaultTransaction(file.getName)) } {
              try {

                featureStore.setTransaction(trans)
                val collection = new DefaultFeatureCollection()
                layer.foreach { attrs =>
                  collection.add(buildFeature(featureType, layer.schema, shapeSchema, attrs))
                }
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


}


