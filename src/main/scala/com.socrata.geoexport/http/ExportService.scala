package com.socrata.geoexport.http
import com.socrata.soql.SoQLPackIterator

import java.io.{OutputStream, _}
import java.nio.charset.StandardCharsets.UTF_8
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.util.{AutomaticJsonCodec, AutomaticJsonCodecBuilder, JsonUtil}
import com.socrata.geoexport.UnmanagedCuratedServiceClient
import com.socrata.geoexport.conversions.Converter
import com.socrata.geoexport.encoders.{GeoJSONEncoder, KMLEncoder, KMZEncoder, ShapefileEncoder}
import com.socrata.geoexport.encoders.geotypes._
import com.socrata.geoexport.http.ExportService._
import com.socrata.http.client.{RequestBuilder, Response}
import com.socrata.http.server.responses.{Json, _}
import com.socrata.http.server.routing.{SimpleResource, TypedPathComponent}
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import com.socrata.http.server.implicits._
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.geoexport.encoders.GeoEncoder

import javax.servlet.http.HttpServletResponse.{SC_NOT_FOUND => ScNotFound, SC_OK => ScOk}
import scala.util.{Failure, Success, Try}

object ExportService {
  // to appease the scala linter.. ಠ_ಠ
  val errorKey = "reason"
  val FourbyFour = "[\\w0-9]{4}-[\\w0-9]{4}"
  val NbeResourceName = "_[\\w0-9]{4}-[\\w0-9]{4}"
  val encoders = List(KMLEncoder, ShapefileEncoder, KMZEncoder, GeoJSONEncoder)
  val formats: Set[String] = encoders.flatMap(_.encodes).toSet
  type LayerFailure = (Int, JValue)

  private def isValidFourByFour(s: String) = s.matches(FourbyFour) || s.matches(NbeResourceName)

  def validateFourByFours(layers: Seq[String]): Either[HttpResponse, Seq[String]] = {
    layers.partition(isValidFourByFour(_)) match {
      case (_valid, invalid) if invalid.size > 0 =>
        Left(BadRequest ~> Json(JsonUtil.renderJson(Map(
          errorKey -> s"""${invalid.mkString(", ")} is not a valid dataset identifier"""
        ))))
      case (valid, _) =>
        Right(valid)
    }
  }

  // temporary measure while we deploy the update to core that passes nbe resource names directly
  def resourceNameify(fbf: String): String = {
    if (!fbf.matches(NbeResourceName)) {
      s"_${fbf}"
    } else {
      fbf
    }
  }

  object LayerWithQuery {
    implicit val codec = AutomaticJsonCodecBuilder[LayerWithQuery]
  }

  case class LayerWithQuery(uid: String, query: Option[String], context: Option[String], copy: Option[String])
}

class ExportService(sodaClient: UnmanagedCuratedServiceClient) extends SimpleResource {
  lazy val log = LoggerFactory.getLogger(getClass)

  private def mergeUpstreamErrors(errors: Seq[LayerFailure]): HttpResponse = {
    val statusCode = errors.map{ case (status, _) => status }.max
    val reasons = JsonEncode.toJValue[Seq[JValue]](errors.map { case (status, reason) =>
      log.warn(s"SodaFountain returned an error ${status} ${reason}")
      JsonEncode.toJValue[Map[String, JValue]](Map(
        "status" -> JNumber(status),
        errorKey -> reason
      ))
    })

    log.warn(s"SodaFountain failures, returning a ${statusCode}")
    Status(statusCode) ~> Json(JsonEncode.toJValue(Map(errorKey -> reasons)))
  }

  private def getUpstreamLayers(req: HttpRequest,
                                layers: Seq[LayerWithQuery]): Either[HttpResponse, Seq[Response with Closeable]] = {
    val defaultSoql = s"select * limit ${Int.MaxValue}"
    val defaultCopy = "published"
    val defaultContext = "{}"

    layers.map { layer =>
      val reqBuilder = { base: RequestBuilder =>
        base
          .path(Seq("resource", s"${resourceNameify(layer.uid)}.soqlpack"))
          .addParameter(("$query", layer.query.getOrElse(defaultSoql)))
          .addParameter(("$$copy", layer.copy.getOrElse(defaultCopy)))
          .addParameter(("$$context", layer.context.getOrElse(defaultContext)))
          .addHeader(ReqIdHeader -> req.requestId)
          .addHeader("X-Socrata-Lens-Uid" -> layer.uid)
          .get
      }
      Try(sodaClient.execute(reqBuilder)) match {
        case Success(response) =>
          response.resultCode match {
            case ScOk => Right(response)
            case status: Int =>
              val body = IOUtils.toString(response.inputStream(), UTF_8)
              val err = Try(JsonReader.fromString(body)) match {
                case Success(js) => js
                case Failure(_) =>
                  log.warn(s"Unable to parse SodaFountain error as json")
                  JsonEncode.toJValue[String]("Unknown upstream error")
              }
              Left((status, err))
          }
        case Failure(exc) =>
          log.error(s"Failed to contact SodaFountain", exc)
          Left((ScNotFound, JsonEncode.toJValue[String](s"${exc.getMessage}")))
      }
    }.partition(_.isLeft) match {
      case (Seq(), successes)  => Right(successes.map(_.right.get))
      case (errors, successes) => Left(mergeUpstreamErrors(errors.map(_.left.get)))
    }
  }

  private def parseLayersWithQueries(req: HttpRequest): Either[HttpResponse, Seq[LayerWithQuery]] = {
    val thing = req.queryParameter("layersWithQueries").getOrElse("[]")
    JsonUtil.parseJson[Seq[LayerWithQuery]](thing) match {
      case Right(parsed: Seq[LayerWithQuery]) => Right(parsed)
      case Left(_) => Left(BadRequest ~> Json(JsonUtil.renderJson(Map(
        errorKey -> s"""Could not parse the 'layersWithQueries' parameter"""
      ))))
    }
  }

  private def createLayersWithQuerys(
    req: HttpRequest,
    baseFxfs: Seq[String],
    parsedLayersWithQueries: Seq[LayerWithQuery]
  ): Seq[LayerWithQuery] = {

    baseFxfs.map(fxf => {
      LayerWithQuery(
        fxf,
        req.queryParameter("query"),
        req.queryParameter("context"),
        req.queryParameter("copy")
      )
    }) ++ parsedLayersWithQueries
  }

  private def proxy(req: HttpRequest,
                    fourByFours: String): Either[HttpResponse, Seq[Response with Closeable]] = {
    parseLayersWithQueries(req) match {
      case Right(layersWithQueries) =>
        val baseUids = fourByFours.split(",")
        val allUids = baseUids ++ layersWithQueries.map(_.uid)
        validateFourByFours(allUids) match {
          case Right(_) =>
            val layers = createLayersWithQuerys(req, baseUids, layersWithQueries)
            getUpstreamLayers(req, layers)
          case Left(reason) =>
            log.warn(s"Received bad 4x4s: ${allUids}")
            Left(reason)
        }
      case Left(err) =>
        log.warn(s"Unable to parse layersWithQueries parameter: ${req.queryParameter("layersWithQueries")}")
        Left(err)
    }
  }

  private def getEncoder(format: String): Option[GeoEncoder] = {
    encoders.find(_.encodes.contains(format))
  }

  def handleRequest(req: HttpRequest, fxfs: TypedPathComponent[String]): HttpResponse = {
    val TypedPathComponent(fourByFours, format) = fxfs
    getEncoder(format) match {
      case Some(encoder) =>
        proxy(req, fourByFours) match {
          case Right(responses) =>
            encoder match {
              case ShapefileEncoder =>
                try {
                  val layerStream: Layers = responses
                    .map(r => r.inputStream())
                    .map(is => new SoQLPackIterator(new DataInputStream(is)))

                  val files = ShapefileEncoder.buildFiles(req.resourceScope, layerStream)
                  OK ~> ContentType(encoder.encodedMIME) ~> Stream({out: OutputStream =>
                    ShapefileEncoder.streamZip(files, out)
                    out.flush()
                    log.info(s"Finished writing export for ${fxfs}")
                  })
                } catch {
                  case e: IOException => {
                    log.error("Error exporting shapefile", e)
                    NotAcceptable
                  }
                } finally {
                  responses.foreach(_.close())
                }
              case _ =>
                OK ~> ContentType(encoder.encodedMIME) ~> Stream({out: OutputStream =>
                  val streams = responses.map { r => r.inputStream() }
                  Converter.execute(req.resourceScope, streams, encoder, out) match {
                    case Success(s) =>
                      out.flush()
                      log.info(s"Finished writing export for ${fxfs}")
                    case Failure(failure) =>
                      log.error(
                        "Encountered a fatal error; 200 status was already committed.",
                        failure
                      )
                  }
                  responses.foreach(_.close())
              })
            }
          case Left(error) => error
        }
      case None =>
        BadRequest ~> Json(JsonUtil.renderJson(Map(
          errorKey -> s"""${format} is not a valid dataset encoding"""
        )))

    }
  }

  def service(fxfs: TypedPathComponent[String]): SimpleResource = {
    new SimpleResource {
      override def get: HttpService = {
        req => handleRequest(req, fxfs)
      }

      override def query: HttpService = {
        req => handleRequest(req, fxfs)
      }
    }
  }
}
