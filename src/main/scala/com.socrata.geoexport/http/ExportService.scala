package com.socrata.geoexport.http

import java.io.{OutputStream, _}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.geoexport.UnmanagedCuratedServiceClient
import com.socrata.geoexport.conversions.Converter
import com.socrata.geoexport.encoders.{KMLEncoder, ShapefileEncoder, KMZEncoder, GeoJSONEncoder}
import com.socrata.geoexport.http.ExportService._
import com.socrata.http.client.{RequestBuilder, Response}
import com.socrata.http.server.responses.{Json, _}
import com.socrata.http.server.routing.{SimpleResource, TypedPathComponent}
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.geoexport.encoders.GeoEncoder
import javax.servlet.http.HttpServletResponse.{
  SC_OK => ScOk,
  SC_NOT_FOUND => ScNotFound,
  SC_BAD_GATEWAY => ScBadGateway
}

import scala.util.{Failure, Success, Try}

object ExportService {
  // to appease the scala linter.. ಠ_ಠ
  val errorKey = "reason"
  val FourbyFour = "[\\w0-9]{4}-[\\w0-9]{4}"
  val encoders = List(KMLEncoder, ShapefileEncoder, KMZEncoder, GeoJSONEncoder)
  val formats: Set[String] = encoders.flatMap(_.encodes).toSet
  type LayerFailure = (Int, JValue)

  private def isValidFourByFour(s: String) = s.matches(FourbyFour)

  def toLayerNames(fourByFours: String): Either[HttpResponse, Seq[String]] = {
    val layers = fourByFours.split(",")
    layers.partition(isValidFourByFour(_)) match {
      case (_valid, invalid) if invalid.size > 0 =>
        Left(BadRequest ~> Json(JsonUtil.renderJson(Map(
          errorKey -> s"""${invalid.mkString(", ")} is not a valid dataset identifier"""
        ))))
      case (valid, _) =>
        Right(valid)
    }
  }
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
                                fxfs: Seq[String]): Either[HttpResponse, Seq[Response with Closeable]] = {
    fxfs.map { fbf =>
      val reqBuilder = {
        base: RequestBuilder =>
          val sfReq = base
            .path(Seq("resource", s"_${fbf}.soqlpack"))
            .addParameter(("$query", s"select * limit ${Int.MaxValue}"))
            .addHeader(ReqIdHeader -> req.requestId)
            .get
          log.info(s"""SodaFountain <<< ${URLDecoder.decode(req.toString, "UTF-8")}""")
          sfReq
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

  private def proxy(req: HttpRequest,
                    fourByFours: String): Either[HttpResponse, Seq[Response with Closeable]] = {
    toLayerNames(fourByFours) match {
      case Right(layers) => getUpstreamLayers(req, layers)
      case Left(reason) =>
        log.warn(s"Received bad 4x4s ${fourByFours}")
        Left(reason)
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
    }
  }
}
