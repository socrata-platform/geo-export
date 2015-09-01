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
import com.socrata.geoexport.encoders.KMLEncoder
import com.socrata.geoexport.http.ExportService._
import com.socrata.http.client.{RequestBuilder, Response}
import com.socrata.http.server.responses.{Json, _}
import com.socrata.http.server.routing.{SimpleResource, TypedPathComponent}
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._

import scala.util.{Failure, Success, Try}

object ExportService {
  val formats: Set[String] = Set("kml")
  val FourbyFour = "[\\w0-9]{4}-[\\w0-9]{4}"

  type LayerFailure = (Int, String)

  private def isValidFourByFour(s: String) = s.matches(FourbyFour)

  def toLayerNames(fxfs: String): Either[HttpResponse, Seq[String]] = {
    val layers = fxfs.split(",")
    layers.partition(isValidFourByFour(_)) match {
      case (_valid, invalid) if invalid.size > 0 =>
        Left(BadRequest ~> Json(JsonUtil.renderJson(Map(
          "reason" -> s"""${invalid.mkString(", ")} is not a valid dataset identifier"""
        ))))
      case (valid, _) =>
        Right(valid)
    }
  }
}

class ExportService(sodaClient: UnmanagedCuratedServiceClient) extends SimpleResource {
  lazy val log = LoggerFactory.getLogger(getClass)


  private def mergeUpstreamErrors(errors: Seq[LayerFailure]): HttpResponse = {
    val reasons = JsonEncode.toJValue[Seq[JValue]](errors.map { case (status, reason) =>
      Try(JsonReader.fromString(reason)) match {
        case Success(js) =>
          log.warn(s"SodaFountain returned an error ${status} ${reason}")
          JsonEncode.toJValue[Map[String, JValue]](Map(
            "status" -> JNumber(status),
            "reason" -> js
          ))
        case Failure(exc) =>
          log.warn(s"Unable to parse SodaFountain error as json ${exc.toString}")
          JsonEncode.toJValue[String](reason)
      }
    })

    log.warn("SodaFountain failures, returning a 502")
    BadGateway ~> Json(JsonUtil.renderJson(JsonEncode.toJValue(Map("reason" -> reasons))))
  }

  private def getUpstreamLayers(fxfs: Seq[String]): Either[HttpResponse, Seq[Response with Closeable]] = {
    fxfs.map { fbf =>
      val reqBuilder = {
        base: RequestBuilder =>
          val req = base
            .path(Seq("export", s"_${fbf}.soqlpack"))
            .get
          log.info(s"""SodaFountain <<< ${URLDecoder.decode(req.toString, "UTF-8")}""")
          req
      }
      Try(sodaClient.execute(reqBuilder)) match {
        case Success(response) =>
          response.resultCode match {
            case 200 => Right(response)
            case err => Left((err, IOUtils.toString(response.inputStream(), UTF_8)))
          }
        case Failure(exc) => Left((404, s""""reason": ${exc.getMessage}"""))
      }
    }.partition(_.isLeft) match {
      case (Seq(), successes)  => Right(successes.map(_.right.get))
      case (errors, successes) => Left(mergeUpstreamErrors(errors.map(_.left.get)))
    }
  }

  def proxy(fxfs: TypedPathComponent[String]): Either[HttpResponse, Seq[Response with Closeable]] = {
    fxfs match {
      case TypedPathComponent(fourByFours, format) =>
        toLayerNames(fourByFours) match {
          case Right(layers) => getUpstreamLayers(layers)
          case Left(resp) =>
            log.warn(s"Received bad 4x4s ${fourByFours}")
            Left(resp)
        }
      case _ =>
        val warning = s"Received bad path component ${fxfs.toString}"
        log.warn(warning)
        Left(NotFound ~> Json(JsonUtil.renderJson(Map("reason" -> warning))))
    }
  }



  def handleRequest(req: HttpRequest, fxfs: TypedPathComponent[String]): HttpResponse = {
    proxy(fxfs) match {
      case Right(responses) =>
        OK ~> ContentType("application/xml") ~> Stream({out: OutputStream =>

          val streams = responses.map { r => r.inputStream() }
          Converter.execute(streams, List(), new KMLEncoder(), out) match {
            case Success(s) =>
              log.info(s"Finished writing export for ${fxfs}")
            case Failure(failure) =>
              log.warn(failure.getMessage, failure.getStackTrace)
          }
          responses.foreach(_.close())
        })
      case Left(error) => error
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
