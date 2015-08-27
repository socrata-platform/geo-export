package com.socrata.geoexport.http

import java.net.URLDecoder

import com.socrata.geoexport.http.ExportService._
import com.socrata.http.client.RequestBuilder
import org.apache.http.HttpStatus
import org.joda.time.DateTime
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.util.JsonUtil
import org.slf4j.LoggerFactory
import buildinfo.BuildInfo
import java.io.{OutputStream, InputStream}
import com.socrata.geoexport.encoders.{KMLEncoder, GeoEncoder}
import com.socrata.geoexport.conversions.Converter
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import com.socrata.http.client.Response
import com.socrata.http.server.implicits.httpResponseToChainedResponse
import com.socrata.http.server.HttpService
import com.socrata.http.server.routing.TypedPathComponent
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.thirdparty.curator.CuratedServiceClient


object ExportService {
  val formats: Set[String] = Set("kml")
  type LayerResponse = (String, InputStream)

  def toLayerNames(fxfs: String): Seq[String] = {
    ///TODO: validate this
    fxfs.split(",")
  }
}

class ExportService(sodaClient: CuratedServiceClient) extends SimpleResource {
  lazy val log = LoggerFactory.getLogger(getClass)



  def getLayers(fxfs: Seq[String]): Seq[Either[Response, LayerResponse]] = {
    fxfs.map { fbf =>
      val reqBuilder = {
        base: RequestBuilder =>
          val req = base.path(Seq("resource", s"_${fbf}.geojson")).get
          log.info(URLDecoder.decode(req.toString, "UTF-8"))
          req
      }
      sodaClient.execute(reqBuilder,  { response =>
        response.resultCode match {
          case HttpStatus.SC_OK =>
            Right((fbf, response.inputStream()))
          case statusCode =>
            Left(response)
        }
      })
    }
  }



  def handleRequest(req: HttpRequest, fxfs: TypedPathComponent[String]): HttpResponse = {
    (fxfs match {
      case TypedPathComponent(fourByFours, format) =>
        val layers = toLayerNames(fourByFours)
        val (errors, successes) = getLayers(layers).partition(_.isLeft)
        (errors.map(_.left.get), successes.map(_.right.get))
      case _ =>
        (Seq((400, "Malformed dataset descriptor!")), Seq())
    }) match {
      case (Seq(), successes) =>
        OK ~> Stream({out: OutputStream =>
          val streams = successes.map{ case (_, stream) => stream }
          Converter.execute(streams, List(), new KMLEncoder(), out)
        })
      case (errors, _) =>
        BadRequest ~> Content("text/plain", "nope")
    }


//    getLayers(layers).partition(_.isLeft)
  }

  def service(fxfs: TypedPathComponent[String]): SimpleResource = {
    new SimpleResource {
      override def get: HttpService = {
        req => handleRequest(req, fxfs)
      }
    }
  }
}
