package com.socrata.geoexport

import com.rojoma.json.v3.interpolation._
import org.slf4j.LoggerFactory

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext.{Route, Routes}
import com.socrata.http.server.routing.TypedPathComponent
import com.socrata.http.server.util.RequestId.{ReqIdHeader, generate}
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler}
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}

import http.{VersionService, ExportService}

// $COVERAGE-OFF$ Disabled because this is basically configuration.
class Router(export: ExportService) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val logWrapper =
    NewLoggingHandler(LoggingOptions(logger, Set("X-Socrata-Host",
                                                 "X-Socrata-RequestId",
                                                 "X-Socrata-Resource"))) _

  val availableFormats = ExportService.formats
  println(availableFormats)
  /** Routing table. */
  val routes = Routes(
    Route("/version", VersionService),

    //wrapped services
    Route("/export/{{String!availableFormats}}", export.service _)
  )

  /** 404 error. */
  val notFound: HttpService = req => {
    logger.warn("path not found: {}", req.requestPathStr)
    NotFound ~>
      Json(json"""{error:"not found"}""")
  }

  val handler: HttpRequest => HttpResponse = req =>
    logWrapper(routes(req.requestPath).getOrElse(notFound))(req)
}
// $COVERAGE-ON$
