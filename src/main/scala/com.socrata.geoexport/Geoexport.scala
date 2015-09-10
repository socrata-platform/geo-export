package com.socrata.geoexport

import java.util.concurrent.Executors
import javax.servlet.http.HttpServletRequest
import org.geotools.factory.FactoryRegistry
import org.geotools.referencing.ReferencingFactoryFinder
import org.slf4j.LoggerFactory

import com.rojoma.simplearm.v2.conversions._
import com.rojoma.simplearm.v2.{Resource, managed}

import com.socrata.http.client.{HttpClientHttpClient, RequestBuilder}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService, SocrataServerJetty}
import com.socrata.thirdparty.curator._
import http.{VersionService, ExportService}
import config.GeoexportConfig

// $COVERAGE-OFF$ Disabled because this is framework boilerplate.
object Geoexport extends App {

  implicit val shutdownTimeout = Resource.executorShutdownNoTimeout

  val logger = LoggerFactory.getLogger(getClass)

  for {
    executor <- managed(Executors.newCachedThreadPool())
    http <- managed(new HttpClientHttpClient(executor,
                                             HttpClientHttpClient.
                                               defaultOptions.
                                               withUserAgent("geoexport")))

    broker <- DiscoveryBrokerFromConfig(GeoexportConfig.broker, http)
    curator <- CuratorFromConfig(GeoexportConfig.broker.curator)
    discovery <- DiscoveryFromConfig(classOf[Void], curator, GeoexportConfig.broker.discovery)
    upstream <- new UnmanagedDiscoveryBroker(discovery, http).clientFor(GeoexportConfig.upstream)
  } {


    val wat = new CuratorBroker(discovery, GeoexportConfig.broker.discovery.address, GeoexportConfig.broker.discovery.name, None)
    wat.register(GeoexportConfig.port)

    val exportService = new ExportService(upstream)


    val router = new Router(exportService)

    val server = new SocrataServerJetty(
      handler = router.handler,
      options = SocrataServerJetty.defaultOptions.
        withPort(GeoexportConfig.port).
        withPoolOptions(SocrataServerJetty.Pool(GeoexportConfig.threadpool)))

    logger.info("Starting Geoexport")
    server.run()
  }
}
  // $COVERAGE-ON$
