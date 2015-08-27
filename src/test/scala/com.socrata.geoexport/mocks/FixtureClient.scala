package com.socrata.geoexport.mocks

import com.socrata.geoexport.http.ExportService
import com.socrata.http.server.routing.TypedPathComponent
import com.typesafe.config.Config
import java.io.InputStream
import java.nio.charset.{StandardCharsets, Charset}

import com.socrata.http.common.util.Acknowledgeable
import com.socrata.thirdparty.curator.{CuratedClientConfig, CuratedServiceClient}
import com.socrata.http.client.{RequestBuilder, Response, SimpleHttpRequest}
import com.socrata.thirdparty.curator.ServerProvider
import org.scalatest.mock.MockitoSugar




class FixtureClient extends MockitoSugar {

  class FixtureInputStream(fixtureName: String) extends InputStream with Acknowledgeable {
    println(s"Loading features /fixtures/http/${fixtureName}.json")
    val underlying = getClass.getResourceAsStream(s"/fixtures/http/${fixtureName}.json")
    override def acknowledge(): Unit = ()
    override def read(): Int = underlying.read
  }


  def getFixture(builder: (RequestBuilder => SimpleHttpRequest)): Response = {
    new Response {

      override def charset: Charset = StandardCharsets.UTF_8

      override def streamCreated: Boolean = true

      override def inputStream(maximumSizeBetweenAcks: Long): InputStream with Acknowledgeable = {
        val request = builder(RequestBuilder(""))
        val uid = (request.builder.path.toList)(1) match {
          case uidAndFormat =>
            (uidAndFormat.toString.split("\\.").toList)(0)
        }
        new FixtureInputStream(uid)
      }

      override def resultCode: Int = 200

      override def headerNames: Set[String] = Set()

      override def headers(name: String): Array[String] = Array.empty

    }
  }

  val emptyConfig = new CuratedClientConfig(mock[Config], "") {
    override val serviceName = ""
    override val connectTimeout = 0
    override val maxRetries = 0
  }

  val client = new CuratedServiceClient(mock[ServerProvider], emptyConfig) {
    override def execute[T](request: (RequestBuilder => SimpleHttpRequest), callback: Response => T): T = {


      callback(getFixture(request))
    }
  }
}