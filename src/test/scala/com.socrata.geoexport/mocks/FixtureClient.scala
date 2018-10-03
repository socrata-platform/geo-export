package com.socrata.geoexport.mocks

import java.io.{Closeable, InputStream}
import java.nio.charset.{Charset, StandardCharsets}

import com.socrata.geoexport.UnmanagedCuratedServiceClient
import com.socrata.http.client.{RequestBuilder, Response, SimpleHttpRequest}
import com.socrata.http.common.util.Acknowledgeable
import com.socrata.curator.{CuratedClientConfig, ServerProvider}
import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.scalatest.mock.MockitoSugar




class FixtureClient extends MockitoSugar {

  def getStream(fixtureName: String) = Option(getClass.getResourceAsStream(s"/fixtures/http/${fixtureName}"))

  class FixtureInputStream(fixtureName: String) extends InputStream with Acknowledgeable {
    val underlying = {
      getStream(fixtureName) match {
        case Some(is) => is
        case None =>
          IOUtils.toInputStream("""{"mock": "reason"}""")
      }
    }
    override def acknowledge(): Unit = ()
    override def read(): Int = underlying.read
  }


  def getFixture(builder: (RequestBuilder => SimpleHttpRequest)): Response with Closeable = {

    new Response with Closeable {

      var status: Int = 200

      private def getFilename: String = {
        val request = builder(RequestBuilder(""))
        request.builder.path.toList match {
          case Seq("resource", uidAndFormat) => uidAndFormat
        }
      }

      override def charset: Charset = StandardCharsets.UTF_8

      override def streamCreated: Boolean = true

      override def inputStream(maximumSizeBetweenAcks: Long): InputStream with Acknowledgeable = {
        new FixtureInputStream(this.getFilename)
      }

      override def resultCode: Int = {
        getStream(this.getFilename) match {
          case Some(_) => 200
          case _ => 404
        }
      }

      override def close: Unit = None

      override def headerNames: Set[String] = Set()

      override def headers(name: String): Array[String] = Array.empty

    }
  }

  val emptyConfig = new CuratedClientConfig(mock[Config], "") {
    override val serviceName = ""
    override val connectTimeout = 0
    override val maxRetries = 0
  }

  val client = new UnmanagedCuratedServiceClient(mock[ServerProvider], emptyConfig) {
    override def execute[T](request: RequestBuilder => SimpleHttpRequest): Response with Closeable = {
      getFixture(request)
    }
  }
}
