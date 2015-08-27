package com.socrata.geoexport
package http

import javax.servlet.http.HttpServletResponse.{SC_OK => ScOk}

import com.socrata.geoexport.mocks.FixtureClient
import com.socrata.http.server.routing.TypedPathComponent
import org.mockito.Mockito.{verify, when}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.http.server.HttpRequest
import scala.xml.{NodeSeq, XML, Node}
import org.scalatest.mock.MockitoSugar

class ExportServiceTest extends TestBase with MockitoSugar {
  test("export endpoint returns a 400 on invalid 4x4 list") {

  }

  test("can get a single KML dataset") {

    val fixtureClient = new FixtureClient

    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("pt54-77iv", "kml"))
    service.get(req)(resp)

    val document = XML.loadString(outputStream.getString)
    pluckPlacemark(document).size must be(51)

  }

  test("can get a multi layer KML dataset") {

    val fixtureClient = new FixtureClient

    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("pt54-77iv,pt54-77iw", "kml"))
    service.get(req)(resp)

    val document = XML.loadString(outputStream.getString)
    val folders = document \ "Document" \ "Folder"
    folders.size must be(2)
    (folders(0) \ "Placemark").size must be(51)
    (folders(1) \ "Placemark").size must be(51)
  }

}
