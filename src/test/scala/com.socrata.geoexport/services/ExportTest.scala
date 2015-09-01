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

class ExportTest extends TestBase with MockitoSugar {

  test("can get a single KML dataset") {

    val fixtureClient = new FixtureClient

    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "kml"))
    service.get(req)(resp)

    verify(resp).setStatus(200)

    val document = XML.loadString(outputStream.getString)
    pluckPlacemark(document).size must be(77)
    (document \ "Document" \ "Folder" \ "Placemark" \ "MultiGeometry").size must be(77)
  }

  test("can get a multi layer KML dataset") {

    val fixtureClient = new FixtureClient

    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt5y-77do", "kml"))
    service.get(req)(resp)

    verify(resp).setStatus(200)

    val document = XML.loadString(outputStream.getString)
    val folders = document \ "Document" \ "Folder"
    folders.size must be(2)
    (folders(0) \ "Placemark").size must be(77)
    (folders(1) \ "Placemark").size must be(77)
  }


  test("a 400 is returned on an invalid 4x4") {
    val fixtureClient = new FixtureClient
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt", "kml"))
    service.get(req)(resp)

    println(outputStream.getString)

    verify(resp).setStatus(400)
  }


  test("a soqlpack error is returned when requesting malformed soqlpack") {
    val fixtureClient = new FixtureClient
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("nope-nope", "kml"))
    service.get(req)(resp)
    println(outputStream.getString)
    verify(resp).setStatus(200)

  }
}
