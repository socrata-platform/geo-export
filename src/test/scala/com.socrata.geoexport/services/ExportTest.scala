package com.socrata.geoexport
package http

import java.io.{FileOutputStream, File}
import java.util.UUID
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

  test("can get a single Shapefile dataset") {

    val fixtureClient = new FixtureClient

    val req = mock[HttpRequest]

    val fileName = s"/tmp/export_test_${UUID.randomUUID()}.zip"
    val file = new File(fileName)


    def exportShape(tpc: TypedPathComponent[String]) = {
        val outputStream = new mocks.FileServletOutputStream(file)
        val resp = outputStream.responseFor

        val service = new ExportService(fixtureClient.client).service(tpc)
        service.get(req)(resp)
        verify(resp).setStatus(200)

        readShapeArchive(file) match {
          case Seq((featureType, features)) =>
            features.size must be(77)
        }
    }


    exportShape(new TypedPathComponent("vt5y-77dn", "shp"))
    exportShape(new TypedPathComponent("vt5y-77dn", "shapefile"))

  }

  test("can get a multi layer Shapefile dataset") {
    val fixtureClient = new FixtureClient

    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt5y-77do", "shp"))
    service.get(req)(resp)

    verify(resp).setStatus(200)
  }

  test("a 400 is returned on an invalid 4x4") {
    val fixtureClient = new FixtureClient
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt", "kml"))
    service.get(req)(resp)

    verify(resp).setStatus(400)
  }

  test("a 502 is returned on an unknown 4x4 and the error message is helpful") {
    val fixtureClient = new FixtureClient
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-zzzz", "kml"))
    service.get(req)(resp)

    verify(resp).setStatus(502)

    outputStream.getString must be("""{"reason":[{"status":404,"reason":{"mock":"reason"}}]}""")
  }

  test("kml export has mimetype application/vnd.google-earth.kml+xml") {
    val fixtureClient = new FixtureClient
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "kml"))
    service.get(req)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/vnd.google-earth.kml+xml")
  }

  test("shp export has mimetype application/zip") {
    val fixtureClient = new FixtureClient
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "shp"))
    service.get(req)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/zip")
  }

  test("kmz export has mimetype application/vnd.google-earth.kmz") {
    val fixtureClient = new FixtureClient
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "kmz"))
    service.get(req)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/vnd.google-earth.kmz")
  }


}
