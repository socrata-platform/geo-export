package com.socrata.geoexport
package http

import scala.collection.JavaConverters._

import java.io.{FileOutputStream, File}
import java.util.UUID
import javax.servlet.http.HttpServletResponse.{SC_OK => ScOk}
import javax.servlet.http.HttpServletRequest

import com.socrata.geoexport.mocks.FixtureClient
import com.socrata.http.server.routing.TypedPathComponent
import org.mockito.Mockito.{verify, when}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.http.server.HttpRequest
import scala.xml.{NodeSeq, XML, Node}
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.rojoma.json.v3.ast._
import org.scalatest.mock.MockitoSugar
import com.rojoma.simplearm.v2._

class ExportTest extends TestBase  {

  def Unused: HttpRequest = new HttpRequest {
    override def servletRequest = new HttpRequest.AugmentedHttpServletRequest(
      new HttpServletRequest {
        override def getQueryString = ""
        override def getHeaderNames = java.util.Collections.enumeration(Nil.asJava)
        override def getHeader(name: String) = null
        override def getHeaders(name: String) = java.util.Collections.enumeration(Nil.asJava)
        override def getIntHeader(name: String) = -1
        override def getDateHeader(name: String) = -1L
        override def getMethod = "GET"
        override def authenticate(x$1: javax.servlet.http.HttpServletResponse): Boolean = ???
        override def changeSessionId(): String = ???
        override def getAuthType(): String = ???
        override def getContextPath(): String = ???
        override def getCookies(): Array[javax.servlet.http.Cookie] = ???
        override def getPart(x$1: String): javax.servlet.http.Part = ???
        override def getParts(): java.util.Collection[javax.servlet.http.Part] = ???
        override def getPathInfo(): String = ???
        override def getPathTranslated(): String = ???
        override def getRemoteUser(): String = ???
        override def getRequestURI(): String = ???
        override def getRequestURL(): StringBuffer = ???
        override def getRequestedSessionId(): String = ???
        override def getServletPath(): String = ???
        override def getSession(): javax.servlet.http.HttpSession = ???
        override def getSession(x$1: Boolean): javax.servlet.http.HttpSession = ???
        override def getUserPrincipal(): java.security.Principal = ???
        override def isRequestedSessionIdFromCookie(): Boolean = ???
        override def isRequestedSessionIdFromURL(): Boolean = ???
        override def isRequestedSessionIdFromUrl(): Boolean = ???
        override def isRequestedSessionIdValid(): Boolean = ???
        override def isUserInRole(x$1: String): Boolean = ???
        override def login(x$1: String,x$2: String): Unit = ???
        override def logout(): Unit = ???
        override def upgrade[T <: javax.servlet.http.HttpUpgradeHandler](x$1: Class[T]): T = ???
        override def getAsyncContext(): javax.servlet.AsyncContext = ???
        override def getAttribute(x$1: String): Object = ???
        override def getAttributeNames(): java.util.Enumeration[String] = ???
        override def getCharacterEncoding(): String = ???
        override def getContentLength(): Int = ???
        override def getContentLengthLong(): Long = ???
        override def getContentType(): String = ???
        override def getDispatcherType(): javax.servlet.DispatcherType = ???
        override def getInputStream(): javax.servlet.ServletInputStream = ???
        override def getLocalAddr(): String = ???
        override def getLocalName(): String = ???
        override def getLocalPort(): Int = ???
        override def getLocale(): java.util.Locale = ???
        override def getLocales(): java.util.Enumeration[java.util.Locale] = ???
        override def getParameter(x$1: String): String = ???
        override def getParameterMap(): java.util.Map[String,Array[String]] = ???
        override def getParameterNames(): java.util.Enumeration[String] = ???
        override def getParameterValues(x$1: String): Array[String] = ???
        override def getProtocol(): String = ???
        override def getReader(): java.io.BufferedReader = ???
        override def getRealPath(x$1: String): String = ???
        override def getRemoteAddr(): String = ???
        override def getRemoteHost(): String = ???
        override def getRemotePort(): Int = ???
        override def getRequestDispatcher(x$1: String): javax.servlet.RequestDispatcher = ???
        override def getScheme(): String = ???
        override def getServerName(): String = ???
        override def getServerPort(): Int = ???
        override def getServletContext(): javax.servlet.ServletContext = ???
        override def isAsyncStarted(): Boolean = ???
        override def isAsyncSupported(): Boolean = ???
        override def isSecure(): Boolean = ???
        override def removeAttribute(x$1: String): Unit = ???
        override def setAttribute(x$1: String,x$2: Any): Unit = ???
        override def setCharacterEncoding(x$1: String): Unit = ???
        override def startAsync(x$1: javax.servlet.ServletRequest,x$2: javax.servlet.ServletResponse): javax.servlet.AsyncContext = ???
        override def startAsync(): javax.servlet.AsyncContext = ???
      }
    )
    override val resourceScope = new ResourceScope
  }

  test("can get a single KML dataset") {

    val fixtureClient = new FixtureClient

    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "kml"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)

    val document = XML.loadString(outputStream.getString)
    pluckPlacemark(document).size must be(77)
    (document \ "Document" \ "Folder" \ "Placemark" \ "MultiGeometry").size must be(77)
  }

  test("can get a multi layer KML dataset") {

    val fixtureClient = new FixtureClient

    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt5y-77do", "kml"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)

    val document = XML.loadString(outputStream.getString)
    val folders = document \ "Document" \ "Folder"
    folders.size must be(2)
    (folders(0) \ "Placemark").size must be(77)
    (folders(1) \ "Placemark").size must be(77)
  }

  test("can get a single Shapefile dataset") {

    val fixtureClient = new FixtureClient


    val fileName = s"/tmp/export_test_${UUID.randomUUID()}.zip"
    val file = new File(fileName)


    def exportShape(tpc: TypedPathComponent[String]) = {
        val outputStream = new mocks.FileServletOutputStream(file)
        val resp = outputStream.responseFor

        val service = new ExportService(fixtureClient.client).service(tpc)
        service.get(Unused)(resp)
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

    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt5y-77do", "shp"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)
  }

  test("can get a multi layer geoJSON dataset") {

    val fixtureClient = new FixtureClient

    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt5y-77do", "geojson"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)

    val js = JsonReader.fromString(outputStream.getString)

    js match {
      case JObject(fields) => println(fields.keys)
        fields("features") match {
          case JArray(features) =>
            //geoJSON merges heterogenous shapes into the same feature list, so this list will be the sum
            //of the length of each fixture
            features.size must be (77 + 77)
          case unexpected => throw new Exception(s"Expected JArray, found ${unexpected}")
        }
      case unexpected => throw new Exception(s"Expected JObject, found ${unexpected}")
    }
  }


  test("a 400 is returned on an invalid 4x4") {
    val fixtureClient = new FixtureClient
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn,vt", "kml"))
    service.get(Unused)(resp)

    verify(resp).setStatus(400)
  }

  test("a 502 is returned on an unknown 4x4 and the error message is helpful") {
    val fixtureClient = new FixtureClient
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-zzzz", "kml"))
    service.get(Unused)(resp)

    verify(resp).setStatus(404)

    outputStream.getString must be("""{"reason":[{"status":404,"reason":{"mock":"reason"}}]}""")
  }

  test("kml export has mimetype application/vnd.google-earth.kml+xml") {
    val fixtureClient = new FixtureClient
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "kml"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/vnd.google-earth.kml+xml")
  }

  test("shp export has mimetype application/zip") {
    val fixtureClient = new FixtureClient
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "shp"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/zip")
  }

  test("kmz export has mimetype application/vnd.google-earth.kmz") {
    val fixtureClient = new FixtureClient
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "kmz"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/vnd.google-earth.kmz")
  }

  test("geoJSON export has mimetype application/vnd.geo+json") {
    val fixtureClient = new FixtureClient
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    val service = new ExportService(fixtureClient.client).service(new TypedPathComponent("vt5y-77dn", "geojson"))
    service.get(Unused)(resp)

    verify(resp).setStatus(200)
    verify(resp).setContentType("application/vnd.geo+json")
  }


}
