package com.socrata.geoexport
package http

import javax.servlet.http.HttpServletResponse.{SC_OK => ScOk}

import org.mockito.Mockito.{verify, when}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.http.server.HttpRequest

class VersionServiceTest extends TestBase with MockitoSugar {
  test("version endpoint") {
    val req = mock[HttpRequest]
    val outputStream = new mocks.ByteArrayServletOutputStream
    val resp = outputStream.responseFor

    VersionService.get(req)(resp)

    verify(resp).setStatus(ScOk)
    verify(resp).setContentType("application/json; charset=UTF-8")

    outputStream.getLowStr must include ("health")
    outputStream.getLowStr must include ("alive")
    outputStream.getLowStr must include ("buildtime")
    outputStream.getLowStr must include ("scalaversion")
  }

}
