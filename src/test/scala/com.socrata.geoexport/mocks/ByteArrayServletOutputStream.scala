package com.socrata.geoexport.mocks

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletResponse

import org.mockito.Mockito.{mock, when}

class ByteArrayServletOutputStream extends ServletOutputStream {
  val underlying: ByteArrayOutputStream = new ByteArrayOutputStream

  def write(x: Int): Unit = underlying.write(x)
  def isReady(): Boolean = true
  def setWriteListener(x: javax.servlet.WriteListener): Unit = {}

  def getBytes: Array[Byte] = underlying.toByteArray
  def getString: String = new String(getBytes, UTF_8)
  // Shortened so it is the same length as "getString"
  def getLowStr: String = getString.toLowerCase

  val responseFor: HttpServletResponse = {
    val resp = mock(classOf[HttpServletResponse])
    when(resp.getOutputStream()).thenReturn(this)
    resp
  }
}
