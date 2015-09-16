package com.socrata.geoexport.mocks

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletResponse
import java.io.{FileOutputStream, File}
import org.mockito.Mockito.{mock, when}

class FileServletOutputStream(file: File) extends ServletOutputStream {
  val underlying: FileOutputStream = new FileOutputStream(file)

  def write(x: Int): Unit = underlying.write(x)
  def isReady(): Boolean = true
  def setWriteListener(x: javax.servlet.WriteListener): Unit = {}

  val responseFor: HttpServletResponse = {
    val resp = mock(classOf[HttpServletResponse])
    when(resp.getOutputStream()).thenReturn(this)
    resp
  }

}
