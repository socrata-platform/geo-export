package com.socrata.geoexport.encoders

import com.socrata.soql.SoQLPackIterator
import com.socrata.soql.types._


package geoexceptions {
  case class MultipleGeometriesFoundException(message: String) extends Exception
  case class UnknownGeometryException(message: String) extends Exception
}

package object geotypes {
  type Layers = Iterable[SoQLPackIterator]
  type Schema = Seq[(String, SoQLType)]
  type Fields = Array[_ <: SoQLValue]
}
