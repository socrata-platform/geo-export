package com.socrata.geoexport.util

/**
  Class that allows task and encode operations to map on to 
  reasonable http failures

*/
trait Failure {
  // def unapply: ActionResult = T(message)
}

class InvalidFormat(message: String) extends Failure {
  // def foo: ActionResult = BadRequest
}