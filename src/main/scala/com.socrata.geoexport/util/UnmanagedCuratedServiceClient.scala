package com.socrata.geoexport

import java.io.Closeable

import com.socrata.http.client.{RequestBuilder, Response, SimpleHttpRequest}
import com.socrata.thirdparty.curator.ServerProvider.{RetryWhen, Complete}
import com.socrata.thirdparty.curator.{CuratedClientConfig, ServerProvider, ServiceDiscoveryException}
import org.slf4j.LoggerFactory

/**
  * Manages connections and requests to the provided service.
  * @param provider Service discovery object.
  * @param config The configuration for this client.
  */
case class UnmanagedCuratedServiceClient(provider: ServerProvider,
                                config: CuratedClientConfig) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val connectTimeout = config.connectTimeout
  private val maxRetries = config.maxRetries

  /**
    * Sends a get request to the provided service.
    * @return HTTP response code and body
    */
  def execute[T](request: RequestBuilder => SimpleHttpRequest): Response with Closeable = {
    val requestWithTimeout = { base: RequestBuilder =>
      val req = base.connectTimeoutMS match {
        case Some(timeout) => base
        case None => base.connectTimeoutMS(connectTimeout)
      }

      request(req)
    }

    provider.withRetriesUnmanaged(maxRetries,
                         requestWithTimeout,
                         ServerProvider.RetryOnAllExceptionsDuringInitialRequest: RetryWhen) {
      throw ServiceDiscoveryException(s"Failed to discover service: ${config.serviceName}")
    }
  }
}
