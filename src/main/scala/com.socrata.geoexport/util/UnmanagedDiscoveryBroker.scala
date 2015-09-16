package com.socrata.geoexport

import com.rojoma.simplearm.v2.{Managed, Resource}
import com.socrata.thirdparty.curator._
import org.apache.curator.x.discovery.ServiceDiscovery

import com.socrata.http.client.{HttpClient, HttpClientHttpClient}

class UnmanagedDiscoveryBroker(discovery: ServiceDiscovery[Void],
                                        httpClient: HttpClient) {
  def clientFor(config: CuratedClientConfig): Managed[UnmanagedCuratedServiceClient] = {
    for {
      sp <- ServiceProviderFromName[Void](discovery, config.serviceName)
    } yield {
      val provider = CuratorServerProvider(httpClient, sp, identity)
      UnmanagedCuratedServiceClient(provider, config)
    }
  }
}
