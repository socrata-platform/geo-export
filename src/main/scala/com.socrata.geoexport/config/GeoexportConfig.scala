package com.socrata.geoexport.config

import com.typesafe.config.ConfigFactory

import com.socrata.thirdparty.curator.{DiscoveryConfig, CuratedClientConfig, DiscoveryBrokerConfig}

// $COVERAGE-OFF$
object GeoexportConfig {
  private lazy val config = ConfigFactory.load().getConfig("com.socrata")

  lazy val port = config.getInt("geoexport.port")

  lazy val threadpool = config.getConfig("geoexport.threadpool")

  lazy val broker = new DiscoveryBrokerConfig(config, "curator")

  lazy val upstream = new CuratedClientConfig(config, "upstream")
}
// $COVERAGE-ON$
