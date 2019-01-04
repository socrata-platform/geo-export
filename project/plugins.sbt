resolvers ++= Seq("sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
                  Resolver.url("Socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns))

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" %"1.6.8")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

libraryDependencies += "com.rojoma" %% "rojoma-json-v3" % "3.9.1"
