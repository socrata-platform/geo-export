externalResolvers ++= Seq(Resolver.url("Socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns))

libraryDependencies += "com.rojoma" %% "rojoma-json-v3" % "3.9.1"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
