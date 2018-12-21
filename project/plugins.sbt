resolvers ++= Seq("sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
                  "Socrata Artifactory" at "https://repo.socrata.com/artifactory/libs-release")

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" %"1.6.8")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.7.2")

libraryDependencies += "com.rojoma" %% "rojoma-json-v3" % "3.9.1"
