name := "geo-export"

scalaVersion := "2.11.7"

externalResolvers := Seq(
   "Socarata SBT Repo" at "https://repo.socrata.com/artifactory/socrata-sbt-repo/",
   "Socrata Artifactory" at "https://repo.socrata.com/artifactory/libs-release/",
   "Socrata Artifactory Snapshot" at "https://repo.socrata.com/artifactory/libs-snapshot/",
   "Socrata Jcenter" at "https://repo.socrata.com/artifactory/jcenter/",
   Resolver.url("Socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns),
  Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns))

val JettyVersion = "9.2.10.v20150310"

libraryDependencies ++= Seq(
  "ch.qos.logback"           % "logback-classic"          % "1.1.3",
  "com.rojoma"              %% "rojoma-json-v3"           % "3.9.1",
  "com.rojoma"              %% "rojoma-json-v3-jackson"   % "1.0.0" excludeAll(
    ExclusionRule(organization = "com.rojoma")),
  "com.rojoma"              %% "simple-arm-v2"            % "2.3.1",
  "com.socrata"             %% "socrata-curator-utils"    % "1.1.2" excludeAll(
    ExclusionRule(organization = "com.socrata", name = "socrata-http-client"),
    ExclusionRule(organization = "com.socrata", name = "socrata-http-jetty")),
  "com.socrata"             %% "socrata-http-client"      % "3.11.4" excludeAll(
    ExclusionRule(organization = "com.rojoma"),
    ExclusionRule(organization = "com.socrata", name = "socrata-thirdparty-utils_2.10")),
  "com.socrata"             %% "socrata-http-jetty"       % "3.11.4" excludeAll(
    ExclusionRule(organization = "com.rojoma"),
    ExclusionRule(organization = "com.socrata", name = "socrata-thirdparty-utils_2.10")),
  "com.socrata"             %% "socrata-thirdparty-utils" % "4.0.16",

  "com.typesafe"             % "config"                   % "1.2.1",

  "commons-codec"            % "commons-codec"            % "1.10",
  "commons-io"               % "commons-io"               % "2.4",
  // curator versions in the 4.x range are incompatible with our current zk version 3.4.14
  "org.apache.curator"       % "curator-x-discovery"      % "2.7.0",
  "com.socrata"             %% "soql-pack"                % "2.11.13",

  "org.geotools"             % "gt-shapefile"             % "14.0"
)

libraryDependencies ++= Seq(
  "org.mockito"              % "mockito-core"             % "1.10.19" % "test",
  "org.scalacheck"          %% "scalacheck"               % "1.11.6" % "test",
  "com.socrata"             %% "socrata-test-common"      % "0.5.3" % "test"
)

val TestOptionNoTraces = "-oD"
val TestOptionShortTraces = "-oDS"
val TestOptionFullTraces = "-oDF"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, TestOptionNoTraces)

// Setup revolver.
Revolver.settings

com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false

sourceGenerators in Compile <+= (sourceManaged in Compile, version, scalaVersion) map { (root, version, scalaVersion) =>
  import com.rojoma.json.v3.ast.JString
  import java.io.FileWriter

  val target = root / "BuildInfo.scala"

  root.mkdirs()
  val result = s"""package buildinfo

object BuildInfo {
  val version = ${JString(version)}
  val scalaVersion = ${JString(scalaVersion)}
  val buildTime = ${System.currentTimeMillis}L
}
"""

  val w = new FileWriter(target)
  try {
    w.write(result)
  } finally {
    w.close()
  }

  Seq(target)
}
