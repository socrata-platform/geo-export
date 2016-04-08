name := "geo-export"

resolvers ++= Seq(
  "ecc" at "https://github.com/ElectronicChartCentre/ecc-mvn-repo/raw/master/releases",
  "velvia maven" at "https://dl.bintray.com/velvia/maven",
  "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases",
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"
)

val JettyVersion = "9.2.10.v20150310"

libraryDependencies ++= Seq(
  "ch.qos.logback"           % "logback-classic"          % "1.1.3",
  "com.rojoma"              %% "rojoma-json-v3"           % "3.3.0",
  "com.rojoma"              %% "rojoma-json-v3-jackson"   % "1.0.0" excludeAll(
    ExclusionRule(organization = "com.rojoma")),
  "com.rojoma"              %% "simple-arm-v2"            % "2.1.0",
  "com.socrata"             %% "socrata-curator-utils"    % "1.0.3" excludeAll(
    ExclusionRule(organization = "com.socrata", name = "socrata-http-client"),
    ExclusionRule(organization = "com.socrata", name = "socrata-http-jetty")),
  "com.socrata"             %% "socrata-http-client"      % "3.8.1" excludeAll(
    ExclusionRule(organization = "com.rojoma"),
    ExclusionRule(organization = "com.socrata", name = "socrata-thirdparty-utils_2.10")),
  "com.socrata"             %% "socrata-http-jetty"       % "3.8.1-SNAPSHOT" excludeAll(
    ExclusionRule(organization = "com.rojoma"),
    ExclusionRule(organization = "com.socrata", name = "socrata-thirdparty-utils_2.10")),
  "com.socrata"             %% "socrata-thirdparty-utils" % "4.0.1",

  "com.typesafe"             % "config"                   % "1.2.1",

  "commons-codec"            % "commons-codec"            % "1.10",
  "commons-io"               % "commons-io"               % "2.4",
  "org.apache.curator"       % "curator-x-discovery"      % "2.7.0",
  "com.socrata"             %% "soql-pack"                % "0.8.3",

  "org.geotools"             % "gt-shapefile"             % "14.0"
)

libraryDependencies ++= Seq(
  "org.mockito"              % "mockito-core"             % "1.10.19" % "test",
  "org.scalacheck"          %% "scalacheck"               % "1.11.6" % "test",
  "com.socrata"             %% "socrata-test-common"      % "0.2.8" % "test"
)

val TestOptionNoTraces = "-oD"
val TestOptionShortTraces = "-oDS"
val TestOptionFullTraces = "-oDF"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, TestOptionNoTraces)

// Setup revolver.
Revolver.settings


