name := "geo-export"

scalaVersion := "2.12.17"

externalResolvers := Seq(
   "Socarata SBT Repo" at "https://repo.socrata.com/artifactory/socrata-sbt-repo/",
   "Socrata Artifactory" at "https://repo.socrata.com/artifactory/libs-release/",
   "Socrata Artifactory Snapshot" at "https://repo.socrata.com/artifactory/libs-snapshot/",
   "Socrata Jcenter" at "https://repo.socrata.com/artifactory/jcenter/",
   Resolver.url("Socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns),
  Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns))

libraryDependencies ++= Seq(
  "ch.qos.logback"           % "logback-classic"          % "1.1.3",
  "com.rojoma"              %% "rojoma-json-v3"           % "3.9.1",
  "com.rojoma"              %% "rojoma-json-v3-jackson"   % "1.0.0",
  "com.rojoma"              %% "simple-arm-v2"            % "2.3.1",
  "com.socrata"             %% "socrata-curator-utils"    % "1.2.0",
  "com.socrata"             %% "socrata-http-client"      % "3.16.0",
  "com.socrata"             %% "socrata-http-jetty"       % "3.16.0",
  "com.socrata"             %% "socrata-thirdparty-utils" % "5.1.0",

  "com.typesafe"             % "config"                   % "1.2.1",

  "commons-codec"            % "commons-codec"            % "1.10",
  "commons-io"               % "commons-io"               % "2.4",
  // curator versions in the 4.x range are incompatible with our current zk version 3.4.14
  "org.apache.curator"       % "curator-x-discovery"      % "2.7.0",
  "com.socrata"             %% "soql-pack"                % "4.12.7",

  "org.geotools"             % "gt-shapefile"             % "26.0"
)

libraryDependencies ++= Seq(
  "org.mockito"              % "mockito-core"             % "1.10.19" % "test",
  "org.scalacheck"          %% "scalacheck"               % "1.11.6" % "test",
  "org.scalatest"           %% "scalatest"                % "3.0.0" % "test"
)

val TestOptionNoTraces = "-oD"
val TestOptionShortTraces = "-oDS"
val TestOptionFullTraces = "-oDF"

evictionErrorLevel := Level.Warn

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, TestOptionNoTraces)

Compile / sourceGenerators += Def.task {
  import com.rojoma.json.v3.ast.JString
  import java.io.FileWriter

  val target = (Compile/sourceManaged).value / "BuildInfo.scala"

  (Compile/sourceManaged).value.mkdirs()
  val result = s"""package buildinfo

object BuildInfo {
  val version = ${JString(version.value)}
  val scalaVersion = ${JString(scalaVersion.value)}
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

assembly/assemblyMergeStrategy ~= { old =>
  {
    case x if x.endsWith("module-info.class") => MergeStrategy.discard // J9 module stuff
    case "plugin.xml" | "plugin.properties" => MergeStrategy.discard // IDE plugin stuff
    case x => old(x)
  }
}
