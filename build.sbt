import sbt.Keys._
import sbt._

//// config scala(c) ////
//autoScalaLibrary := false
//scalaVersion := "2.11.7"
//scalacOptions ++= Seq("-target:jvm-1.8")
//scalacOptions ++= Seq("-encoding", "utf8")
//scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
//scalacOptions ++= Seq("-optimise")
// scalacOptions ++= Seq("-Yinline-warnings")
// scalacOptions ++= Seq("-Xexperimental")
//ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
/////////////////////////

lazy val common = Seq(
  organization  := "io.landy",
  version       := "0.0.1",

  scalaVersion      := "2.11.7",
  autoScalaLibrary  := false,

  scalacOptions     := Seq("-target:jvm-1.8")
                    ++ Seq("-encoding", "utf8")
                    ++ Seq("-unchecked", "-deprecation", "-feature")
                    ++ Seq("-optimise"),
//                    ++ Seq("-Yinline-warnings")
//                    ++ Seq("-Xexperimental")

  ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
)

/// Project ///////////////////////////////////////////////////////////////////////////////////////

lazy val root = (project in file("."))
  .settings(common: _*)
  .settings(
    name := "zax"
  )

//// One-Jar //////////////////////////////////////////////////////////////////////////////////////
import com.github.retronym.SbtOneJar._

oneJarSettings
///////////////////////////////////////////////////////////////////////////////////////////////////

resolvers += "sonatype-releases"  at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.scalaz"          %%  "scalaz-core"    % "7.1.3" withSources(),
  "org.specs2"          %%  "specs2-core"    % "2.4.16" % "test"
)

libraryDependencies ++= {
  val ver = "2.3.12"
  Seq(
    "com.typesafe.akka"   %%  "akka-actor"     % ver withSources(),
    "com.typesafe.akka"   %%  "akka-testkit"   % ver % "test"
  )
}

libraryDependencies ++= {
  val sprayVer      = "1.3.3"
  val sprayJsonVer  = "1.3.2"
  Seq(
    "io.spray"            %%  "spray-can"      % sprayVer withSources(),
    "io.spray"            %%  "spray-routing"  % sprayVer withSources(),
    "io.spray"            %%  "spray-caching"  % sprayVer withSources(),
    "io.spray"            %%  "spray-testkit"  % sprayVer  % "test",
    "io.spray"            %%  "spray-json"     % sprayJsonVer withSources()
  )
}

libraryDependencies ++= {
  val sparkVersion = "1.5.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion withSources(),
    "org.apache.spark" %% "spark-mllib" % sparkVersion withSources()
  )
}

libraryDependencies ++= {
  Seq(
    "com.typesafe.play"  %% "play-iteratees" % "2.4.2",
    "org.reactivemongo"  %% "reactivemongo"  % "0.11.3"
  )
}

libraryDependencies ++= {
  Seq(
    "org.scala-lang.modules" %% "scala-pickling" % "0.10.2-SNAPSHOT"
  )
}

libraryDependencies ++= {
  // https://scalameter.github.io/
  // http://scalameter.github.io/home/gettingstarted/0.7/configuration/index.html
  Seq(
    //"com.storm-enroute" %% "scalameter" % "0.7-SNAPSHOT" % "test"
  )
}


/// Local Bindings ////////////////////////////////////////////////////////////////////////////////

lazy val runMongo = taskKey[Unit]("Starts the MongoDB instance locally")

runMongo := {
  println("Starting MongoD")
  "mongod --fork --config /usr/local/etc/mongod.conf"!
}

lazy val stopMongo = taskKey[Unit]("Stops the MongoDB local instance")

stopMongo := {
  println("Stopping MongoD")
  "mongod --shutdown"!
}

lazy val runWithMongo = taskKey[Unit]("Runs the app starting MongoDB-daemon locally!")

runWithMongo := Def.sequential(runMongo, (run in Compile).toTask(""), stopMongo).value


lazy val runSpark = taskKey[Unit]("Starts the local instance of Spark's master")

runSpark := {
  println("Starting Spark Master")
  "$SPARK_HOME/sbin/start-master.sh -p 7077 --webui-port 8082"!
}

lazy val stopSpark = taskKey[Unit]("Stops the local instance of Spark's master")

stopSpark := {
  println("Stopping Spark Master")
  "$SPARK_HOME/sbin/stop-master.sh"!
}

lazy val runWithSpark = taskKey[Unit]("Runs the app starting Spark's Master instance locally!")

runWithSpark := Def.sequential(runSpark, (run in Compile).toTask(""), stopSpark).value