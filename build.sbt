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


//// use it with `sbt one-jar` ////
import com.github.retronym.SbtOneJar._

oneJarSettings
///////////////////////////////////

//name := "zax"
//version := "0.0.1"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

// commons
libraryDependencies ++= Seq(
  "org.scalaz"          %%  "scalaz-core"    % "7.1.3" withSources(),
  "org.specs2"          %%  "specs2-core"    % "2.4.16" % "test"
)

// akka
libraryDependencies ++= {
  val akkaV = "2.3.12"
  Seq(
    "com.typesafe.akka"   %%  "akka-actor"     % akkaV withSources(),
    "com.typesafe.akka"   %%  "akka-testkit"   % akkaV % "test"
  )
}

// spray
libraryDependencies ++= {
  val sprayV = "1.3.3"
  val sprayJsonV = "1.3.2"
  Seq(
    "io.spray"            %%  "spray-can"      % sprayV withSources(),
    "io.spray"            %%  "spray-routing"  % sprayV withSources(),
    "io.spray"            %%  "spray-caching"  % sprayV withSources(),
    "io.spray"            %%  "spray-testkit"  % sprayV  % "test",
    "io.spray"            %%  "spray-json"     % sprayJsonV withSources()
  )
}

// mongo
libraryDependencies ++= {
  Seq(
    "com.typesafe.play"  %%  "play-iteratees"  % "2.4.2",
    "org.reactivemongo"  %% "reactivemongo"    % "0.11.3"
  )
}

// jackson
libraryDependencies ++= {
  Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.5.3"
  )
}

// benchmarks
libraryDependencies ++= {
  // https://scalameter.github.io/
  // http://scalameter.github.io/home/gettingstarted/0.7/configuration/index.html
  Seq(
    //"com.storm-enroute" %% "scalameter" % "0.7-SNAPSHOT" % "test"
  )
}

// spark
libraryDependencies ++= {
  val sparkVersion = "1.4.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion withSources(),
    "org.apache.spark" %% "spark-mllib" % sparkVersion withSources()
  )
}


/// Local Bindings ////////////////////////////////////////////////////////////////////////////////

lazy val runMongo = taskKey[Unit]("Starts the MongoDB instance locally")

runMongo := {
  println("MongoD started")
  "mongod --fork --config /usr/local/etc/mongod.conf"!
}

lazy val stopMongo = taskKey[Unit]("Stops the MongoDB local instance")

stopMongo := {
  println("MongoD stopped")
  "mongod --shutdown"!
}

lazy val runWithMongo = taskKey[Unit]("Runs the app starting MongoDB-daemon locally!")

runWithMongo := Def.sequential(runMongo, (run in Compile).toTask(""), stopMongo).value