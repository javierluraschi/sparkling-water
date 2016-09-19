package org.apache.spark.h2o.backends.external

/**
  * Created by kuba on 19/09/16.
  */
class H2ODriverForker {


/*
  /** Start H2O Cluster
    *
    * @param className name of class to launch as integration test
    * @param env Spark environment
    */
  def launch(className: String, env: IntegTestEnv): Unit = {
    val cmdToLaunch = Seq[String](
      "hadoop jar",
      "--class", className,
      "--jars", env.assemblyJar,
      "--verbose",
      "--master", env.sparkMaster) ++
      env.sparkConf.get("spark.driver.memory").map(m => Seq("--driver-memory", m)).getOrElse(Nil) ++
      // Disable GA collection by default
      Seq("--conf", "spark.ext.h2o.disable.ga=true") ++
      Seq("--conf", s"spark.driver.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version=${env.hdpVersion}") ++
      Seq("--conf", s"spark.yarn.am.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version=${env.hdpVersion}") ++
      Seq("--conf", s"spark.executor.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version=${env.hdpVersion}") ++
      Seq("--conf", "spark.scheduler.minRegisteredResourcesRatio=1") ++
      Seq("--conf", "spark.ext.h2o.repl.enabled=false") ++ // disable repl in tests
      Seq("--conf", s"spark.test.home=${env.sparkHome}") ++
      Seq("--conf", s"spark.driver.extraClassPath=${env.assemblyJar}") ++
      env.sparkConf.flatMap( p => Seq("--conf", s"${p._1}=${p._2}") ) ++
      Seq[String](env.itestJar)

    import scala.sys.process._
    val proc = cmdToLaunch.!
    assert (proc == 0, s"Process finished in wrong way! response=$proc from \n${cmdToLaunch mkString " "}")

  }*/
}
