/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.h2o.backends.external

import java.io.{File, FileOutputStream}
import java.net.{HttpURLConnection, URL, URLConnection}

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.SharedH2OConf
import org.apache.spark.util.h2o.Utils
import water.H2O

/**
  * External backend configuration
  */
trait ExternalBackendConf extends SharedH2OConf {
  self: H2OConf =>

  import ExternalBackendConf._
  def h2oCluster = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)

  def setYARNQueue(queueName :String) ={
    sparkConf.set(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1, queueName)
    self
  }

  def YARNQueue = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)

  def h2oClusterHost = {
    val value = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
    if (value.isDefined){
      Option(value.get.split(":")(0))
    }else{
      None
    }
  }

  def h2oClusterPort = {
    val value = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
    if (value.isDefined){
      Option(value.get.split(":")(1))
    }else{
      None
    }
  }

  def hadoopVersion = sparkConf.get(PROP_EXTERNAL_CLUSTER_HADOOP_VERSION._1)
  def h2oDriverPath = sparkConf.get(PROP_EXTERNAL_CLUSTER_H2O_DRIVER._1)
  def autoStartMode = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1) == "auto"
  def manualStartMode = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1) == "manual"
  def numOfExternalH2ONodes = sparkConf.getOption(PROP_EXTERNAL_H2O_NODES._1)

  /**
    * Sets node and port representing H2O Cluster to which should H2O connect when started in external mode.
    * This method automatically sets external cluster mode
    *
    * @param host host representing the cluster
    * @param port port representing the cluster
    * @return H2O Configuration
    */
  def setH2OCluster(host: String, port: Int): H2OConf = {
    setExternalClusterMode()
    sparkConf.set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, host + ":" + port)
    self
  }

  private[this] def setExternalClusterStartMode(startMode: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_START_MODE._1, startMode)
    self
  }
  def setExternalStartModeToAuto() : H2OConf = setExternalClusterStartMode("auto")

  def setExternalStartModeToManual() : H2OConf = setExternalClusterStartMode("manual")

  private val supportedHadoopVersions = "cdh5.2 cdh5.3 cdh5.4 cdh5.5 cdh5.6 cdh5.7 cdh5.8 hdp2.1 hdp2.2 hdp2.3 hdp2.4 mapr3.1 mapr4.0 mapr5.0 mapr5.1".split(" ")

  def setHadoopVersion(hadoopVersion: String): H2OConf = {
    if(!supportedHadoopVersions.contains(hadoopVersion)){
      throw new scala.IllegalArgumentException(s"$hadoopVersion is not supported, supported hadoop versions are $supportedHadoopVersions")
    }
    sparkConf.set(PROP_EXTERNAL_CLUSTER_HADOOP_VERSION._1, hadoopVersion)
    self
  }

  def setH2ODriver(path: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_H2O_DRIVER._1, path)
    self
  }

  def setNumOfExternalH2ONodes(numOfExternalH2ONodes: Int): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_H2O_NODES._1, numOfExternalH2ONodes.toString)
    self
  }

  def externalConfString: String =
    s"""Sparkling Water configuration:
        |  backend cluster mode : ${backendClusterMode}
        |  cloudName            : ${cloudName.getOrElse("Not set yet")}
        |  cloud representative : ${h2oCluster.getOrElse("Not set, using cloud name only")}
        |  clientBasePort       : ${clientBasePort}
        |  h2oClientLog         : ${h2oClientLogLevel}
        |  nthreads             : ${nthreads}""".stripMargin
}

object ExternalBackendConf {

  val PROP_EXTERNAL_CLUSTER_H2O_DRIVER = ("spark.ext.h2o.external.h2o.driver", null.asInstanceOf[String])

  val PROP_EXTERNAL_CLUSTER_HADOOP_VERSION = ("spark.ext.h2o.external.hadoop.version", null.asInstanceOf[String])

  /** This option can have 2 values - manual, auto. When set to manual, it is expected that
    * h2o cluster is already started and running. If it is set to auto, the h2o cluster will be started
    * automatically on specified YARN queue using h2odriver jar for corresponding hadoop version
    */
  val PROP_EXTERNAL_CLUSTER_START_MODE = ("spark.ext.h2o.external.start.mode", "manual")

  /** Yarn queue on which external cluster should be started */
  val PROP_EXTERNAL_CLUSTER_YARN_QUEUE = ("spark.ext.h2o.external.yarn.queue", null.asInstanceOf[String])

  /** ip:port of arbitrary h2o node to identify external h2o cluster */
  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE = ("spark.ext.h2o.cloud.representative",null.asInstanceOf[String])

  /** Number of nodes to wait for when connecting to external H2O cluster */
  val PROP_EXTERNAL_H2O_NODES = ("spark.ext.h2o.external.cluster.num.h2o.nodes", null.asInstanceOf[String])

  def downloadH2ODriverIfNecessary(conf: H2OConf): Unit = {
    if (conf.h2oDriverPath.isEmpty) {
      // in this case conf.hadoopVersion is not empty
      val hadoopVersion = conf.hadoopVersion
      val h2oVersion = H2O.ABV.projectVersion
      val downloadUrl = new URL(s"http://download.h2o.ai/download/h2o-$h2oVersion-$hadoopVersion")
      val outputFile = new File(Utils.createTempDir(), "h2odriver.jar")
      downloadFile(downloadUrl, outputFile)
      conf.setH2ODriver(outputFile.getAbsolutePath)
    }
  }

  /**
    * Download file from URL to given target file.
    */
  private def downloadFile(url : URL, file : File) : Unit = {
    val conn = url.openConnection
    try {
      downloadFile(conn, file)
    } finally conn match {
      case conn: HttpURLConnection => conn.disconnect()
      // But only disconnect if it's a http connection. Can't do this with file:// URLs.
      case _ =>
    }
  }

  /**
    * Download file from URL to given target file.
    */
  private def downloadFile(conn: URLConnection, file : File): Unit = {
    val in = conn.getInputStream
    try
    {
      val out = new FileOutputStream(file)
      try
      {
        Iterator
          .continually(in.read())
          .takeWhile( -1 !=)
          .foreach(out.write)
      }
      finally out.close()
    }
    finally in.close()
  }
}
