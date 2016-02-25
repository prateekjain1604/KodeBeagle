/*
 *Created by jatina on 22/2/16.
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
package com.kodebeagle.spark

import java.net.InetAddress
import java.util
import com.google.common.collect.TreeMultiset
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.common.settings.Settings.Builder

import scala.Predef
import scala.util.Random


import com.kb.GraphUtils;
import com.kb.java.graph.{NamedDirectedGraph}
import com.kb.java.model.{Cluster, Clusterer}

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.spark.SparkIndexJobHelper._
import com.kodebeagle.logging.Logger
import com.google.common.collect.TreeMultiset
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.spark._
import scala.collection.JavaConversions._
import scala.collection.{Map}


object CreateCollisionGraph extends Logger {
  val esPortKey = "es.port"
  val esNodesKey = "es.nodes"
  val jobName = "CreateCollisionGraph"
  val conf = new SparkConf().setAppName(jobName).setMaster("local[*]")
  conf.set("es.http.timeout", "5m")
  conf.set("es.scroll.size", "20")

  val settings: Settings = Settings.settingsBuilder().put("cluster.name", "kodebeagle1").build()

  val transportClient = TransportClient.builder().settings(settings).build()
    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.149"), 9301))

//  val transportClient = new TransportClient(
//    ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build()
//  ).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.140"), 9301))

  /* Fetches fileContent from the elastic search for the given fileName */
  def getSourceFileContent(transportClient: TransportClient, fileName: String): String = {
    val fileSourceSearchRequest: SearchRequestBuilder = transportClient.prepareSearch("sourcefile")
      .setTypes("typesourcefile")
      .addField("fileContent");
    val fileSourceSearchResponse: SearchResponse = fileSourceSearchRequest.setQuery(
      QueryBuilders.matchQuery("fileName", fileName)
    ).execute().get()
    fileSourceSearchResponse.getHits.getAt(0).getFields.get("fileContent").getValue[String]
  }

  /* Fetches uniques list of Apis from importMethods index of elastic search */
  def getApis(sc: SparkContext): RDD[String] = {
       val query = """{
                  | "fields": [
                  |   "tokens.importName"
                  | ]
                  |}""".stripMargin

    sc.esRDD(KodeBeagleConfig.eImportsMethodsIndex, query).flatMap {
          case(repoId, valuesMap) => {
            try {valuesMap.get("tokens.importName").map(_.asInstanceOf[scala.collection.convert.Wrappers.JListWrapper[String]].toList)}
            catch  {
              case ex:Exception => None
            }
          }
        }.flatMap(_.distinct).distinct()

  }

  def main(args: Array[String]) {
    conf.set(esNodesKey, args(0))
    conf.set(esPortKey, args(1))
    val sc: SparkContext = createSparkContext(conf)

    /* Step : Getting unique list of apis from importMethodsIndex across all the indexed data */
//    val listOfApis: RDD[String] =  getApis(sc)
    val apiRanks = sc.textFile("/home/jatina/workspace/PageRankDump_desc/apiRanks").cache()
    val sortedApis: RDD[(Double, String)] = apiRanks.map{a =>
      val b = a.replaceAll("\\(|\\)", "").split(",")
      var rank = 0.0
      try {
        if(b(1) != "")
          rank = b(1).toDouble
      }
      catch {
        case ex: Exception => log.error("Exception converting  string to number", ex)
          rank = 0.0
      }
      (rank,b(0).toString)
    }.filter(item => item._1 > 1.0)
    val listOfApis: List[String] = sortedApis.values.take(100000).toList

//    val listOfApis = List("java.io.BufferedReader", "java.nio.channels.FileChannel", "java.io.PrintWriter", "java.io.File")

    /* Query to obtain information about given apiName*/
    def query(apiName: String) =
      //"{\n  \"query\": {\n    \"bool\": {\n      \"must\": [\n        {\n          \"term\": {\n            \"typeimportsmethods.tokens.importName\": \"" + apiName + "\"\n          }\n        }\n      ],\n      \"must_not\": [],\n      \"should\": []\n    }\n  }\n}"
      s"""{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {
        |          "term": {
        |            "tokens.importName":"$apiName"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin


    /*Step 2: Method to obtain all (fileName, ApiName) pairs by any given apiName*/

    def getApiNameAndFiles(apiName: String): RDD[(String, String)] = {
      sc.esRDD(KodeBeagleConfig.eImportsMethodsIndex, query(apiName.toLowerCase())).map {
        case (repoId, valuesMap) => {
          val score = valuesMap.get("score").getOrElse(null).asInstanceOf[Int]
          val fileName: String = valuesMap.get("file").getOrElse("").asInstanceOf[String]
          (fileName, score) -> apiName
        }
      }.filter { case ((fileName, score), apiName) => score >= 200}.map { case ((fileName, score), apiName) => (fileName, apiName)}
    }

    /* Obtaining an list of RDD containing  (FileName, List of ApiNames) */
    val apiFileInfo: List[RDD[(String, Iterable[String])]] = listOfApis.map { apiName =>
      getApiNameAndFiles(apiName).groupByKey()
    }

    /*Step 3: Combining list of RDD's into a single RDD using reduceLeft  */
    val fileWithApiNames: RDD[(String, List[String])] = apiFileInfo.reduceLeft(_ ++ _).reduceByKey(_ ++ _).mapValues(_.toList.distinct)

    fileWithApiNames.persist(StorageLevel.MEMORY_AND_DISK)
    println("$$$$$$$$$$ Number of files : "+fileWithApiNames.count())
    /*Step 4: Obtaining list of Weighted Directed Graphs for each Api*/
    val apiGraphs: RDD[(String, List[NamedDirectedGraph])] = fileWithApiNames.flatMap { case (fileName, apiNames) =>
      val fileContent: String = getSourceFileContent(transportClient, fileName)
      val graphUtils: GraphUtils = new GraphUtils()
      val apiWeightedGraphs: List[(String, List[NamedDirectedGraph])] = apiNames.map { api =>
        val graphsInfo: util.HashMap[String, NamedDirectedGraph] = new util.HashMap[String, NamedDirectedGraph]()
        val apiGraphList: util.List[NamedDirectedGraph] = graphUtils.getGraphsFromFile(fileContent, api, fileName)
        graphUtils.getNamedDirectedGraphs(graphsInfo, apiGraphList)
        (api, graphsInfo.values().toList)
      }
      apiWeightedGraphs
    }.reduceByKey(_ ++ _)


    /* Printing number of obtained directed graph for each Api*/
    apiGraphs.persist(StorageLevel.MEMORY_AND_DISK)
    val samplingApiGraphs: RDD[(String, List[NamedDirectedGraph])] = apiGraphs.mapValues{list =>
      val totalGraphs = list.size
      if(totalGraphs > 1000)
        Random.shuffle(list).take(1000)
      else
        list
    }

    val apiGraphsCount: List[(String, Int)] = apiGraphs.mapValues(list => list.size).collect().toList
    apiGraphsCount.foreach { it =>
      val apiName: String = it._1
      val apiCount: Int = it._2
      println("$$$$$$$$$$$$$" + apiName + "," + apiCount + "$$$$$$$$$$$$$$$$$$$")
    }

    fileWithApiNames.unpersist()
    /* Step 6: Applying Kmedoid algorithm for clustering on list of graphs to obtain Abstract graph and Concrete Graph for each Api*/
    val noc = 5
    val edgeSupport = 0.2

    val clustering: RDD[(String, List[(String, Int, util.List[String], String, String, Int, Int)])] = samplingApiGraphs.mapValues { it =>
      val clusterClass = new Clusterer()
      val apiMinerMerging = new GraphUtils()
      val clusters: util.List[Cluster[NamedDirectedGraph]] = clusterClass.getClusters(it, noc, 0.7D/noc)
      clusters.map { cluster =>
        var collisionGraph: NamedDirectedGraph = new NamedDirectedGraph()
        val seedNames: TreeMultiset[String] = TreeMultiset.create()
        val graphsInCluster: util.List[NamedDirectedGraph] = cluster.getInstances
        val clusterSize = cluster.getSizeOfCluster
        graphsInCluster.foreach { graphInstance =>
          seedNames.add(graphInstance.getSeedName)
          collisionGraph = apiMinerMerging.mergeGraphs(collisionGraph, graphInstance)
        }
        apiMinerMerging.trim(collisionGraph, graphsInCluster.size() * edgeSupport)
        val conGraph = cluster.getMean
        (exportGraphs(collisionGraph), clusterSize, apiMinerMerging.getTopN(seedNames,5), conGraph.getMethodName, conGraph.getFileName,
          conGraph.getStartLineNumber, conGraph.getEndLineNumber)
      }.toList
    }
    clustering.persist(StorageLevel.MEMORY_AND_DISK)
    println("$$$$$$$$$$$$$" + clustering.count() + "$$$$$$$$$$$$$$$$$$$")
    apiGraphs.unpersist()

    /* Step 7 : Converting obtained graphs into dot format to store Api graph patterns into Json format */
    val printingGraphs: RDD[String] = clustering.flatMap {
      case (apiName, apiListOfAbstractGraphAndConcreteGraph) => {
        var clusterIndex: Int = 0
        apiListOfAbstractGraphAndConcreteGraph.map { graphTuple =>
          val (abst, clusterSize, seedNames, methodName, fileName, startLineNumber, endLineNumber) = graphTuple
          clusterIndex = clusterIndex + 1
          toJson(ApiPatternIndex(apiName, clusterIndex, abst, clusterSize, seedNames.toList, methodName, fileName,
            startLineNumber, endLineNumber))
        }
      }
    }

    printingGraphs.persist()
    println("@@@@@@@@@@@@@@@Total number of graphs@@@@@@@@@@@@@: " +printingGraphs.count())
    clustering.unpersist()
    printingGraphs.saveAsTextFile("/home/jatina/graphResults/resForOneLakhApis")
//    printingGraphs.saveJsonToEs("apipatternindex/typeapipatternindex", Map("es.write.operation" -> "index"))
//    printingGraphs.saveJsonToEs("apipatternindex/typeapipatternindex")
    sc.stop()
  }

  def exportGraphs(graph: NamedDirectedGraph): String = {
    val graphUtils = new GraphUtils()
    graphUtils.saveToString(graph)
  }

  def longRunningOperation(): Int = {
    Thread.sleep(15000);
    1
  }

}

case class ApiPatternIndex(api: String, clusterIndex: Int, abstractGraph : String,
                           clusterSize: Int, seedNames: List[String], methodName: String,
                           fileName: String, startlineNumber : Int, endLineNumber: Int)