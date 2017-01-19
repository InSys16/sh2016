/**
  * Baseline for hackaton
  */

import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import breeze.numerics.abs
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.lib.PageRank

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

case class GraphFriend(uid: Int, group: Int)//, interactionScore: Double = 0f)
case class UserFriends(uid: Int, friends: Array[GraphFriend])
case class Demography(age: Int, gender: Int, position : Int)

case class Pair(uid1: Int,
                uid2: Int,
                features: Features)

object Baseline {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Baseline")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._

    val dataDir = if (args.length == 1) args(0) else "./"

    //val dataDir = "D:\\SNAHackaton2016\\"
    val graphPath = dataDir + "trainGraph"
    val reversedGraphPath = dataDir + "reversedGraph"
    val pairsPath = dataDir + "pairs"
    val demographyPath = dataDir + "demography"
    val predictionPath = dataDir + "Prediction"
    val modelPath = dataDir + "LogisticRegressionModel"
    val pageRankPath = dataDir + "PageRank"
    val regionProximityPath = dataDir + "RegionProximity"
    val numGraphParts = 200
    val numPairsParts = 107

    // read graph, flat and reverse it
    // step 1.a from description

    /**
      * list of [User * Friends[]]
      */
    
    }
    /*
    def calculateRegionsProximity() = {
      val x =
        graph
          .flatMap(uf => {
            val userRegion = demographyBC.value.getOrElse(uf.uid, Demography(0, 0, 0)).position
            uf.friends
              .map(fr => {
                val friendRegion = demographyBC.value.getOrElse(fr.uid, Demography(0, 0, 0)).position
                if (userRegion < friendRegion)
                  (userRegion, friendRegion)
                else
                  (friendRegion, userRegion)
              })
          }).groupBy(w => w)
            .map(x => (x._1._1, x._1._2, x._2.size))
            .toDF().write.parquet(regionProximityPath)

    }
    */
    def loadRegionsProximity() = {
      sqlc.read
        .parquet(regionProximityPath)
        .map{case Row(f1: Int, f2: Int, prox : Int) => (f1,f2) -> prox}
        .collectAsMap()
    }

    //calculateRegionsProximity()
    

    def calculatePageRank() = {
      val edges = {
      sc.textFile(graphPath)
        .flatMap(line => {
          val lineSplit = line.split("\t")
          val user = lineSplit(0).toInt
          val friends = {
            lineSplit(1)
              .replace("{(", "")
              .replace(")}", "")
              .split("\\),\\(")
              .map(t => Edge(user, t.split(",")(0).toInt, 1))
          }
          friends
        })
      val graphForPageRank = Graph.fromEdges(edges, 1)
      val pageRank =
        graphForPageRank.pageRank(5)
          .vertices
        .map(vert => (vert._1.toInt, vert._2))
      val maxPageRank = pageRank.map(pair => pair._2).max()
      val normalized =
        pageRank.map(pair => (pair._1.toInt, pair._2 / maxPageRank))
      val x = normalized.toDF().write.parquet(pageRankPath)
    }

    def loadPageRank() = {
      sqlc.read
        .parquet(pageRankPath)
        .map{case Row(k: Int, v: Double) => k -> v}
        .collectAsMap()
    }

    calculatePageRank()

    //val pageRankBC = sc.broadcast(loadPageRank())
    /*
    val graph = {
      sc.textFile(graphPath)
        .map(line => {
          val lineSplit = line.split("\t")
          val user = lineSplit(0).toInt
          val friends = {
            lineSplit(1)
              .replace("{(", "")
              .replace(")}", "")
              .split("\\),\\(")
              .map(t => GraphFriend(t.split(",")(0).toInt,
                                    GroupDefiner.defineGroupByNumber(t.split(",")(1).toInt)))
          }
          UserFriends(user, friends)
        })

    val coreUsers = graph.map(user => user.uid)
    val coreUsersBC = sc.broadcast(coreUsers.collect().toSet)
    /*
    graph
      .filter(user => user.friends.length >= 8 && user.friends.length <= 1000)
      .flatMap(user => user.friends.map(x => (x.uid, GraphFriend(user.uid, x.group))))
      .groupByKey(numGraphParts)
      .map(t => UserFriends(t._1, t._2.toArray
        .filter(x => coreUsersBC.value.contains(x.uid))
        .sortWith(_.uid < _.uid)))
      .filter(userFriends => userFriends.friends.length >= 2 && userFriends.friends.length <= 2000)
      .toDF.write.parquet(reversedGraphPath)
    */
    val reversedGraph = IO.readReversedGraph(sqlc, reversedGraphPath)

    val mainUsersFriendsCount = graph.map(user => user.uid -> user.friends.length)
    val otherUsersFriendsCount = reversedGraph.map(user => user.uid -> user.friends.length)
    val friendsCount = mainUsersFriendsCount.union(otherUsersFriendsCount)
    val friendsCountBC = sc.broadcast(friendsCount.collectAsMap())

    def generatePairs(userFriends: UserFriends,
                      numOfPart: Int,
                      coreUsers: Broadcast[Set[Int]],
                      friendsCount: Broadcast[Map[Int, Int]]) = {
      val pairs = ArrayBuffer.empty[((Int, Int), Features)]

      val commonFriendFriendsCount = friendsCount.value.getOrElse(userFriends.uid, 0)
      val commonFriendAdamicAdar = if (commonFriendFriendsCount >= 2) 1.0 / Math.log(commonFriendFriendsCount.toDouble) else 1.0
      //val commonUserPageRank = pageRankBC.value.getOrElse(userFriends.uid, 0.0)
      val fedorScore = 100.0 / Math.pow(commonFriendFriendsCount.toDouble + 5, 1.0/3.0) - 8

      for (i <- userFriends.friends.indices) {
        val user1 = userFriends.friends(i)
        if (user1.uid % numPairsParts == numOfPart) {
          for (j <- i + 1 until userFriends.friends.length) {
            val user2 = userFriends.friends(j)
            val features = Features(commonFriendAdamicAdar,
              1,
              fedorScore,
              //commonUserPageRank,
              GroupDefiner.getGroupsScoresByCommonFriendGroups(user1.group, user2.group))

            pairs.append(((user1.uid, user2.uid), features))
          }
        }
      }
      pairs
    }
    /*
    for (part <- 0 until numPairsParts) {
      val pairs = {
        reversedGraph
          .flatMap(t => generatePairs(t, part, coreUsersBC, friendsCountBC))
          .reduceByKey((features1, features2) => FeatureHelper.sumFeatures(features1, features2))
          .filter(pair => pair._2.commonFriendsCount > 8)
          .map(p => Pair(p._1._1, p._1._2, p._2))
      }

      pairs.map(pair => {(
        pair.uid1,
        pair.uid2,
        pair.features.adamicAdar,
        pair.features.commonFriendsCount,
        pair.features.fedorScore,
        //pair.features.pageRank,
        pair.features.groupScores.commonRelatives,
        pair.features.groupScores.commonColleagues,
        pair.features.groupScores.commonSchoolmates,
        pair.features.groupScores.commonArmyFellows,
        pair.features.groupScores.commonFriends)    })
        .toDF.repartition(4).write.parquet(pairsPath + "/part_" + part)
    }
    */
	*/
    //val pairs = IO.readPairs(sqlc, pairsPath + "/part_*/")
    /*
    val pairScoreMap =
      pairs
        .map(pair => (pair.uid1, pair.uid2) -> pair.features.commonFriendsCount)

    val pairScoreMapBC = sc.broadcast(pairScoreMap.collectAsMap())


    val userFriendsMap =
      graph
        .map(userFriends => userFriends.uid -> userFriends.friends.map(graphFriend => graphFriend.uid))

    val userFriendsMapBC = sc.broadcast(userFriendsMap.collectAsMap())

    def simRank(x : Int, y : Int) = if (x == y) 1.0 else {
      var sum = 0
      for (friend1 <- userFriendsMapBC.value(x)) {
        for (friend2 <- userFriendsMapBC.value(y)) {
          sum += pairScoreMapBC.value.getOrElse((friend1, friend2), 0)
        }
      }
      sum.toDouble / (friendsCountBC.value(x) * friendsCountBC.value(y)).toDouble
    }

    pairs
      .map(pair => {(
        pair.uid1,
        pair.uid2,
        pair.features.adamicAdar,
        pair.features.commonFriendsCount,
        pair.features.groupScores.commonRelatives,
        pair.features.groupScores.commonColleagues,
        pair.features.groupScores.commonSchoolmates,
        pair.features.groupScores.commonArmyFellows,
        pair.features.groupScores.commonFriends,
        simRank(pair.uid1, pair.uid2))
      })
      .toDF.repartition(4).write.parquet(dataDir + "pairsWithSimRank")
    */
    /*
    /// (coreUser1,  coreUser2) -> 1.0  :  coreUser1 < coreUser2
    /// I.e. realFriends
    val positives = graph.flatMap(
          userFriends => userFriends.friends
            //take friends that in coreUsersSet and > user
            .filter(x => coreUsersBC.value.contains(x.uid) && userFriends.uid < x.uid)
            .map(x => (userFriends.uid, x.uid) -> 1.0)
        )

    val pairsForLearning = IO.readPairs(sqlc, pairsPath + "/part_33")


    val demography = {
      sc.textFile(demographyPath)
        .map(line => { // 0userId 1create_date 2birth_date 3gender 4ID_country 5ID_Location 6loginRegion
        val lineSplit = line.trim().split("\t")
          if (lineSplit(2) == "") {
            lineSplit(0).toInt -> Demography(0, lineSplit(3).toInt, lineSplit(5).toInt)
          }
          else {
            lineSplit(0).toInt -> Demography(lineSplit(2).toInt, lineSplit(3).toInt, lineSplit(5).toInt)
          }
        })
    }
    val demographyBC = sc.broadcast(demography.collectAsMap())

    val regionsProximityBC = sc.broadcast(loadRegionsProximity())

    def prepareData( pairs: RDD[Pair], positives: RDD[((Int, Int), Double)]) = {
      pairs
        .map(pair => FeatureExtractor.getFeatures(pair, demographyBC, friendsCountBC, regionsProximityBC))
        .leftOuterJoin(positives)
    }

    val dataForLearning = {
      prepareData(pairsForLearning, positives)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }

    val splits = dataForLearning.randomSplit(Array(0.2, 0.8), seed = 11L)
    val trainingData = splits(0).cache()
    val validationData = splits(1)

    // run training algorithm to build the model
    val model = {
      new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(trainingData)
    }

    // try to use RandomForest
    /*
    val treeStrategy = Strategy.defaultStrategy("Classification")
    val numTrees = 100 // Use more in practice.
    val featureSubsetStrategy = "auto"


    val model = RandomForest.trainClassifier(trainingData, treeStrategy, numTrees, featureSubsetStrategy, 152645)

    val testErr = validationData.map { point =>
      val prediction = model.predict(point.features)
      if (point.label == prediction) 1.0 else 0.0
    }//.mean()
    */
    //val model = RandomForestModel.load(sc, modelPath)
    model.clearThreshold()

    model.save(sc, modelPath)
    val predictionAndLabels = {
      validationData.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
    }

    // estimate model quality
    @transient val metricsLogReg = new BinaryClassificationMetrics(predictionAndLabels, 100)
    val threshold = metricsLogReg.fMeasureByThreshold(2.0).sortBy(-_._2).take(1)(0)._1

    //val rocLogReg = metricsLogReg.areaUnderROC()
    //println("model ROC = " + rocLogReg.toString)
	*/
    // compute scores on the test set
    //val testCommonFriendsCounts = {
    //  IO.readPairs(sqlc, pairsPath + "/part_*/")
    //    .filter(pair => pair.uid1 % 11 == 7 || pair.uid2 % 11 == 7)
    //}
    /*
    prepareData(testCommonFriendsCounts, positives)
      .map(t => t._1 -> LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
      .filter(t => t._2.label == 0.0)

      .flatMap { case (id, LabeledPoint(label, features)) =>
        val prediction = model.predict(features)
        Seq(id._1 -> (id._2, prediction), id._2 -> (id._1, prediction))
      }
      .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
      .groupByKey(numGraphParts)

      .map(t => {
        val user = t._1
        val friendsWithRatings = t._2.toList
        val topBestFriends = friendsWithRatings.sortBy(-_._2).take(100).map(x => x._1)
        (user, topBestFriends)
      })
      .sortByKey(true, 1)
      .map(t => t._1 + "\t" + t._2.mkString("\t"))

      .saveAsTextFile(predictionPath, classOf[GzipCodec])
      */
  }
}

