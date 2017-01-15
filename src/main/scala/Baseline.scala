/**
  * Baseline for hackaton
  */


import breeze.numerics.abs
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

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
    val reversedGraphPath = dataDir + "trainSubReversedGraph"
    val pairsPath = dataDir + "pairs"
    val demographyPath = dataDir + "demography"
    val predictionPath = dataDir + "Prediction"
    val modelPath = dataDir + "LogisticRegressionModel"
    val numGraphParts = 200
    val numPairsParts = 107

    // read graph, flat and reverse it
    // step 1.a from description

    /**
      * list of [User * Friends[]]
      */
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
    }

    val coreUsers = graph.map(user => user.uid)
    val coreUsersBC = sc.broadcast(coreUsers.collect().toSet)

    graph
      .filter(user => user.friends.length >= 8 && user.friends.length <= 1000)
      .flatMap(user => user.friends.map(x => (x.uid, GraphFriend(user.uid, x.group))))
      .groupByKey(numGraphParts)
      .map(t => UserFriends(t._1, t._2.toArray
        .filter(x => coreUsersBC.value.contains(x.uid))
        .sortWith(_.uid < _.uid)))
      .filter(userFriends => userFriends.friends.length >= 2 && userFriends.friends.length <= 2000)
      .toDF.write.parquet(reversedGraphPath)

    val reversedGraph = IO.readReversedGraph(sqlc, reversedGraphPath)

    val mainUsersFriendsCount = graph.map(user => user.uid -> user.friends.length)
    val otherUsersFriendsCount = reversedGraph.map(user => user.uid -> user.friends.length)
    val friendsCount = mainUsersFriendsCount.union(otherUsersFriendsCount)
    val friendsCountBC = sc.broadcast(friendsCount.collectAsMap())

    def generatePairs(userFriends: UserFriends,
                      numOfPart: Int,
                      coreUsers: Broadcast[Set[Int]],
                      //pageRanks: Broadcast[Map[Int, Double]],
                      friendsCount: Broadcast[Map[Int, Int]]) = {
      val pairs = ArrayBuffer.empty[((Int, Int), Features)]

      val commonFriendFriendsCount = friendsCount.value.getOrElse(userFriends.uid, 0)
      val commonFriendScore = if (commonFriendFriendsCount >= 2) 1.0 / Math.log(commonFriendFriendsCount.toDouble) else 1.0

      for (i <- userFriends.friends.indices) {
        val user1 = userFriends.friends(i)
        if (user1.uid % numPairsParts == numOfPart) {
          for (j <- i + 1 until userFriends.friends.length) {
            val user2 = userFriends.friends(j)
            val features = Features(commonFriendScore,
              1,
              GroupDefiner.getGroupsScoresByCommonFriendGroups(user1.group, user2.group))

            pairs.append(((user1.uid, user2.uid), features))
          }
        }
      }
      pairs
    }

    for (part <- 0 until numPairsParts) {
      val pairs = {
        reversedGraph
          //.map(t => generatePairs(re, numPartitionsGraph, k))
          //.flatMap(pair => pair.map(x => x -> 1))
          //.reduceByKey((x, y) => x + y)
          //.map(t => PairWithCommonFriends(t._1._1, t._1._2, t._2))
          //.filter(pair => pair.commonFriendsCount > 8)
          .flatMap(t => generatePairs(t, part, coreUsersBC, /*pageRanks,*/ friendsCountBC))
          .reduceByKey((features1, features2) => FeatureHelper.sumFeatures(features1, features2))
          .map(p => Pair(p._1._1, p._1._2, p._2))
      }

      IO.writePairs(pairs, pairsPath + "/part_" + part)
    }
    val pairs = IO.readPairs(sqlc, pairsPath + "/part_33")

    val positives = graph.flatMap(
          userFriends => userFriends.friends
            //take friends that in Set and > user(remove duplicates)
            .filter(x => coreUsersBC.value.contains(x.uid) && x.uid > userFriends.uid)
            .map(x => (userFriends.uid, x.uid) -> 1.0)
        )

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

    def prepareData( pairs: RDD[Pair],
                     positives: RDD[((Int, Int), Double)]) = {
      pairs
        .map(pair => FeatureExtractor.getFeatures(pair, demographyBC, friendsCountBC))
        .leftOuterJoin(positives)
    }
    val data = {
      prepareData(pairs, positives)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }

    val splits = data.randomSplit(Array(0.1, 0.9), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1)

    // run training algorithm to build the model
    val model = {
      new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(training)
    }

    model.clearThreshold()
    model.save(sc, modelPath)

    val predictionAndLabels = {
      validation.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
    }

    // estimate model quality
    @transient val metricsLogReg = new BinaryClassificationMetrics(predictionAndLabels, 100)
    val threshold = metricsLogReg.fMeasureByThreshold(2.0).sortBy(-_._2).take(1)(0)._1

    val rocLogReg = metricsLogReg.areaUnderROC()
    println("model ROC = " + rocLogReg.toString)

    // compute scores on the test set
    // step 7
    val testCommonFriendsCounts = {
      IO.readPairs(sqlc, pairsPath + "/part_*/")
        .filter(pair => pair.uid1 % 11 == 7 || pair.uid2 % 11 == 7)
    }

    prepareData(testCommonFriendsCounts, positives)
      .map(t => t._1 -> LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
      .filter(t => t._2.label == 0.0)

      .flatMap { case (id, LabeledPoint(label, features)) =>
        val prediction = model.predict(features)
        Seq(id._1 -> (id._2, prediction), id._2 -> (id._1, prediction))
      }
      .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
      .groupByKey(numPairsParts)

      .map(t => {
        val user = t._1
        val firendsWithRatings = t._2
        val topBestFriends = firendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
        (user, topBestFriends)
      })
      .sortByKey(true, 1)
      .map(t => t._1 + "\t" + t._2.mkString("\t"))

      .saveAsTextFile(predictionPath, classOf[GzipCodec])

  }
}
