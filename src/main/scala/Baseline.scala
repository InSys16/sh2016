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
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class PairWithCommonFriends(
            user1: Int,
            user2: Int,
            commonFriendsCount: Int,
            typeOfConnection: Int)

case class GraphFriend(uid: Int, mask: Int)//, interactionScore: Double = 0f)
case class UserFriends(uid: Int, friends: Array[GraphFriend])
case class AgeGenderPosition(age: Int, gender: Int, position : Int)

object Baseline {

  def getTypeOfConnection(id : Int) = id match {
    case 1 => 0
    case 2

    1. Love
      2. Spouse
      3. Parent
      4. Child
      5. Brother/Sister
      6. Uncle/Aunt
      7. Relative
      8. Close friend
    9. Colleague
      10. Schoolmate
      11. Nephew
      12. Grandparent
      13. Grandchild
      14. College/University fellow
    15. Army fellow
    16. Parent in law
      17. Child in law
      18. Godparent
      19. Godchild
      20. Playing together
  }

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
    val commonFriendsPath = dataDir + "commonFriendsCountsPartitioned"
    val demographyPath = dataDir + "demography"
    val predictionPath = dataDir + "ModifiedPredictionForAge"
    val modelPath = dataDir + "ModifiedLogisticRegressionModelForAge"
    val numPartitions = 200
    val numPartitionsGraph = 107

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
              .map(t => GraphFriend(t.split(",")(0).toInt, t.split(",")(1).toInt))
          }
          UserFriends(user, friends)
        })
    }

    graph
      .filter(user => user.friends.length >= 8 && user.friends.length <= 1000)
      .flatMap(user => user.friends.map(x => (x.uid, GraphFriend(user.uid, x.mask))))
      .groupByKey(numPartitions)
      .map(t => UserFriends(t._1, t._2.toArray.sortWith(_.uid < _.uid)))
      .filter(userFriends => userFriends.friends.length >= 2 && userFriends.friends.length <= 2000)
      .toDF
      .write.parquet(reversedGraphPath)

      //.filter(userFriends => userFriends.friends.length >= 8 && userFriends.friends.length <= 1000)
      //.flatMap(userFriends => userFriends.friends.map(x => (x, userFriends.user)))
      //.groupByKey(numPartitions)
      //.map(t => UserFriends(t._1, t._2.toArray))
      //.map(userFriends => userFriends.friends.sorted)
      //.filter(friends => friends.length >= 2 && friends.length <= 2000)
      //.map(friends => new Tuple1(friends))
      //.toDF
      //.write.parquet(reversedGraphPath)

    // for each pair of ppl count the amount of their common friends
    // amount of shared friends for pair (A, B) and for pair (B, A) is the same
    // so order pair: A < B and count common friends for pairs unique up to permutation
    // step 1.b

    def generatePairs(pplWithCommonFriends: Seq[Int], numPartitions: Int, k: Int) = {
      val pairs = ArrayBuffer.empty[(Int, Int)]
      for (i <- 0 until pplWithCommonFriends.length) {
        if (pplWithCommonFriends(i) % numPartitions == k) {
          for (j <- i + 1 until pplWithCommonFriends.length) {
            pairs.append((pplWithCommonFriends(i), pplWithCommonFriends(j)))
          }
        }
      }
      pairs
    }

    val reversedGraph = sqlc.read.parquet(reversedGraphPath)

    for (k <- 0 until numPartitionsGraph) {
      val commonFriendsCounts = {
        reversedGraph
          .map(t => generatePairs(t.getAs[Seq[Int]](0), numPartitionsGraph, k))
          .flatMap(pair => pair.map(x => x -> 1))
          .reduceByKey((x, y) => x + y)
          .map(t => PairWithCommonFriends(t._1._1, t._1._2, t._2))
          .filter(pair => pair.commonFriendsCount > 8)
      }

      commonFriendsCounts.toDF.repartition(4).write.parquet(commonFriendsPath + "/part_" + k)
    }

    // prepare data for training model
    // step 2

    val commonFriendsCounts = {
      sqlc
        .read.parquet(commonFriendsPath + "/part_33")
        .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1), t.getAs[Int](2)))
    }

    // step 3
    val usersBroadcastSet = sc.broadcast(graph.map(userFriends => userFriends.user).collect().toSet)

    /**
      * pairs (user * friend) and prob = 1
      */
    val positives = {
      graph
        .flatMap(
          userFriends => userFriends.friends
            //take friends that in Set and > user(remove duplicates)
            .filter(x => usersBroadcastSet.value.contains(x) && x > userFriends.user)
            .map(x => (userFriends.user, x) -> 1.0)
        )
    }

    // step 4
    val userToAgeSex = {
      sc.textFile(demographyPath)
        .map(line => { // 0userId 1create_date 2birth_date 3gender 4ID_country 5ID_Location 6loginRegion
          val lineSplit = line.trim().split("\t")
          if (lineSplit(2) == "") {
            lineSplit(0).toInt -> AgeGenderPosition(0, lineSplit(3).toInt, lineSplit(5).toInt)
          }
          else {
            lineSplit(0).toInt -> AgeGenderPosition(lineSplit(2).toInt, lineSplit(3).toInt, lineSplit(5).toInt)
          }
        })
    }

    val userToAgeSexBC = sc.broadcast(userToAgeSex.collectAsMap())

    val userToFriendsCount = graph.map(uf => uf.user -> uf.friends.length)
    val userToFriendsCountBC = sc.broadcast(userToFriendsCount.collectAsMap())



    def prepareData( commonFriendsCounts: RDD[PairWithCommonFriends],
                     pairsWithProb: RDD[((Int, Int), Double)],
                     userToAgeSexBC: Broadcast[scala.collection.Map[Int, AgeGenderPosition]]) = {
      commonFriendsCounts
        .map(pair => FeatureExtractor.getFeatures(pair))
        .leftOuterJoin(pairsWithProb)
    }

    /**
      * list of points (prob * vector)
      */
    val data = {
      prepareData(commonFriendsCounts, positives, userToAgeSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }


    // split data into training (10%) and validation (90%)
    // step 6
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
      sqlc
        .read.parquet(commonFriendsPath + "/part_*/")
        .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1), t.getAs[Int](2)))
        .filter(pair => pair.person1 % 11 == 7 || pair.person2 % 11 == 7)
    }

    val testData = {
      prepareData(testCommonFriendsCounts, positives, userToAgeSexBC)
        .map(t => t._1 -> LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
        .filter(t => t._2.label == 0.0)
    }

    // step 8
    val testPrediction = {
      testData
        .flatMap { case (id, LabeledPoint(label, features)) =>
          val prediction = model.predict(features)
          Seq(id._1 -> (id._2, prediction), id._2 -> (id._1, prediction))
        }
        .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
        .groupByKey(numPartitions)
        .map(t => {
          val user = t._1
          val firendsWithRatings = t._2
          val topBestFriends = firendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
          (user, topBestFriends)
        })
        .sortByKey(true, 1)
        .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPrediction.saveAsTextFile(predictionPath,  classOf[GzipCodec])

  }
}
