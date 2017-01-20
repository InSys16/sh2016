import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

case class GraphFriend(uid: Int)//, interactionScore: Double = 0f)
case class UserFriends(uid: Int, friends: Array[GraphFriend])

case class Pair(uid1: Int,
                uid2: Int,
                features: Features)

object Baseline {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Baseline")
      .set("spark.driver.maxResultSize", "10g")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._

    val dataDir = if (args.length == 1) args(0) else "./"

    val graphPath = dataDir + "trainGraph"
    val reversedGraphPath = dataDir + "reversedGraph"
    val pairsPath = dataDir + "pairs"
    val predictionPath = dataDir + "Prediction"
    val numGraphParts = 200
    val numPairsParts = 107


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
              .map(t => GraphFriend(t.split(",")(0).toInt))
          }
          UserFriends(user, friends)
        })
    }

    val coreUsers = graph.map(user => user.uid)
    val coreUsersBC = sc.broadcast(coreUsers.collect().toSet)

    graph
      .filter(user => user.friends.length >= 8 && user.friends.length <= 1000)
      .flatMap(user => user.friends.map(x => (x.uid, GraphFriend(user.uid))))
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
                      friendsCount: Broadcast[Map[Int, Int]]) = {
      val pairs = ArrayBuffer.empty[((Int, Int), Features)]

      val commonFriendFriendsCount = friendsCount.value.getOrElse(userFriends.uid, 0)
      val fedorScore = 100.0 / Math.pow(commonFriendFriendsCount.toDouble + 5, 1.0/3.0) - 8

      for (i <- userFriends.friends.indices) {
        val user1 = userFriends.friends(i)
        if (user1.uid % numPairsParts == numOfPart) {
          for (j <- i + 1 until userFriends.friends.length) {
            val user2 = userFriends.friends(j)
            val features = Features(
              1,
              fedorScore)
            pairs.append(((user1.uid, user2.uid), features))
          }
        }
      }
      pairs
    }

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
        pair.features.commonFriendsCount,
        pair.features.fedorScore)})
        .toDF.repartition(4).write.parquet(pairsPath + "/part_" + part)
    }
    /// (coreUser1,  coreUser2) -> 1.0  :  coreUser1 < coreUser2
    /// I.e. realFriends
    val positives = graph.flatMap(
          userFriends => userFriends.friends
            //take friends that in coreUsersSet and > user
            .filter(x => coreUsersBC.value.contains(x.uid) && userFriends.uid < x.uid)
            .map(x => (userFriends.uid, x.uid) -> 1.0)
        )

    def prepareData( pairs: RDD[Pair], positives: RDD[((Int, Int), Double)]) = {
      pairs
        .map(pair => FeatureExtractor.getFeatures(pair))
        .leftOuterJoin(positives)
    }
    val pairsForPrediction = {
      IO.readPairs(sqlc, pairsPath + "/part_*/")
        .filter(pair => pair.uid1 % 11 == 7 || pair.uid2 % 11 == 7)
    }

    var minFedor = 4000000.0

    prepareData(pairsForPrediction, positives)
      .map(pair => pair._1 -> (pair._2._2.getOrElse(0.0), pair._2._1))
      .flatMap { case (pair, (label, features)) =>
        val prediction = features
        if (label == 1.0) {
          minFedor = Math.min(minFedor, prediction)
          Seq.empty[(Int, (Int, Double))]
        }
        else
          Seq(pair._1 -> (pair._2, prediction), pair._2 -> (pair._1, prediction))
      }
      .filter(t => t._1 % 11 == 7 && t._2._2 >= minFedor)
      .groupByKey(numGraphParts)

      .map(t => {
        val user = t._1
        val friendsWithRatings = t._2.toList
        val topBestFriends = friendsWithRatings.sortBy(x => x._2).take(100).map(x => x._1)
        (user, topBestFriends)
      })
      .sortByKey(true, 1)
      .map(t => t._1 + "\t" + t._2.mkString("\t"))

      .saveAsTextFile(predictionPath, classOf[GzipCodec])
  }
}

