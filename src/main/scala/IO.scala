import breeze.numerics.abs
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Artem Gorokhov on 1/15/2017.
  */
object IO {

  def readPairs(sqlc: SQLContext,
                path : String) =
  {
    sqlc.read.parquet(path)
        .map{
          case Row(uid1: Int,
                   uid2: Int,
                   commonFriendScoreOK : Double,
                   commonFriendsCount : Int,
                   commonRelatives: Int,
                   commonColleagues: Int,
                   commonSchoolmates: Int,
                   commonArmyFellows: Int,
                   commonFriends: Int ) =>
            Pair(uid1, uid2, Features(
              commonFriendScoreOK,
              commonFriendsCount,
              GroupScores(commonRelatives,
                          commonColleagues,
                          commonSchoolmates,
                          commonArmyFellows,
                          commonFriends)
            ))}
  }

  def readReversedGraph(sqlc: SQLContext,
                        path : String) = {
    sqlc.read.parquet(path)
      .map(r => {
        val user = r.getAs[Int](0)
        val friends =
          r.getAs[Seq[Row]](1)
            .map { case Row(uid: Int, mask: Int) => GraphFriend(uid, mask) }
            .toArray
        UserFriends(user, friends)
      })
  }
}
