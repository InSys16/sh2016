import breeze.numerics.abs
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.broadcast.Broadcast
import scala.collection.Map

/**
  * Created by art on 14.01.17.
  */
object FeatureExtractor {

  def countCosine(a : Int, b : Int, c : Int) = {
    if (a == 0 && b ==0) 0.0 else c / math.sqrt(a * b)
  }

  def countJaccard(a : Int, b : Int, c : Int) = {
    val x = a + b - c
    if (x == 0) 0.0 else c.toDouble / x.toDouble
  }

  def getFeatures(pair: Pair,
                  demographyBC: Broadcast[Map[Int, Demography]],
                  friendsCountBC: Broadcast[Map[Int, Int]]) = {
    val demography = demographyBC.value
    val friendsCount = friendsCountBC.value
    val features = pair.features
    val groupFeatures = pair.features.groupScores

    val firstDemography = demography.getOrElse(pair.uid1, Demography(0, 0, 0))
    val secondDemography = demography.getOrElse(pair.uid2, Demography(0, 0, 0))
    val firstFriendsCount = friendsCount.getOrElse(pair.uid1, 0)
    val secondFriendsCount = friendsCount.getOrElse(pair.uid2, 0)
    val jaccard = countJaccard(firstFriendsCount, secondFriendsCount, features.commonFriendsCount)
    val cosine  = countCosine(firstFriendsCount, secondFriendsCount, features.commonFriendsCount)
    val sameGender = if (firstDemography.gender == secondDemography.gender) 1.0 else 0.0
    val ageDiff = {
      val ageDiff = abs(firstDemography.age - secondDemography.age).toDouble
      //1/scala.math.pow(2,ageDiff)
      ageDiff
    }
    val samePosition = if ((firstDemography.position == secondDemography.position) && (firstDemography.position != 0)) 1.0 else 0.0

    (pair.uid1, pair.uid2) -> Vectors.dense(
      cosine,
      jaccard,
      ageDiff,
      sameGender,
      samePosition,
      features.commonFriendScoreOK,
      groupFeatures.commonRelatives.toDouble,
      groupFeatures.commonColleagues.toDouble,
      groupFeatures.commonSchoolmates.toDouble,
      groupFeatures.commonArmyFellows.toDouble,
      groupFeatures.commonFriends.toDouble)

  }
}
