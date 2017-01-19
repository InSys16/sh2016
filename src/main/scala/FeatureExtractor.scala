import breeze.numerics.abs
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.apache.spark

/**
  * Created by art on 14.01.17.
  */
object FeatureExtractor {
  def isEq(x: Int, y: Int) = {
    if (x == y && x != 0) 1.0 else 0.0
  }
  def isEqL(x: Long, y: Long) = {
    if (x == y && x != 0) 1.0 else 0.0
  }

  def countCosine(a : Int, b : Int, c : Int) = {
    if (a == 0 && b ==0) 0.0 else c / math.sqrt(a * b)
  }

  def countJaccard(a : Int, b : Int, c : Int) = {
    val x = a + b - c
    if (x == 0) 0.0 else c.toDouble / x.toDouble
  }

  def getFeatures(pair: Pair,
                  demographyBC: Broadcast[scala.collection.Map[Int, Demography]],
                  friendsCountBC: Broadcast[scala.collection.Map[Int, Int]],
                  regionsProximityBC: Broadcast[scala.collection.Map[(Int, Int), Int]]) = {
                  //interactionsBC: Broadcast[scala.collection.Map[(Int, Int), Double]]) = {
    val demography = demographyBC.value
    val friendsCount = friendsCountBC.value
    val features = pair.features
    val groupFeatures = pair.features.groupScores

    val firstDemography = demography.getOrElse(pair.uid1, Demography(0, 0, 0, 0, 0, 0))
    val secondDemography = demography.getOrElse(pair.uid2, Demography(0, 0, 0, 0, 0, 0))
    val firstFriendsCount = friendsCount.getOrElse(pair.uid1, 0)
    val secondFriendsCount = friendsCount.getOrElse(pair.uid2, 0)
    val jaccard = countJaccard(firstFriendsCount, secondFriendsCount, features.commonFriendsCount)
    val cosine  = countCosine(firstFriendsCount, secondFriendsCount, features.commonFriendsCount)
    val sameGender = if (firstDemography.gender == secondDemography.gender) 1.0 else 0.0
    val ageDiff = abs(firstDemography.age - secondDemography.age).toDouble
    val ageMean = (firstDemography.age + secondDemography.age) * 0.5

    val orderedPair = if (pair.uid1 < pair.uid2) (pair.uid1, pair.uid2) else (pair.uid2, pair.uid1)
    val regionProximity = regionsProximityBC.value.getOrElse(orderedPair, 0)
    val positionProximity =
      if ((firstDemography.country == secondDemography.country) && (firstDemography.country != 0)) 1.0 else
        if (regionProximity >= 50000) 0.5 else 0.0

    //val interactions = interactionsBC.value.getOrElse((pair.uid1, pair.uid2), 0.0)

    val minFriendsCount = math.min(firstFriendsCount, secondFriendsCount)
    val normalizedCommonFriends = if (minFriendsCount == 0) 0.0 else features.commonFriendsCount.toDouble / minFriendsCount.toDouble
    val regDiff = abs(firstDemography.createDate - secondDemography.createDate).toDouble
    (pair.uid1, pair.uid2) -> Vectors.dense(
      cosine,
      jaccard,
      ageDiff,
      sameGender,
      positionProximity,
      features.adamicAdar,
      features.commonFriendsCount.toDouble,
      features.fedorScore,
      //features.pageRank,
      groupFeatures.commonRelatives.toDouble,
      groupFeatures.commonColleagues.toDouble,
      groupFeatures.commonSchoolmates.toDouble,
      groupFeatures.commonArmyFellows.toDouble,
      groupFeatures.commonFriends.toDouble,

      Math.log(features.commonFriendsCount.toDouble + 1.0),
      Math.log(features.adamicAdar + 1.0),
      Math.log((firstFriendsCount * secondFriendsCount) + 1.0),

      (firstFriendsCount + secondFriendsCount) * 5.0,
      abs(firstFriendsCount * secondFriendsCount),

      normalizedCommonFriends,

      isEqL(firstDemography.country,secondDemography.country),
      isEq(firstDemography.loginRegion,secondDemography.loginRegion),
      regDiff,
      ageMean
      //interactions,
      //Math.log(interactions + 1.0)
    )

  }
}
