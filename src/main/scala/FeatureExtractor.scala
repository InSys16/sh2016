import breeze.numerics.abs
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.broadcast.Broadcast

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

  def getFeatures(pair: PairWithCommonFriends,
                  userToAgeSexBC: Broadcast[Map[Int, AgeGenderPosition]],
                  userToFriendsCountBC: Broadcast[Map[Int, Int]]) = {
    val firstAGP = userToAgeSexBC.value.getOrElse(pair.person1, AgeGenderPosition(0, 0, 0))
    val secondAGP = userToAgeSexBC.value.getOrElse(pair.person2, AgeGenderPosition(0, 0, 0))
    val firstFriendsCount = userToFriendsCountBC.value.getOrElse(pair.person1, 0)
    val secondFriendsCount = userToFriendsCountBC.value.getOrElse(pair.person2, 0)
    val jaccard = countJaccard(firstFriendsCount, secondFriendsCount, pair.commonFriendsCount)
    val cosine  = countCosine(firstFriendsCount, secondFriendsCount, pair.commonFriendsCount)
    val sameGender = if (firstAGP.gender == secondAGP.gender) 1.0 else 0.0
    val ageDiff = {
      val ageDiff = abs(firstAGP.age - secondAGP.age).toDouble
      //1/scala.math.pow(2,ageDiff)
      ageDiff
    }
    val samePosition = if ((firstAGP.position == secondAGP.position) && (firstAGP.position != 0)) 1.0 else 0.0



    (pair.person1, pair.person2) -> Vectors.dense(
      cosine,
      jaccard,
      ageDiff,
      sameGender,
      samePosition
    )

  }
}
