/**
  * Created by Artem Gorokhov on 1/15/2017.
  */
case class Features(commonFriendScoreOK: Double,
                    commonFriendsCount: Int,
                    groupScores: GroupScores)
object FeatureHelper {

  def sumFeatures(features1: Features, features2: Features) =
  {
    Features(
      features1.commonFriendScoreOK + features2.commonFriendScoreOK,
      features1.commonFriendsCount + features2.commonFriendsCount,
      GroupDefiner.sumGroupScores(features1.groupScores, features2.groupScores)
      )
  }
}
