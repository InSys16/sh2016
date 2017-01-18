/**
  * Created by Artem Gorokhov on 1/15/2017.
  */
case class Features(adamicAdar: Double,
                    commonFriendsCount: Int,
                    fedorScore : Double,
                    //pageRank : Double,
                    groupScores: GroupScores)
object FeatureHelper {

  def sumFeatures(features1: Features, features2: Features) =
  {
    Features(
      features1.adamicAdar + features2.adamicAdar,
      features1.commonFriendsCount + features2.commonFriendsCount,
      features1.fedorScore + features2.fedorScore,
      //features1.pageRank + features2.pageRank,
      GroupDefiner.sumGroupScores(features1.groupScores, features2.groupScores)
      )
  }
}
