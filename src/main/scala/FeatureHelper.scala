/**
  * Created by Artem Gorokhov on 1/15/2017.
  */
case class Features(commonFriendsCount: Int,
                    fedorScore : Double)
object FeatureHelper {

  def sumFeatures(features1: Features, features2: Features) =
  {
    Features(features1.commonFriendsCount + features2.commonFriendsCount,
      features1.fedorScore + features2.fedorScore)
  }
}
