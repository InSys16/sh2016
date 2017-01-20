/**
  * Created by art on 14.01.17.
  */
object FeatureExtractor {
  def getFeatures(pair: Pair) = {
    (pair.uid1, pair.uid2) -> pair.features.fedorScore
  }
}
