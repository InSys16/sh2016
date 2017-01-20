import breeze.numerics.abs
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import org.apache.spark

/**
  * Created by art on 14.01.17.
  */
object FeatureExtractor {
  def getFeatures(pair: Pair) = {
    (pair.uid1, pair.uid2) -> pair.features.fedorScore
  }
}
