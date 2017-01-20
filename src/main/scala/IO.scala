import org.apache.spark.sql.{Row, SQLContext}

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
                   commonFriendsCount : Int,
                   fedorScore : Double) =>
            Pair(uid1, uid2, Features(
              commonFriendsCount,
              fedorScore
            ))}
  }

  def readReversedGraph(sqlc: SQLContext,
                        path : String) = {
    sqlc.read.parquet(path)
      .map(r => {
        val user = r.getAs[Int](0)
        val friends =
          r.getAs[Seq[Row]](1)
            .map { case Row(uid: Int) => GraphFriend(uid) }
            .toArray
        UserFriends(user, friends)
      })
  }
}
