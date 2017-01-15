/**
  * Created by Artem Gorokhov on 1/15/2017.
  */
/*
  1. Love
  2. Spouse
  3. Parent
  4. Child
  5. Brother/Sister
  6. Uncle/Aunt
  7. Relative
  8. Close friend
  9. Colleague
  10. Schoolmate
  11. Nephew
  12. Grandparent
  13. Grandchild
  14. College/University fellow
  15. Army fellow
  16. Parent in law
  17. Child in law
  18. Godparent
  19. Godchild
  20. Playing together

 */
case class GroupScores(commonRelatives: Int,
                       commonColleagues: Int,
                       commonSchoolmates: Int,
                       commonArmyFellows: Int,
                       commonFriends: Int)
object GroupDefiner {
  val Love = 1
  val Spouse = 2
  val Parent = 3
  val Child = 4
  val BrotherOrSister = 5
  val UncleOrAunt = 6
  val Relative = 7
  val CloseFriend = 8
  val Colleague = 9
  val Schoolmate = 10
  val Nephew = 11
  val Grandparent = 12
  val Grandchild = 13
  val CollegeOrUniversityFellow = 14
  val ArmyFellow = 15
  val ParentInLaw = 16
  val ChildInLaw = 17
  val Godparent = 18
  val Godchild = 19
  val PlayingTogether = 20

  val relativesMask = (1 << Love) | (1 << Spouse) | (1 << Parent) | (1 << Child) |
    (1 << BrotherOrSister) | (1 << UncleOrAunt) | (1 << Relative) |
    (1 << Grandparent) | (1 << Grandchild) | (1 << ParentInLaw) |
    (1 << ChildInLaw) | (1 << Godparent) | (1 << Godchild) | (1 << Nephew)

  val colleagueMask = (1 << Colleague) | (1 << CollegeOrUniversityFellow)

  val schoolmateMask = 1 << Schoolmate

  val armyFellowMask = 1 << ArmyFellow

  val friendsMask = (1 << PlayingTogether) | (1 << CloseFriend)

  val masks = Array(relativesMask, colleagueMask, schoolmateMask, armyFellowMask, friendsMask)

  def defineGroupByNumber(number: Int) : Int = {
    val maskClear = number & 0xFFFFFFFE
    for (i <- masks.indices) {
      if ((maskClear & masks(i)) != 0) return i+1
    }
    0
  }

  def getGroupsScoresByCommonFriendGroups(group1 : Int,
                                          group2 : Int): GroupScores =
  {
    val commonRelatives = if (group1 == group2 && group1 == 1) 1 else 0
    val commonColleagues = if (group1 == group2 && group1 == 2) 1 else 0
    val commonSchoolmates = if (group1 == group2 && group1 == 3) 1 else 0
    val commonArmyFellows = if (group1 == group2 && group1 == 4) 1 else 0
    val commonFriends = if (group1 == group2 && group1 == 5) 1 else 0
    GroupScores(commonRelatives, commonColleagues, commonSchoolmates, commonArmyFellows, commonFriends)
  }

  def sumGroupScores(scores1 : GroupScores, scores2 : GroupScores) =
  {
    GroupScores(
      scores1.commonRelatives + scores2.commonRelatives,
      scores1.commonColleagues + scores2.commonColleagues,
      scores1.commonSchoolmates + scores2.commonSchoolmates,
      scores1.commonArmyFellows + scores2.commonArmyFellows,
      scores1.commonFriends + scores2.commonFriends)
  }
}
