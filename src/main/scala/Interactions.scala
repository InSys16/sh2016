/**
  * Created by Artem Gorokhov on 1/19/2017.
  */

case class Interactions(sum : Int,
                        log1 : Double,
                        adar : Double)

object Interactions {

  def typeOfInterEvaluator(typeOfInter : Int) =
    typeOfInter match {
      case 1 => 0// удаление фида из ленты
      case 2 => 1//поход в гости
      case 3 => 1//участие в опросе
      case 4 => 1//отправка личного сообщения
      case 5 => 0//удаление личного сообщения
      case 6 => 1//класс объекта
      case 7 => 0//"разкласс" объекта
      case 8 => 1//комментирование пользовательского поста
      case 9 => 1//комментирование пользовательского фото
      case 10 => 1//комментирование пользовательского видео
      case 11 => 1//комментирование фотоальбома
      case 12 => 1//класс к комментарию
      case 13 => 1//отправка сообщения на форуме
      case 14 => 1//оценка фото
      case 15 => 1//просмотр фото
      case 16 => 1//отметка пользователя на фотографиях
      case 17 => 1//отметка пользователя на отдельном фото
      case 18 => 1//отправка подарка
    }

  def calculateInteractions(interactions: Seq[(Int, Double)]) = {
    var sum = 0.0
    for ((typeOfInter, n) <- interactions){
      sum += typeOfInterEvaluator(typeOfInter) * n
    }
    sum
  }
}
