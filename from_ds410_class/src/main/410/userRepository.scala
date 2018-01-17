case class User(id:Int, firstName:String, lastName:String, age:Int, gender:Option[String])

/**
  * Created by karen on 17-9-26.
  */
object userRepository {
  private val users=Map(1->User(1, "John", "S", 19, Some("male")),
                        2->User(2, "Johnna", "S", 19, Some("female")))

  def findById(id:Int):Option[User]=users.get(id)
  def findAll=users.values
  def main(args: Array[String]): Unit = {
    val user1 = userRepository.findById(1)
      .map(_.firstName.toUpperCase)
      .foreach(println(_))
    val first = userRepository.findById(1).get.firstName.toUpperCase()
    val last = userRepository.findById(1).get.lastName.toUpperCase()
    println(s"$first, $last")

    for {
      user <- userRepository.findById(1)
      gender <- user.gender
    } yield gender // results in Some("male")

    for {
      User(_, _, _, _, Some(gender)) <- userRepository.findAll
    } yield gender
  }


}



