package pandr

/**
  * Created by bjoern on 27/03/17.
  */
class ArgumentParser(usage: String, argumentsMap: Map[String, Any]) {

  private def isArgument(name: String): Boolean = {
    if (name.substring(0, 2) != "--") {
      throw new IllegalArgumentException(s"Illegal argument: $name . All arguments need to start with --")
    }
    argumentsMap.contains(name.substring(2))
  }

  private def castArgumentValue(argumentName: String, value: String) = {
    argumentsMap.get(argumentName.substring(2)) match {
      case None => throw new IllegalArgumentException(s"Unknown argument: $argumentName. Usage: $usage")
      case Some("String") => value
      case Some("Int") => value.toInt
      case Some("Boolean") => value.toBoolean
      case Some("Float") => value.toFloat
      case Some(unknown) => throw new IllegalArgumentException(s"Unknown argument type: $unknown. ")
    }
  }

  def parseArguments(args: Array[String]): Map[String, Any] = {
    if (args.length == 0) throw new IllegalArgumentException(usage)

    def nextOption(map: Map[String, Any], list: List[String]): Map[String, Any] = {

      list match {
        case Nil => map
        case head :: value :: tail =>
          if(isArgument(head)) {
            nextOption(map ++ Map(head.substring(2) -> castArgumentValue(head,value)), tail)
          } else {
            throw new IllegalArgumentException(s"Unknown argument: $head. Usage: $usage")
          }
        case option :: _ => throw new IllegalArgumentException(s"Unknown parameter: $option")
      }
    }

    nextOption(Map(), args.toList)
  }

}
