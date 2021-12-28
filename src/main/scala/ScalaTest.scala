object ScalaTest extends App{
  def cube(x: Int) = {
    x * x * x
  }

  val x=cube(5)
  print("result : "+x)
}
