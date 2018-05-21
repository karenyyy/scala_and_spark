import Lab4.{cube, gcd, nodesum, wordCount}


object Lab4Testing extends App{

  test()

  def test(): Unit ={

    println(wordCount("retweetsmall.csv"))
    println(cube(1))
    println(cube(2))
    println(cube(3))
    println(cube(4))
    println(gcd(2091, 4807))

    val Leaf_1=Node(null, null, 1)
    val Leaf_2=Node(null, null, 2)
    val Leaf_3=Node(null, null, 3)
    val Leaf_4=Node(null, null, 4)
    val node_1=Node(Leaf_1, Leaf_2, 193)
    val node_2=Node(Leaf_3,Leaf_4, 63)
    val node_3=Node(Leaf_1, Leaf_2, 13)
    val node_4= Node(Leaf_3,Leaf_4, 633)
    val node_5=Node(node_1, node_2, 45)
    val node_6=Node(node_3, node_4, 89)


    val root = Node(node_5, node_6, 1234)

    println(nodesum(root))

    }
  }
