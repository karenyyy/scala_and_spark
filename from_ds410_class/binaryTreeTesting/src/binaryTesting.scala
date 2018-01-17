
import scala.collection.mutable.{Queue, Stack}
/**
  * Created by karen on 17-9-28.
  */
object binaryTesting {
  def main(args: Array[String]): Unit = {
    var tree = BinarySearchTree[Int](null)
    tree.insert(5); tree.insert(1); tree.insert(6);
    tree.insert(0); tree.insert(3); tree.insert(2); tree.insert(4);
    tree.insert(8); tree.insert(7); tree.insert(9);


    //Some tests//

    println("Example tree:     ")
    println("        5         ")
    println("      /   \\      ")
    println("     1     6      ")
    println("    / \\     \\   ")
    println("   0   3     8    ")
    println("      / \\   / \\ ")
    println("     2   4 7   9  ")



    print("\nIn order traversal: 0123456789 = ")
    tree.inOrderApply(print)

    print("\nPre order traversal: 5103246879 = ")
    tree.preOrderApply(print)

    print("\nPost order traversal: 0243179865 = ")
    tree.postOrderApply(print)

    print("\nLevel order traversal: 5160382479 = ")
    tree.levelOrderApply(print)

    println
  }
}

case class Node[T <% Ordered[T]](var data: T, var left: Node[T], var right: Node[T]){
  override def toString = data.toString
}

case class BinarySearchTree[T <% Ordered[T]](var root: Node[T]) {
  def search(data: T): Node[T] = {
    var node = root
    while(node != null){
      val res = data.compare(node.data)
      if (res == 0)
        return node
      if (res < 0) node = node.left else node = node.right
    }
    return null
  }
  def insert(data: T){
    if (root == null) { root = Node[T](data, null, null); return }
    var node = root
    while(node != null){
      val res = data.compare(node.data)
      if (res == 0) return

      if (res < 0)
        if (node.left == null) { node.left = Node[T](data, null, null); return }
        else node = node.left
      else
      if (node.right == null) { node.right = Node[T](data, null, null); return }
      else node = node.right
    }
  }
  def remove(data: T): Boolean = {
    val newroot = new Node[T](root.data,root,null)
    val res = remove(root, newroot, data)
    root = newroot.left
    return res
  }



  private def remove(node: Node[T], parent: Node[T], data: T): Boolean = {
    if (node == null) return false

    val res = data.compare(node.data)

    if (res < 0) return remove(node.left, node, data)
    else if (res > 0) return remove(node.right, node, data)
    else {
      if (node.left!=null && node.right!=null){
        node.data = findMin(node.right)
        remove(node.right, node, node.data)
      }
      else if (node == parent.left)
        parent.left = if (node.left != null) node.left else node.right
      else if (node == parent.right)
        parent.right = if (node.left != null) node.left else node.right

      return true
    }
  }
  private def findMin(node: Node[T]): T = {
    var n = node
    while (n.left!=null) n = n	.left
    return node.data
  }
  private def findMax(node: Node[T]): T = {
    var n = node
    while (n.right!=null) n = n.right
    return node.data
  }

  def inOrderApply(apply: (Node[T]) => Unit) { inOrderApply(apply, false) }
  def inOrderApplyDesc(apply: (Node[T]) => Unit) { inOrderApply(apply, true) }
  private def inOrderApply(apply: (Node[T]) => Unit, isDesc: Boolean ){
    val s = new Stack[Node[T]]
    var node = root
    while(s.nonEmpty || node!=null){
      if (node!=null) {
        s.push(node)
        node = if (isDesc) node.right else node.left
      }else {
        node = s.pop
        apply(node)
        node = if (isDesc) node.left else node.right
      }
    }
  }
  def inOrderApplyRec(apply: (Node[T]) => Unit ){ inOrderApply(root, apply) }
  private def inOrderApply(node: Node[T], apply: (Node[T]) => Unit){
    if (node.left!=null) inOrderApply(node.left, apply)
    apply(node)
    if (node.right!=null) inOrderApply(node.right, apply)
  }

  def preOrderApply(apply: (Node[T]) => Unit) {
    val s = new Stack[Node[T]]
    s.push(root)
    while (s.nonEmpty){
      var node = s.pop
      apply(node)
      if (node.right != null) s.push(node.right)
      if (node.left != null) s.push(node.left)
    }
  }

  def postOrderApply(apply: (Node[T]) => Unit) {
    val s = new Stack[Node[T]]
    s.push(root); s.push(root)
    while (s.nonEmpty){
      var node = s.pop
      if (s.nonEmpty && node.data.compare(s.top.data)==0){
        if (node.right!=null) { s.push(node.right); s.push(node.right) }
        if (node.left!=null) { s.push(node.left); s.push(node.left) }
      }
      else apply(node)
    }
  }

  //BFS
  def levelOrderApply(apply:(Node[T]) => Unit){
    var s = new Queue[Node[T]]
    s.enqueue(root)
    while(s.nonEmpty){
      val node = s.dequeue
      apply(node)
      if(node.left!=null) s.enqueue(node.left)
      if(node.right!=null) s.enqueue(node.right)
    }
  }


}

