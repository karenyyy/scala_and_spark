class ListNode(var value:Int, var next:ListNode=null) {
  def +(that:ListNode):ListNode={
    if (that==null) this
    else {
      val sum=this.value+that.value
      val carry=sum/10
      var next=new ListNode(carry)+this.next+that.next
      if (next.value == null && next.next == null){
        next=null
      }
      new ListNode(sum%10, next)
    }
  }
}

object addTwoNumbers {
  def addTwoNumbers(l1: ListNode, l2: ListNode): ListNode = {
    if (l1 != null && l2 != null) {
      l1 + l2
    } else if (l1 == null) {
      l1
    } else {
      l2
    }

  }


  def createLinkedList(nums: Array[Int]): ListNode = {
    nums.map(i => new ListNode(i))
      .foldLeft[ListNode](null)((l: ListNode, r: ListNode) => {
      l.next = r; l
    })
  }


  def printList(cur: ListNode): Unit = {
    var curr = cur
    if (curr == null) {
      println("empty")
    } else {
      println(curr.value)
      curr = curr.next
    }
  }

  def main(args: Array[String]): Unit = {
    var n1 = createLinkedList(Array(0))
    var n2 = createLinkedList(Array(1))

    printList(addTwoNumbers(n1, n2))
  }
}

