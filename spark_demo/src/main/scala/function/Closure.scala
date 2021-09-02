package function

import org.junit.Test

class Closure {

  /**
   * 编写一个高级函数，在函数中有一个变量，返回一个函数，通过变量完成计算
   */
  @Test
  def test(): Unit ={
//    val f=closu()
//    val area=f(2)
//    println(area)
    /**
     * 在这是否能访问到closu中的pai，不能
     * 说明pai在一个单独的作用域中
     *
     * 在拿到f的时候，可以通过f间接的访问到closu作用域中的内容
     * 说明f携带了一个作用域
     * 如果一个函数携带了一个外包的作用域，
     * 这种函数我们称之为闭包
     */
    val f=closu()
    f(5)

    /**
     * 闭包的本质是什么？
     * f就是闭包，闭包的本质就是一个函数
     * 在scala中函数是一个特殊的类型，Function（X）
     * 闭包也是一个Function（X）类型的对象
     * 闭包是一个对象
     */
  }

  /**
   * 返回一个新的函数
   */
  def closu(): Int =>Double ={

    val pai=3.14
    val yuan=(r:Int)=>math.pow(r,2)*pai
    yuan
  }
}
