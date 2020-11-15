package com.kdg.bigdata

import java.nio.file.{Files, Paths}

object HelloWorld {
  /* 这是我的第一个 Scala 程序
   * 以下程序将输出'Hello World!'
   */
  def main(args: Array[String]): Unit = {
    System.err.println(1)
    Files.copy(Paths.get(""), Paths.get(""))
    println("Hello, world!") // 输出 Hello World
    println(test(1, 2))
    if (test(1, 2) == 3) {
      Array(1, 2, 3, 4, 5).filter(_.!=(1)).foreach(println(_))
    }
    val dto = new Dto(1, 2)
    println(dto)

  }

  def test(x: Int, y: Int): Int = {
    x + y
  }

  class Point(xc: Int, yc: Int) {
    var x: Int = xc
    var y: Int = yc

    def move(dx: Int, dy: Int) {
      x = x + dx
      y = y + dy
      println("x 的坐标点: " + x);
      println("y 的坐标点: " + y);
    }
  }

  class Dto(x1: Int, y1: Int) {
    var x: Int = x1
    var y: Int = y1

    override def toString: String = "x=" + x + ",y=" + y
  }

}