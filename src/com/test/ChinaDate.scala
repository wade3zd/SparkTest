package com.test

/**
  * Created by zhangdong on 2016/5/3.
  */
import java.util.{Date, Locale}
import java.text.DateFormat
import java.text.DateFormat._

object ChinaDate {
  def main(args: Array[String]) {
    val now = new Date
    val df = getDateInstance(LONG, Locale.ENGLISH)
    println(df format now)
  }
}

