package org.ddos.detection.kafka

object test extends App {
  
  val line ="118.194.191.224 - - [25/May/2015:23:11:54 +0000] 'GET / HTTP/1.0' 200 3557 '-' 'Opera/9.00 (Windows NT 5.1; U; en)'"
  
  val ip = line.split("- -")(0)
  val ts = line.split("- -")(1).split(" ")(0)
  println(ts)

}