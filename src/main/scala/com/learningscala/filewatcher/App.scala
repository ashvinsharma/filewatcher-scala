package com.learningscala.filewatcher

import java.io.{File, FileInputStream}
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.security.{DigestInputStream, MessageDigest}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.JsonParseException
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object App {
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("File Watcher"))
  val dir = "/home/ashvinsharma/test"
  val historyFile = s"$dir/history.json"
  val path: Path = FileSystems.getDefault.getPath(dir)
  val watchService: WatchService = path.getFileSystem.newWatchService
  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)


  def main(args: Array[String]): Unit = {
    path.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY)


    // check if file exists
    if (Files.exists(Paths.get(historyFile))) {
      try {
        // if file exists, try to read the contents of the file
        mapper.readTree(new File(s"$dir/history.json"))
      } catch {
        // if you can not read the contents of the file, save the existing file with some other name
        // and create a new history.json file
        case _: JsonParseException =>
          println("File can not be read")
          Files.copy(Paths.get(historyFile),
            Paths.get(s"$dir/history${System.currentTimeMillis()}.json"),
            StandardCopyOption.COPY_ATTRIBUTES)
          // create file
          createHistory()
        // create hash of all the files and output them in historyFile
      }
    } else {
      // create file
      println()
      createHistory()
    }
    println(s"Listening to any changes in $path...")
    //start watching
    watch()
  }

  private def createHistory(): Unit = {
    Files.createFile(Paths.get(historyFile))
    Files.walk(Paths.get(dir)).iterator.asScala
      .filter(Files.isRegularFile(_))
      .filter(!_.toString.equals(historyFile))
      .foreach(println(_))
  }

  private def watch(): Unit = {
    while (true) {
      val watchKey = watchService.take
      watchKey.pollEvents.asScala
        .filter(!_.toString.equals(historyFile))
        .foreach(e => {
          println(s"${e.kind} => ${e.context}")
          createHash(new File(s"$dir/${e.context}"))
        })
      watchKey.reset
    }
  }


  private def createHash(file: File): String = {
    val sha = MessageDigest.getInstance("SHA-1")
    val digestStream = new DigestInputStream(new FileInputStream(file), sha)
    val buffer = new Array[Byte](1024)

    digestStream.read(buffer, 0, buffer.length)
    val hashString = digestStream.getMessageDigest.digest.map("%02x".format(_)).mkString
    println(s"HASH: $hashString")
    hashString
  }
}
