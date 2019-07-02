import java.io.{File, FileInputStream}
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.{FileSystems, Path, WatchService}
import java.security.{DigestInputStream, MessageDigest}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object App {
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("File Watcher"))

  val dir = "/home/ashvinsharma/test"
  val path: Path = FileSystems.getDefault.getPath(dir)
  val watchService: WatchService = path.getFileSystem.newWatchService

  def main(args: Array[String]): Unit = {
    path.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY)

    println(s"Listening to any changes in $path...")

    // check if old history exists or make new one

    //start watching
    watch()
  }

  private def watch(): Unit = {
    while (true) {
      val watchKey = watchService.take
      watchKey.pollEvents.asScala.foreach(e => {
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
