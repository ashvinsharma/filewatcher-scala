package com.learningscala.filewatcher

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.net.URI
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.security.MessageDigest
import java.util.zip.GZIPOutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object App {
  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("File Watcher"))
  val home = "/home/ashvinsharma"
  val dir = "/test"
  val hashDir = s"$home/$dir/.hash"
  val path: Path = FileSystems.getDefault.getPath(s"$home/$dir")
  val watchService: WatchService = path.getFileSystem.newWatchService
  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {
    path.register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY)
    println(s"Listening to any changes in $path...")
    //start watching
    watch()
  }

  private def watch(): Unit = {
    while (true) {
      val watchKey = watchService.take
      watchKey.pollEvents.asScala
        .filter(wEvent =>
          !((Files.isDirectory(Paths.get(s"$home/$dir/${wEvent.context}")) || wEvent.kind().equals(ENTRY_DELETE))
            && wEvent.context.toString.equals(".hash")))
        .foreach(e => {
          val file = new File(s"$home/$dir/${e.context}")
          println(s"${e.kind} => ${e.context}")
          if (e.kind().equals(ENTRY_MODIFY)) {
            val hash = createHash(file)
            compress(file, hash)
            saveBackup(hash)
          }
        })
      watchKey.reset
    }
  }

  private def createHash(file: File): String = {
    val sha = MessageDigest.getInstance("SHA-1")
    val hashString = sha.digest(file.getName.getBytes).map("%02x".format(_)).mkString
    println(s"HASH: $hashString")
    hashString
  }

  private def compress(file: File, hash: String): Unit = {
    val bos = new ByteArrayOutputStream()
    val gzip = new GZIPOutputStream(bos)
    val fis = new FileInputStream(file)
    val bytes: Array[Byte] = new Array[Byte](1024)
    var read = fis.read(bytes)
    while (read > 0) {
      gzip.write(bytes)
      read = fis.read(bytes)
    }
    gzip.close()
    val fileCompressedPath = new File(s"$hashDir/$hash.gz")
    fileCompressedPath.getParentFile.mkdirs()
    fileCompressedPath.createNewFile()
    bos.writeTo(Files.newOutputStream(fileCompressedPath.toPath))
    bos.close()
  }

  private def saveBackup(hash: String): Unit = {
    val hdfs = fs.FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration)
    val fileHDFSPath = new fs.Path(s"hash/")
    val fileLocalPath = new fs.Path(s"$hashDir/$hash.gz")
    hdfs.copyFromLocalFile(false, fileLocalPath, fileHDFSPath)
  }

  //
  //  private def createHash(file: File): String = {
  //    val sha = MessageDigest.getInstance("SHA-1")
  //    val digestStream = new DigestInputStream(new FileInputStream(file), sha)
  //    val buffer = new Array[Byte](1024)
  //
  //    digestStream.read(buffer, 0, buffer.length)
  //    val hashString = digestStream.getMessageDigest.digest.map("%02x".format(_)).mkString
  //    println(s"HASH: $hashString")
  //    hashString
  //  }
}
