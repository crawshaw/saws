package com.zentus

object HTTP {
  def date(date: java.util.Date): String = {
    val sdf = new java.text.SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss '+0000'")
    sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    sdf.format(date)
  }

  def now(): String =
    date(new java.util.Date())
}

class Item(name: String, lastModified: String, size: Int) {
  override def toString() = "Item("+name+")"
}

class Bucket(private val s3: S3, name: String) extends Iterable[Item] {
  override def elements(): Iterator[Item] = {
    def genItem(contents: xml.Node): Item = {
      new Item(
        (contents \\ "Key").text,
        (contents \\ "LastModified").text,
        (contents \\ "Size").text.toInt
      )
    }

    def sel(marker: Option[String]) = {
      val qs = marker match {
        case None       => ""
        case Some(mkr)  => "?marker="+mkr
      }
      val conn = s3.getconn("GET", "", name, "/", qs)
      s3.getxml(conn)
    }

    new Iterator[Item]() {
      val ret = new collection.mutable.Queue[Item]()
      var res = sel(None)
      def genMarker() = (res \\ "Key").lastOption.map(_.text)
      def genItems()  = ((res \\ "Contents") map genItem)
      var marker = genMarker()
      ret ++= genItems()
      override def hasNext = {
        if (!ret.isEmpty) {
          true
        } else if (marker.isEmpty) {
          false
        } else {
          res = sel(marker)
          marker = genMarker()
          ret ++= genItems()
          !ret.isEmpty
        }
      }
      override def next = {
        if (!hasNext) {
          error("Iterator is done.")
        }
        ret.dequeue
      }
    }
  }
}

class S3(awsKeyId: String, awsSecretKey: String) extends Iterable[Bucket] {
  import java.net._
  import java.io.{ BufferedReader, InputStreamReader, OutputStreamWriter }
  import java.util.{ Date, Calendar, TimeZone }
  import javax.crypto.Mac
  import javax.crypto.spec.SecretKeySpec
  import xml._

  private val encoding = "HmacSHA1"

  // RFC2104
  private def calcHMAC(data: String): String = {
    val key = new SecretKeySpec(awsSecretKey.getBytes, encoding)
    val mac = Mac.getInstance(encoding)
    mac.init(key)
    val rawHmac = mac.doFinal(data.getBytes)
    new String(Base64.encode(rawHmac))
  }

  private def authorization(
      verb: String, contentMD5: String, contentType: String, date: String,
      bucket: String, resource: String) = {
    val toSign = (
      verb        + "\n" +
      contentType + "\n" +
      contentMD5  + "\n" +
      date        + "\n" +
      "/" + bucket + resource
    )
    Console.println("signing: "+toSign)
    "AWS " + awsKeyId + ":" + calcHMAC(toSign)
  }

  def getconn(
      verb: String, contentType: String,
      bucket: String, resource: String, querystring: String) = {
    val date = HTTP.now
    val url = "http://" + bucket + ".s3.amazonaws.com" + resource + querystring
    val conn = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    val auth = authorization(verb, "", contentType, date, bucket, resource)
    conn.setRequestMethod(verb)
    conn.setRequestProperty("Date", date)
    conn.setRequestProperty("Authorization", auth)
    conn
  }

  def getxml(conn: HttpURLConnection) = {
    try {
      val xml = XML.load(conn.getInputStream)
      Console.println(new PrettyPrinter(80, 2).format(xml))
      xml
    } catch {
      case e =>
        Console.println("exception: "+ conn.getResponseMessage)
        Console.println(
          scala.io.Source.fromInputStream(
            conn.getErrorStream).getLines.mkString("\n")
        )
        throw e
    }
  }

  def +=(name: String) = {
    val conn = getconn("PUT", "", name.toLowerCase, "/", "")
    conn.setRequestProperty("Content-Length", "0")
    if (conn.getResponseCode != 200)
      error("Error creating bucket '"+name+"': "+conn.getResponseMessage)
  }

  def apply(name: String) = new Bucket(this, name)

  override def elements(): Iterator[Bucket] = {
    val date = HTTP.now

    val reqUrl = "http://s3.amazonaws.com"
    val conn = new URL(reqUrl).openConnection.asInstanceOf[HttpURLConnection]
    val auth = authorization("GET", "", "", date, "", "")
    conn.setRequestProperty("Date", date)
    conn.setRequestProperty("Authorization", auth)

    try {
      val xml = XML.load(conn.getInputStream)
      //Console.println(new PrettyPrinter(80, 2).format(xml))
      (xml \\ "Name") map { n => new Bucket(this, n.text) } elements
    } catch {
      case e =>
        Console.println("exception: "+ conn.getResponseMessage)
        Console.println(
          scala.io.Source.fromInputStream(
            conn.getErrorStream).getLines.mkString("\n")
        )
        throw e
    }
  }
}

