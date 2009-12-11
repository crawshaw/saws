package com.zentus

class AWS(
    domain: String,
    version: String,
    awsKeyId: String,
    awsSecretKey: String) {
  import java.net.{ URL, URLEncoder, HttpURLConnection }
  import java.text.SimpleDateFormat
  import java.util.{ Calendar, TimeZone }
  import javax.crypto.Mac
  import javax.crypto.spec.SecretKeySpec

  private val encoding = "HmacSHA256"

  // RFC2104
  private def calcHMAC(data: String): String = {
    val key = new SecretKeySpec(awsSecretKey.getBytes, encoding)
    val mac = Mac.getInstance(encoding)
    mac.init(key)
    val rawHmac = mac.doFinal(data.getBytes)
    new String(Base64.encode(rawHmac))
  }

  private def timestamp(): String = {
    val cal = Calendar.getInstance(); 
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf.format(cal.getTime())
  }

  private def urlEnc(src: String): String =
    if (src == null) "" else
    URLEncoder.encode(src).replace("+", "%20").replace("*", "%2A")

  private def url(path: String, attrs: Map[String,String]): String = {
    val fullAttrs = attrs ++ Map(
      "AWSAccessKeyId"    -> awsKeyId,
      "SignatureVersion"  -> "2",
      "SignatureMethod"   -> encoding,
      "Timestamp"         -> timestamp,
      "Version"           -> version
    )
    val keys = fullAttrs.keys.toList.sort((e1,e2) => (e1 compareTo e2) < 0)
    val attrStr = keys.map(k =>
      urlEnc(k) + "=" + urlEnc(fullAttrs(k))
    ).mkString("&")
    val toSign = "GET\n" + domain + "\n" + path + "\n" + attrStr
    val sig = urlEnc(calcHMAC(toSign))

    "http://" + domain + path + "?" + attrStr + "&Signature=" + sig
  }

  private def formatUrl(url: String): String = {
    url.replaceAll("\\?", "\n  ?").replaceAll("&","\n  &")
  }

  def request(path: String, attrs: Map[String,String]): xml.Elem = {
    import scala.xml._
    var reqUrl = url(path, attrs)
    Console.println(formatUrl(reqUrl))
    val conn = new URL(reqUrl).openConnection.asInstanceOf[HttpURLConnection]
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

  def request(attrs: Map[String,String]): xml.Elem =
    request("/", attrs)
}
