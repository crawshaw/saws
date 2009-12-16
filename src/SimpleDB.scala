package com.zentus

import scala.xml.{ Elem, Node }

class Domain(private val conn: AWS, domain: String) {
  def apply(item: String): Map[String,String] = {
    apply(item, List())
  }

  def apply(item: String, attrs: List[String]): Map[String,String] = {
    val ret = conn.request(Map(
      "Action"      -> "GetAttributes",
      "DomainName"  -> domain,
      "ItemName"    -> item
    ) ++ attrs.zipWithIndex.map({case (attr,i) => ("AttributeName."+i, attr)}))

    Map() ++ (ret \\ "Attribute").map(
      attr => ((attr \ "Name").text, (attr \ "Value").text)
    )
  }

  def apply(item: String, attr: String): String = {
    apply(item, List(attr))(attr)
  }

  def get(item: String, attr: String): Option[String] =
    apply(item, List(attr)).get(attr)

  def int(item: String, attr: String): Int =
    SimpleDB.int(get(item, attr).getOrElse(SimpleDB.encode(0))).toInt

  def long(item: String, attr: String): Long =
    SimpleDB.long(get(item, attr).getOrElse(SimpleDB.encode(0))).toLong

  def double(item: String, attr: String): Double =
    SimpleDB.double(get(item, attr).getOrElse(SimpleDB.encode(0)))

  def exists(item: String): Boolean = {
    // TODO: there must be an efficent way to implement this
    apply(item).keys.hasNext
  }

  private def mkAttrs(newAttrs: Map[String,String]): Map[String,String] = {
    Map() ++ newAttrs.keys.toList.zipWithIndex.flatMap({ case (k,i) =>
      List(
        "Attribute."+i+".Name"    -> k,
        "Attribute."+i+".Value"   -> newAttrs(k),
        "Attribute."+i+".Replace" -> "true"
      )
    })
  }

  def ++=(kvs: Iterable[(String, Map[String,String])]): Unit =
    kvs map { case (k,v) => actors.Futures.future (+= (k,v)) } foreach { _() }

  def +=(item: String, attrs: Map[String,String]): Unit = {
    conn.request(Map(
      "Action"      -> "PutAttributes",
      "DomainName"  -> domain,
      "ItemName"    -> item
    ) ++ mkAttrs(attrs))
  }

  def -=(item: String, attrs: Map[String,String]): Unit = {
    conn.request(Map(
      "Action"      -> "DeleteAttributes",
      "DomainName"  -> domain,
      "ItemName"    -> item
    ) ++ mkAttrs(attrs))
  }

  def -=(item: String, attrs: List[String]): Unit = {
    conn.request(Map(
      "Action"      -> "DeleteAttributes",
      "DomainName"  -> domain,
      "ItemName"    -> item
    ) ++ attrs.zipWithIndex.map({ case (k,i) => ("Attribute."+i+".Name", k) }))
  }
}

object SimpleDB {
  val maxNegValue = 100000

  def double(x: String): Double =
    x.toDouble - maxNegValue

  def int(x: String): Int =
    double(x).toInt

  def int(x: Option[String]): Int =
    int(x.getOrElse(SimpleDB.zero))

  def long(x: String): Long =
    double(x).toLong

  def long(x: Option[String]): Long =
    long(x.getOrElse(SimpleDB.zero))

  def encode(x: Double): String =
    "%018f".format(x + maxNegValue)

  def encode(x: Long): String =
    "%018d".format(x + maxNegValue)

  def now(): String =
    encode(new java.util.Date().getTime)

  def zero: String =
    encode(0)
}

class SimpleDB(private val awsKeyId: String, private val awsSecretKey: String) {
  private def conn =
    new AWS("sdb.amazonaws.com", "2009-04-15", awsKeyId, awsSecretKey)

  def get(domain: String): Domain =
    new Domain(conn, domain)

  def apply(domain: String): Domain =
    get(domain)

  def +=(domainName: String): Unit =
    conn.request(Map("Action" -> "CreateDomain", "DomainName" -> domainName))

  def -=(x: String): Unit =
    error("To delete a domain, use deleteDomain()")

  /* Delete domain, amazingly dangerous, so it is not called -=. */
  def deleteDomain(domainName: String): Unit =
    conn.request(Map("Action" -> "DeleteDomain", "DomainName" -> domainName))

  def size: Int =
    (conn.request(Map("Action" -> "ListDomains")) \\ "DomainName").size

  def domains: Seq[String] =
    (conn.request(Map("Action" -> "ListDomains")) \\ "DomainName").map(_.text)

  private def selAccum[T](query: String, f: (xml.Elem => Seq[T]))
      : Iterable[T] = {
    def sel(query: String, nextToken: Option[String]) = {
      conn.request(Map("Action" -> "Select", "SelectExpression" -> query)
        ++ nextToken.toList.map(tok => ("NextToken", tok))
      )
    }
    new Iterable[T] { def elements() = new Iterator[T]() {
      val ret = new collection.mutable.Queue[T]()
      var res = sel(query, None)
      var nextToken = res \\ "NextToken"
      ret ++= f(res)
      override def hasNext =
        !ret.isEmpty || !nextToken.isEmpty
      override def next = {
        if (ret.isEmpty) {
          if (nextToken.isEmpty) {
            error("Iterator is done.")
          }
          res = sel(query, Some(nextToken.text))
          nextToken = res \\ "NextToken"
          ret ++= f(res)
        }
        ret.dequeue
      }
    }}
  }

  /* Returns the select results as a lazy iterator. */
  def selectIterable(query: String): Iterable[(String,Map[String,String])] = {
    def mkAttr(attr: Node) =
      ((attr \ "Name").text, (attr \ "Value").text)
    def mkItem(item: Node) =
      ((item \ "Name").text, Map() ++ (item \ "Attribute").map(mkAttr))

    selAccum(query, res => (res \\ "Item").map(mkItem))
  }

  /* Accumulate all select results. */
  def select(query: String): Map[String,Map[String,String]] =
    Map() ++ selectIterable(query)

  /* Returns the single attribute of the single item selected by the query. */
  def selectOption(query: String): Option[String] = {
    selectIterable(query).flatMap(
      { case (k,v) => v.values.collect }
    ).toStream.firstOption
  }

  def selectNames(query: String): Iterable[String] = {
    selAccum(query, res => (res \\ "Item" \ "Name").map(_.text))
  }

  def selectName(query: String): Option[String] =
    selectNames(query).toSeq.firstOption

  def selectCount(query: String): Int = {
    selAccum(query, xml => {
      assert((xml \\ "Item" \ "Name").text == "Domain")
      assert((xml \\ "Item" \ "Attribute" \ "Name").text == "Count")
      Seq((xml \\ "Item" \ "Attribute" \ "Value").text.toInt)
    }).elements.next
  }
}

