/*
 * Copyright (c) 2010, David Crawshaw <david@zentus.com>
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.

 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package com.zentus.sqs

import com.zentus.AWS

case class Message(
  val id: String,
  val body: String,
  val md5: String,
  val receiptHandle: String,
  val atts: Map[String,String]
)

object Message {
  import xml.Node
  def apply(xml: Node): Message = {
    Message(
      (xml \\ "MessageId").text,
      (xml \\ "Body").text,
      (xml \\ "MD5OfBody").text,
      (xml \\ "ReceiptHandle").text,
      Map() ++
        (xml \\ "Attribute").map(a => ((a \\ "Name").text, (a \\ "Value").text))
    )
  }
}

class Queue(private val conn: AWS, val path: String) {
  def +=(message: String): Unit = {
    conn.request(path, Map(
      "Action" -> "SendMessage",
      "MessageBody" -> message
    ))
  }

  def -=(message: Message): Unit = {
    conn.request(path, Map(
      "Action" -> "DeleteMessage",
      "ReceiptHandle" -> message.receiptHandle
    ))
  }

  def get(): Option[Message] = {
    val response = conn.request(path, Map(
      "Action" -> "ReceiveMessage",
      "AttributeName" -> "All"
    ))
    (response \\ "Message") map (Message(_)) firstOption
  }

  def size(): Int = {
    (conn.request(path, Map(
      "Action" -> "GetQueueAttributes",
      "AttributeName" -> "ApproximateNumberOfMessages"
    )) \\ "Value").text.toInt
  }

  def timeout(): Int = {
    (conn.request(path, Map(
      "Action" -> "GetQueueAttributes",
      "AttributeName" -> "VisibilityTimeout"
    )) \\ "Value").text.toInt
  }

  def timeout(timeout: Int): Unit = {
    conn.request(path, Map(
      "Action"          -> "SetQueueAttributes",
      "Attribute.Name"  -> "VisibilityTimeout",
      "Attribute.Value" -> timeout.toString
    ))
  }

  type AWSAccountID = String
  abstract sealed trait ActionName
  case object SendMessage              extends ActionName
  case object ReceiveMessage           extends ActionName
  case object DeleteMessage            extends ActionName
  case object ChangeMessageVisibility  extends ActionName
  case object GetQueueAttributes       extends ActionName
  case object All                      extends ActionName

  def addPermission(label:String, perms:Iterable[(AWSAccountID,ActionName)]) = {
    val atts = perms.toList.zipWithIndex.flatMap({ case ((id,name),i) =>
      List(
        "AWSAccountID."+i -> id,
        "ActionName."+i   -> name.toString.replace("All","*")
      )
    })
    conn.request(path, Map(
      "Action"  -> "AddPermission",
      "Label"   -> label
    ) ++ atts)
  }

  def removePermission(label: String) = {
    conn.request(path, Map(
      "Action"  -> "RemovePermission",
      "Label"   -> label
    ))
  }
}

class SQS(private val awsKeyId: String, private val awsSecretKey: String) {
  private def conn =
    new AWS("queue.amazonaws.com", "2009-02-01", awsKeyId, awsSecretKey)

  def apply(queueName: String): Queue = {
    val queues = (conn.request(Map(
      "Action"          -> "ListQueues",
      "QueueNamePrefix" -> queueName
    )) \\ "QueueUrl").map(_.text).filter(_.endsWith("/"+queueName))
    if (!queues.isEmpty) {
      val path = queues.first.substring("http://queue.amazonaws.com".length)+"/"
      new Queue(conn, path)
    } else {
      error("No such queue: '"+queueName+"'")
    }
  }

  def +=(name: String): Unit =
    conn.request(Map("Action" -> "CreateQueue", "QueueName" -> name))

  def +=(name: String, defaultVisibilityTimeout: Int): Unit =
    conn.request(Map(
      "Action"                    -> "CreateQueue",
      "QueueName"                 -> name,
      "DefaultVisibilityTimeout"  -> defaultVisibilityTimeout.toString
    ))

  def -=(name: String): Unit =
    conn.request(Map("Action" -> "DeleteQueue", "QueueName" -> name))

  def elements: Iterable[String] =
    (conn.request(Map("Action" -> "ListDomains")) \\ "QueueName").map(_.text)
}

