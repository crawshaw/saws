// Copyright 2003-2009 Christian d'Heureuse,
// Inventec Informatik AG, Zurich, Switzerland
// www.source-code.biz, www.inventec.ch/chdh
//
// This module is multi-licensed and may be used under the terms
// of any of the following licenses:
//
//  EPL, Eclipse Public License, http://www.eclipse.org/legal
//  LGPL, GNU Lesser General Public License,
//    http://www.gnu.org/licenses/lgpl.html
//  AL, Apache License, http://www.apache.org/licenses
//  BSD, BSD License, http://www.opensource.org/licenses/bsd-license.php
//
// Please contact the author if you need another license.
// This module is provided "as is", without warranties of any kind.

package com.zentus

object Base64 {

  // Mapping table from 6-bit nibbles to Base64 characters.
  private val map1 = (
      ('A' to 'Z') ++ ('a' to 'z') ++ ('0' to '9')
  ).toArray[Char] ++ Array('+', '/')

  // Mapping table from Base64 characters to 6-bit nibbles.
  private val map2 = new Array[Byte](128);
  {
    for (i <- 0 until map2.length) map2(i) = -1;
    for (i <- 0 until 64) map2(map1(i)) = i.toByte;
  }

  def encode(s: String) : String =
    new String(encode(s.getBytes()))

  def encode(in: Array[Byte]) : Array[Char] =
    encode(in,in.length)

  def encode(in: Array[Byte], iLen: Int) : Array[Char] = {
    val oDataLen = (iLen*4+2)/3;       // output length without padding
    val oLen = ((iLen+2)/3)*4;         // output length including padding
    val out = new Array[Char](oLen);
    var ip = 0;
    var op = 0;
    while (ip < iLen) {
      val i0 = in(ip) & 0xff;
      ip += 1;
      val i1 = if (ip < iLen) in(ip) & 0xff else 0;
      ip += 1;
      val i2 = if (ip < iLen) in(ip) & 0xff else 0;
      ip += 1;
      val o0 = i0 >>> 2;
      val o1 = ((i0 &   3) << 4) | (i1 >>> 4);
      val o2 = ((i1 & 0xf) << 2) | (i2 >>> 6);
      val o3 = i2 & 0x3F;
      out(op) = map1(o0);
      op += 1;
      out(op) = map1(o1);
      op += 1;
      out(op) = if (op < oDataLen) map1(o2) else '=';
      op += 1;
      out(op) = if (op < oDataLen) map1(o3) else '=';
      op += 1;
    }

    out
  }

  def decode(s : String) : Array[Byte] =
    decode(s.toCharArray())

  def decode(in : Array[Char]) : Array[Byte] = {
    var iLen = in.length;
    if (iLen % 4 != 0) {
      throw new IllegalArgumentException(
        "Length of Base64 encoded input string is not a multiple of 4."
      );
    }
    while (iLen > 0 && in(iLen-1) == '=') iLen -= 1;
    val oLen = (iLen*3) / 4;
    var out = new Array[Byte](oLen);
    var ip = 0;
    var op = 0;
    while (ip < iLen) {
      val i0 = in(ip); ip += 1;
      val i1 = in(ip); ip += 1;
      val i2 = if (ip < iLen) in(ip) else 'A'; ip += 1;
      val i3 = if (ip < iLen) in(ip) else 'A'; ip += 1;
      if (i0 > 127 || i1 > 127 || i2 > 127 || i3 > 127)
         throw new IllegalArgumentException ("Illegal character in Base64 encoded data.");
      val b0 = map2(i0);
      val b1 = map2(i1);
      val b2 = map2(i2);
      val b3 = map2(i3);
      if (b0 < 0 || b1 < 0 || b2 < 0 || b3 < 0)
         throw new IllegalArgumentException ("Illegal character in Base64 encoded data.");
      val o0 = ( b0       <<2) | (b1>>>4);
      val o1 = ((b1 & 0xf)<<4) | (b2>>>2);
      val o2 = ((b2 &   3)<<6) |  b3;
      out(op) = o0.toByte;
      op += 1;
      if (op<oLen) { out(op) = o1.toByte; op +=1; }
      if (op<oLen) { out(op) = o2.toByte; op +=1; }
    }
    out
  }

}
