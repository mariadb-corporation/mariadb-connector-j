/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/
package org.mariadb.jdbc.internal.common;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.TreeMap;


public class CharsetUtils {
    /*private static final Map<String, MySQLCharset> charsets;
    static {
        charsets = new TreeMap();
        charsets.put("1", MySQLCharset.BIG5);
        charsets.put("2", MySQLCharset.ISO8859_2);
        charsets.put("3", MySQLCharset.CP1252);
        /*charsets.put("4","cp850");
        charsets.put("5","latin1");
        charsets.put("6","hp8");
        charsets.put("7","koi8r");
        charsets.put("8","latin1");
        charsets.put("9","latin2");
        charsets.put("10","swe7");
        charsets.put("11","ascii");
        charsets.put("12","ujis");
        charsets.put("13","sjis");
        charsets.put("14","cp1251");
        charsets.put("15","latin1");
        charsets.put("16","hebrew");
        charsets.put("18","tis620");
        charsets.put("19","euckr");
        charsets.put("20","latin7");
        charsets.put("21","latin2");
        charsets.put("22","koi8u");
        charsets.put("23","cp1251");
        charsets.put("24","gb2312");
        charsets.put("25","greek");
        charsets.put("26","cp1250");
        charsets.put("27","latin2");
        charsets.put("28","gbk");
        charsets.put("29","cp1257");
        charsets.put("30","latin5");
        charsets.put("31","latin1");
        charsets.put("32","armscii8");
        charsets.put("33", MySQLCharset.UTF8);
        charsets.put("34","cp1250");
        charsets.put("35","ucs2");
        charsets.put("36","cp866");
        charsets.put("37","keybcs2");
        charsets.put("38","macce");
        charsets.put("39","macroman");
        charsets.put("40","cp852");
        charsets.put("41","latin7");
        charsets.put("42","latin7");
        charsets.put("43","macce");
        charsets.put("44","cp1250");
        charsets.put("45","utf8mb4");
        charsets.put("46","utf8mb4");
        charsets.put("47","latin1");
        charsets.put("48","latin1");
        charsets.put("49","latin1");
        charsets.put("50","cp1251");
        charsets.put("51","cp1251");
        charsets.put("52","cp1251");
        charsets.put("53","macroman");
        charsets.put("54","utf16");
        charsets.put("55","utf16");
        charsets.put("56","utf16le");
        charsets.put("57","cp1256");
        charsets.put("58","cp1257");
        charsets.put("59","cp1257");
        charsets.put("60","utf32");
        charsets.put("61","utf32");
        charsets.put("62","utf16le");
        charsets.put("63","binary");
        charsets.put("64","armscii8");
        charsets.put("65","ascii");
        charsets.put("66","cp1250");
        charsets.put("67","cp1256");
        charsets.put("68","cp866");
        charsets.put("69","dec8");
        charsets.put("70","greek");
        charsets.put("71","hebrew");
        charsets.put("72","hp8");
        charsets.put("73","keybcs2");
        charsets.put("74","koi8r");
        charsets.put("75","koi8u");
        charsets.put("77","latin2");
        charsets.put("78","latin5");
        charsets.put("79","latin7");
        charsets.put("80","cp850");
        charsets.put("81","cp852");
        charsets.put("82","swe7");
        charsets.put("83","utf8");
        charsets.put("84","big5");
        charsets.put("85","euckr");
        charsets.put("86","gb2312");
        charsets.put("87","gbk");
        charsets.put("88","sjis");
        charsets.put("89","tis620");
        charsets.put("90","ucs2");
        charsets.put("91","ujis");
        charsets.put("92","geostd8");
        charsets.put("93","geostd8");
        charsets.put("94","latin1");
        charsets.put("95","cp932");
        charsets.put("96","cp932");
        charsets.put("97","eucjpms");
        charsets.put("98","eucjpms");
        charsets.put("99","cp1250");
        charsets.put("101","utf16");
        charsets.put("102","utf16");
        charsets.put("103","utf16");
        charsets.put("104","utf16");
        charsets.put("105","utf16");
        charsets.put("106","utf16");
        charsets.put("107","utf16");
        charsets.put("108","utf16");
        charsets.put("109","utf16");
        charsets.put("110","utf16");
        charsets.put("111","utf16");
        charsets.put("112","utf16");
        charsets.put("113","utf16");
        charsets.put("114","utf16");
        charsets.put("115","utf16");
        charsets.put("116","utf16");
        charsets.put("117","utf16");
        charsets.put("118","utf16");
        charsets.put("119","utf16");
        charsets.put("120","utf16");
        charsets.put("121","utf16");
        charsets.put("122","utf16");
        charsets.put("123","utf16");
        charsets.put("124","utf16");
        charsets.put("128","ucs2");
        charsets.put("129","ucs2");
        charsets.put("130","ucs2");
        charsets.put("131","ucs2");
        charsets.put("132","ucs2");
        charsets.put("133","ucs2");
        charsets.put("134","ucs2");
        charsets.put("135","ucs2");
        charsets.put("136","ucs2");
        charsets.put("137","ucs2");
        charsets.put("138","ucs2");
        charsets.put("139","ucs2");
        charsets.put("140","ucs2");
        charsets.put("141","ucs2");
        charsets.put("142","ucs2");
        charsets.put("143","ucs2");
        charsets.put("144","ucs2");
        charsets.put("145","ucs2");
        charsets.put("146","ucs2");
        charsets.put("147","ucs2");
        charsets.put("148","ucs2");
        charsets.put("149","ucs2");
        charsets.put("150","ucs2");
        charsets.put("151","ucs2");
        charsets.put("159","ucs2");
        charsets.put("160","utf32");
        charsets.put("161","utf32");
        charsets.put("162","utf32");
        charsets.put("163","utf32");
        charsets.put("164","utf32");
        charsets.put("165","utf32");
        charsets.put("166","utf32");
        charsets.put("167","utf32");
        charsets.put("168","utf32");
        charsets.put("169","utf32");
        charsets.put("170","utf32");
        charsets.put("171","utf32");
        charsets.put("172","utf32");
        charsets.put("173","utf32");
        charsets.put("174","utf32");
        charsets.put("175","utf32");
        charsets.put("176","utf32");
        charsets.put("177","utf32");
        charsets.put("178","utf32");
        charsets.put("179","utf32");
        charsets.put("180","utf32");
        charsets.put("181","utf32");
        charsets.put("182","utf32");
        charsets.put("183","utf32");
        charsets.put("192","utf8");
        charsets.put("193","utf8");
        charsets.put("194","utf8");
        charsets.put("195","utf8");
        charsets.put("196","utf8");
        charsets.put("197","utf8");
        charsets.put("198","utf8");
        charsets.put("199","utf8");
        charsets.put("200","utf8");
        charsets.put("201","utf8");
        charsets.put("202","utf8");
        charsets.put("203","utf8");
        charsets.put("204","utf8");
        charsets.put("205","utf8");
        charsets.put("206","utf8");
        charsets.put("207","utf8");
        charsets.put("208","utf8");
        charsets.put("209","utf8");
        charsets.put("210","utf8");
        charsets.put("211","utf8");
        charsets.put("212","utf8");
        charsets.put("213","utf8");
        charsets.put("214","utf8");
        charsets.put("215","utf8");
        charsets.put("223","utf8");
        charsets.put("224","utf8mb4");
        charsets.put("225","utf8mb4");
        charsets.put("226","utf8mb4");
        charsets.put("227","utf8mb4");
        charsets.put("228","utf8mb4");
        charsets.put("229","utf8mb4");
        charsets.put("230","utf8mb4");
        charsets.put("231","utf8mb4");
        charsets.put("232","utf8mb4");
        charsets.put("233","utf8mb4");
        charsets.put("234","utf8mb4");
        charsets.put("235","utf8mb4");
        charsets.put("236","utf8mb4");
        charsets.put("237","utf8mb4");
        charsets.put("238","utf8mb4");
        charsets.put("239","utf8mb4");
        charsets.put("240","utf8mb4");
        charsets.put("241","utf8mb4");
        charsets.put("242","utf8mb4");
        charsets.put("243","utf8mb4");
        charsets.put("244","utf8mb4");
        charsets.put("245","utf8mb4");
        charsets.put("246","utf8mb4");
        charsets.put("247","utf8mb4");
        charsets.put("576","utf8");
        charsets.put("577","utf8");
        charsets.put("608","utf8mb4");
        charsets.put("609","utf8mb4");
        charsets.put("640","ucs2");
        charsets.put("641","ucs2");
        charsets.put("672","utf16");
        charsets.put("673","utf16");
        charsets.put("736","utf32");
        charsets.put("737","utf32");
    }
*/

    public static MySQLCharset getServerCharset(int serverCharsetByte) {
        switch (serverCharsetByte) {
            case 1 :
            case 84 :
                return MySQLCharset.BIG5;
            case 2:
            case 9:
            case 21:
            case 27:
            case 77:
                return MySQLCharset.ISO8859_2;
            case 3:
            case 69:
                return MySQLCharset.CP1252;
            case 5:
            case 8:
            case 15:
            case 31:
            case 47:
            case 48:
            case 49:
            case 94:
                return MySQLCharset.LATIN1;
            case 33:
            case 83:
            case 192:
            case 193:
            case 194:
            case 195:
            case 196:
            case 197:
            case 198:
            case 199:
            case 200:
            case 201:
            case 202:
            case 203:
            case 204:
            case 205:
            case 206:
            case 207:
            case 208:
            case 209:
            case 210:
            case 211:
            case 212:
            case 213:
            case 214:
            case 215:
            case 223:
            case 576:
            case 577:
                return MySQLCharset.UTF8;
            default:
                return null;

        }
    }

}
