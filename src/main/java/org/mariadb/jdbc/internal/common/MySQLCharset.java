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

public enum MySQLCharset {
    BIG5(1, "big5", "Big5", "Big5"),
    CP1252(3, "dec8", "Cp1252", "windows-1252"),
    CP850(4, "cp850", "Cp850", "IBM850"),
    HP8(6,"hp8","Cp1252","windows-1252"), //
    KOI8R(7,"koi8r","KOI8_R","KOI8-R"),
    LATIN1(8, "latin1", "ISO8859_1", "ISO-8859-1"),
    LATIN2(9, "latin2", "ISO8859_2", "ISO-8859-2"),
    SWE7(10,"swe7", "Cp1252", "windows-1252"),
    ASCII(11,"ascii","ASCII", "US-ASCII"),
    UJIS(12,"ujis","EUC_JP","EUC-JP"),
    SJIS(13,"sjis","SJIS","Shift_JIS"),
    HEBREW(16,"hebrew","ISO8859_8","ISO-8859-8"),
    TIS620(18,"tis620","TIS620","TIS-620"),
    EUCKR(19,"euckr","EUC_KR", "EUC-KR"),
    KOI8U(22,"koi8u","KOI8_U","KOI8-U"),
    GB2312(24,"gb2312","EUC_CN","GB2312"),
    GREEK(25,"greek","ISO8859_7","ISO-8859-7"),
    CP1250(26,"cp1250","Cp1250","windows-1250"),
    GBK(28,"gbk","GBK","GBK"),
    LATIN5(30,"latin5","ISO8859_9","ISO-8859-9"),
    ARMSCII8(32, "armscii8", "ISO8859_1", "ISO-8859-1"),
    UTF8(33, "utf8", "UTF8", "UTF-8"),
    UCS2(35,"ucs2","UnicodeBig", null),
    CP866(36, "cp866", "Cp866", "IBM866"),
    KEYBCS2(37, "keybcs2", "Cp852", "IBM852"), //Cp895 not supported by java
    MACCE(38,"macce","MacCentralEurope","x-MacCentralEurope"),
    MACROMAN(39,"macroman","MacRoman","x-MacRoman"),
    CP852(40, "cp852", "Cp852", "IBM852"),
    UTF8MB4(45,"utf8mb4","UTF-8","UTF-8"),
    LATIN7(41,"latin7","ISO8859_13","ISO-8859-13"),
    CP1251(51,"cp1251","Cp1251","windows-1251"),
    UTF16(54,"utf16","UTF-16","UTF-16"),
    UTF16LE(56,"utf16le","UnicodeLittleUnmarked","UTF-16LE"),
    CP1256(57,"cp1256","Cp1256","windows-1256"),
    CP1257(59,"cp1257","Cp1257","windows-1257"),
    UTF32(60,"utf32","UTF_32","UTF-32"),
    BINARY(63,"binary",null,null),
    GEOSTD8(92,"geostd8","Cp942","x-IBM942"),
    CP932(95,"cp932","Cp942","x-IBM942"),
    EUCJPMS(97,"eucjpms","EUC_JP_Solaris","x-eucJP-Open");


    public final int defaultId;
    public final String mysqlCharsetName;
    public final String javaIoCharsetName;
    public final String javaNioCharsetName;
    //public final Charset nioCharset;

    MySQLCharset(int defaultId, String mysqlCharsetName, String javaIoCharsetName, String javaNioCharsetName) {
        this.defaultId = defaultId;
        this.mysqlCharsetName = mysqlCharsetName;
        this.javaIoCharsetName = javaIoCharsetName;
        this.javaNioCharsetName = javaNioCharsetName;
        /*if (javaIoCharsetName == null) {
            this.nioCharset = null;
        } else  this.nioCharset = Charset.forName(javaIoCharsetName);*/
    }

}
