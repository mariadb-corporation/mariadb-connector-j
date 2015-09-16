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
            case 4:
            case 80:
                return MySQLCharset.CP850;
            case 5:
            case 8:
            case 15:
            case 31:
            case 47:
            case 48:
            case 49:
            case 94:
                return MySQLCharset.LATIN1;
            case 11:
            case 65:
            case 92:
            case 93:
                return MySQLCharset.ASCII;
            case 14:
            case 23:
            case 50:
            case 51:
            case 52:
                return MySQLCharset.CP1251;
            case 16:
            case 71:
                return MySQLCharset.HEBREW;
            case 19:
            case 85:
                return MySQLCharset.EUCKR;
            case 24:
            case 86:
                return MySQLCharset.GB2312;
            case 25:
            case 70:
                return MySQLCharset.GREEK;
            case 26:
            case 34:
            case 44:
            case 66:
            case 99:
                return MySQLCharset.CP1250;
            case 28:
            case 87:
                return MySQLCharset.GBK;
            case 29:
            case 58:
            case 59:
                return MySQLCharset.CP1257;
            case 32:
            case 64:
                return MySQLCharset.ARMSCII8;
            case 36:
            case 68:
                return MySQLCharset.CP866;
            case 40:
            case 81:
                return MySQLCharset.CP852;
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
            case 57:
            case 67:
                return MySQLCharset.CP1256;
            case 63:
                return MySQLCharset.BINARY;
            case 95:
            case 96:
                return MySQLCharset.CP932;
            case 97:
            case 98:
                return MySQLCharset.EUCJPMS;
            default:
                return MySQLCharset.BINARY;

        }
    }

}
