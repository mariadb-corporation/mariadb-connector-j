package org.mariadb.jdbc.internal.common;

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


public class CharsetUtils {
    /**
     * Retrieve Charset from serverCharset identifier.
     * @param serverCharsetByte server charset identifier.
     * @return Charset if found.
     */
    public static MariaDbCharset getServerCharset(int serverCharsetByte) {
        switch (serverCharsetByte) {
            case 1:
            case 84:
                return MariaDbCharset.BIG5;
            case 2:
            case 9:
            case 21:
            case 27:
            case 77:
                return MariaDbCharset.LATIN2;
            case 3:
            case 69:
                return MariaDbCharset.CP1252;
            case 4:
            case 80:
                return MariaDbCharset.CP850;
            case 5:
            case 8:
            case 15:
            case 31:
            case 47:
            case 48:
            case 49:
            case 94:
                return MariaDbCharset.LATIN1;
            case 6:
            case 72:
                return MariaDbCharset.HP8;
            case 7:
            case 74:
                return MariaDbCharset.KOI8R;
            case 10:
            case 82:
                return MariaDbCharset.SWE7;
            case 11:
            case 65:
            case 92:
            case 93:
                return MariaDbCharset.ASCII;
            case 12:
            case 91:
                return MariaDbCharset.UJIS;
            case 13:
            case 88:
                return MariaDbCharset.SJIS;
            case 14:
            case 23:
            case 50:
            case 51:
            case 52:
                return MariaDbCharset.CP1251;
            case 16:
            case 71:
                return MariaDbCharset.HEBREW;
            case 18:
            case 89:
                return MariaDbCharset.TIS620;
            case 19:
            case 85:
                return MariaDbCharset.EUCKR;
            case 20:
            case 41:
            case 42:
            case 79:
                return MariaDbCharset.LATIN7;
            case 22:
            case 75:
                return MariaDbCharset.KOI8U;
            case 24:
            case 86:
                return MariaDbCharset.GB2312;
            case 25:
            case 70:
                return MariaDbCharset.GREEK;
            case 26:
            case 34:
            case 44:
            case 66:
            case 99:
                return MariaDbCharset.CP1250;
            case 28:
            case 87:
                return MariaDbCharset.GBK;
            case 29:
            case 58:
            case 59:
                return MariaDbCharset.CP1257;
            case 30:
            case 78:
                return MariaDbCharset.LATIN5;
            case 32:
            case 64:
                return MariaDbCharset.ARMSCII8;
            case 35:
            case 90:
            case 128:
            case 129:
            case 130:
            case 131:
            case 132:
            case 133:
            case 134:
            case 135:
            case 136:
            case 137:
            case 138:
            case 139:
            case 140:
            case 141:
            case 142:
            case 143:
            case 144:
            case 145:
            case 146:
            case 147:
            case 148:
            case 149:
            case 150:
            case 151:
            case 159:
            case 640:
            case 641:
                return MariaDbCharset.UCS2;
            case 36:
            case 68:
                return MariaDbCharset.CP866;
            case 37:
            case 73:
                return MariaDbCharset.KEYBCS2;
            case 38:
            case 43:
                return MariaDbCharset.MACCE;
            case 39:
            case 53:
                return MariaDbCharset.MACROMAN;
            case 40:
            case 81:
                return MariaDbCharset.CP852;
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
                return MariaDbCharset.UTF8;
            case 45:
            case 46:
            case 224:
            case 225:
            case 226:
            case 227:
            case 228:
            case 229:
            case 230:
            case 231:
            case 232:
            case 233:
            case 234:
            case 235:
            case 236:
            case 237:
            case 238:
            case 239:
            case 240:
            case 241:
            case 242:
            case 243:
            case 244:
            case 245:
            case 246:
            case 247:
            case 608:
            case 609:
                return MariaDbCharset.UTF8MB4;
            case 54:
            case 55:
            case 101:
            case 102:
            case 103:
            case 104:
            case 105:
            case 106:
            case 107:
            case 108:
            case 109:
            case 110:
            case 111:
            case 112:
            case 113:
            case 114:
            case 115:
            case 116:
            case 117:
            case 118:
            case 119:
            case 121:
            case 122:
            case 123:
            case 124:
            case 672:
            case 673:
                return MariaDbCharset.UTF16;
            case 56:
            case 62:
                return MariaDbCharset.UTF16LE;
            case 57:
            case 67:
                return MariaDbCharset.CP1256;
            case 60:
            case 61:
            case 160:
            case 161:
            case 162:
            case 163:
            case 164:
            case 165:
            case 166:
            case 167:
            case 168:
            case 169:
            case 170:
            case 171:
            case 172:
            case 173:
            case 174:
            case 175:
            case 176:
            case 177:
            case 178:
            case 179:
            case 180:
            case 181:
            case 182:
            case 183:
            case 736:
            case 737:
                return MariaDbCharset.UTF32;
            case 63:
                return MariaDbCharset.BINARY;
            case 95:
            case 96:
                return MariaDbCharset.CP932;
            case 97:
            case 98:
                return MariaDbCharset.EUCJPMS;
            default:
                return MariaDbCharset.BINARY;

        }
    }

}
