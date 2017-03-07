/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

package org.mariadb.jdbc.internal.packet.dao.parameters;

import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.MariaDbType;
import java.sql.SQLException;


public class StringParameter extends NotLongDataParameter implements Cloneable {

    private String stringValue;
    private boolean noBackslashEscapes;
    private byte[] escapedArray = null;
    private int position;
    private int charsOffset;
    private boolean binary;

    public StringParameter(String str, boolean noBackslashEscapes) throws SQLException {
        this.stringValue = str;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    /**
     * Send escaped String to outputStream.
     *
     * @param os outpustream.
     */
    public void writeTo(final PacketOutputStream os) {
        if (escapedArray == null) escapeUtf8();
        os.write(escapedArray, 0, position);
    }

    /**
     * Send escaped String to outputStream, without checking outputStream buffer capacity.
     *
     * @param os outpustream.
     */
    public void writeUnsafeTo(final PacketOutputStream os) {
        if (escapedArray == null) escapeUtf8();
        os.buffer.put(escapedArray, 0, position);
    }

    public long getApproximateTextProtocolLength() {
        if (escapedArray == null) escapeUtf8();
        return position;
    }

    /**
     * Send string value to server in binary format.
     *
     * @param writer socket to server.
     */
    public void writeBinary(final PacketOutputStream writer) {
        if (escapedArray == null) utf8();
        writer.assureBufferCapacity(position + 9);
        writer.writeFieldLength(position);
        writer.buffer.put(escapedArray, 0, position);
        escapedArray = null; //in case ServerPrepareStmt fallback to clientPrepareStmt (need escape)
    }

    public MariaDbType getMariaDbType() {
        return MariaDbType.VARCHAR;
    }

    @Override
    public String toString() {
        if (stringValue != null) {
            if (stringValue.length() < 1024) {
                return "'" + stringValue + "'";
            } else {
                return "'" + stringValue.substring(0, 1024) + "...'";
            }
        } else {
            if (position > 1024) {
                //escape bytes have integrated quote, binary hasn't
                if (binary) return "'" + new String(escapedArray, 0, 1024) + "...'";
                return new String(escapedArray, 0, 1024) + "...'";
            } else {
                //escape bytes have integrated quote, binary hasn't
                if (binary) return "'" + new String(escapedArray, 0, position) + "'";
                return new String(escapedArray, 0, position);
            }
        }
    }

    /**
     * Encode char array to UTF-8 byte array, with SQL escape.
     *
     * Driver exchange with server use exclusively UTF-8 and lot of parameters are string.
     * Most the time spend using driver is spend transforming String parameters in UTF-8 escaped bytes.
     *
     * Every String as to be escaped to avoid SQL injection, so whe must loop through the String char array
     * to add escape chars, creating a new char array, and after decoding this char array to UTF-8 bytes.
     *
     * Using standard java, this result in :
     * - recreating a lot of char array (and copy to new array) during escape.
     * - utf-8 encoding is using internally a byte array initialized to 3 * the char length and will be spliced when final length is known.
     * when dealing with big string, that is using a lot of array for nothing (and so memory issues)
     *
     * Resulting byte array will be send to outputStream, so :
     * - escape characters are ASCII characters (one byte) -> the escaped characters can be put directly in array
     * - driver don't need the final byte array shrink, the byte array can be send directly in outputStream.
     *
     *
     * Example for a 1k character String :
     * if using standard java
     * - getting toCharArray() will create a new 1k char array (and copy existing array into it).
     * - escaping (using a StringBuffer) will use a new 1k char array and the internal array may expand (=> new allocation + copy) if there is
     *   any char to be escape
     * - getBytes("UTF-8") : a new 3k byte buffer will be created, and finally a new one (+copy into it) when real length is known.
     *
     * Current implementation :
     * - creating a 3k bytes array, send directly these array with length into outputStream.
     * => only one array.
     *
     */
    private void escapeUtf8() {
        int charsLength = stringValue.length();
        charsOffset = 0;
        position = 0;

        //handle "" String value
        if (charsLength == 0) {
            position = 2;
            escapedArray = new byte[] {(byte)'\'', (byte)'\''};
            return;
        }

        //create UTF-8 byte array
        //since java char are internally using UTF-16 using surrogate's pattern, 4 bytes unicode characters will
        //represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
        //so max size is 3 * charLength + 2 for the quotes.
        //(escaping concern only ASCII characters (1 bytes) and when escaped will be 2 bytes = won't cause any problems)
        escapedArray = new byte[(charsLength * 3) + 2];
        escapedArray[position++] = (byte)'\'';

        char charValue;
        //Handle fast conversion without testing kind of escape for each character
        if (noBackslashEscapes) {
            do {
                charValue = stringValue.charAt(charsOffset);
                if (charValue < 0x80) {
                    if (charValue == '\'') escapedArray[position++] = (byte) '\''; //add a single escape quote
                    escapedArray[position++] = (byte) charValue;
                    charsOffset++;
                } else {
                    break;
                }
            } while (charsOffset < charsLength);
        } else {
            do {
                charValue = stringValue.charAt(charsOffset);
                if (charValue < 0x80) {
                    if (charValue  == '\''
                            || charValue == '\\'
                            || charValue == '"'
                            || charValue == 0) escapedArray[position++] = (byte) '\\'; //add escape slash
                    escapedArray[position++] = (byte) charValue;
                    charsOffset++;
                } else {
                    break;
                }
            } while (charsOffset < charsLength);

        }

        //if contain non ASCII chars
        while (charsOffset < charsLength) {
            char currChar = stringValue.charAt(charsOffset++);
            if (currChar < 0x80) {
                if (currChar == '\'') {
                    escapedArray[position++] = noBackslashEscapes ? (byte) '\'' : (byte) '\\';
                } else if (!noBackslashEscapes && (currChar  == '\\' || currChar  == '"' || currChar  == 0)) {
                    escapedArray[position++] = (byte) '\\';
                }
                escapedArray[position++] = (byte) currChar;
            } else getNonAsciiByte(currChar, stringValue, charsLength);
        }
        escapedArray[position++] = (byte) '\'';
        stringValue = null;
        binary = false;
    }

    private void getNonAsciiByte(char currChar, String stringValue, int charsLength) {
        if (currChar < 0x800) {
            escapedArray[position++] = (byte) (0xc0 | (currChar >> 6));
            escapedArray[position++] = (byte) (0x80 | (currChar & 0x3f));
        } else if (currChar >= 0xD800 && currChar < 0xE000) {
            //reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
            if (currChar >= 0xD800 && currChar < 0xDC00) {
                //is high surrogate
                if (charsOffset + 1 > charsLength) {
                    escapedArray[position++] = (byte)0x63;
                    return;
                }
                char nextChar = stringValue.charAt(charsOffset);
                if (nextChar >= 0xDC00 && nextChar < 0xE000) {
                    //is low surrogate
                    int surrogatePairs =  ((currChar << 10) + nextChar) + (0x010000 - (0xD800 << 10) - 0xDC00);
                    escapedArray[position++] = (byte) (0xf0 | ((surrogatePairs >> 18)));
                    escapedArray[position++] = (byte) (0x80 | ((surrogatePairs >> 12) & 0x3f));
                    escapedArray[position++] = (byte) (0x80 | ((surrogatePairs >> 6) & 0x3f));
                    escapedArray[position++] = (byte) (0x80 | (surrogatePairs & 0x3f));
                    charsOffset++;
                } else {
                    //must have low surrogate
                    escapedArray[position++] = (byte)0x63;
                }
            } else {
                //low surrogate without high surrogate before
                escapedArray[position++] = (byte)0x63;
            }
        } else {
            escapedArray[position++] = (byte) (0xe0 | ((currChar >> 12)));
            escapedArray[position++] = (byte) (0x80 | ((currChar >> 6) & 0x3f));
            escapedArray[position++] = (byte) (0x80 | (currChar & 0x3f));
        }
    }

    /**
     * Get fast UTF-8 array from String.
     */
    private void utf8() {
        //get char array
        int charsLength = stringValue.length();
        charsOffset = 0;
        position = 0;

        escapedArray = new byte[(charsLength * 3)];

        while (charsOffset < charsLength) {
            char currChar = stringValue.charAt(charsOffset++);
            if (currChar < 0x80) {
                escapedArray[position++] = (byte) currChar;
            } else getNonAsciiByte(currChar, stringValue, charsLength);
        }
        binary = true;
    }
}
