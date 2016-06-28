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

import java.lang.reflect.Field;
import java.sql.SQLException;


public class StringParameter implements ParameterHolder, Cloneable {
    public static Field charsFieldValue;
    static {
        try {
            charsFieldValue = String.class.getDeclaredField("value");
        } catch (NoSuchFieldException e) { }
        charsFieldValue.setAccessible(true);
    }

    private String string;
    private boolean noBackslashEscapes;
    private byte[] escapedArray = null;
    private int position;

    public StringParameter(String string, boolean noBackslashEscapes) throws SQLException {
        this.string = string;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    /**
     * Send escaped String to outputStream.
     *
     * @param os outpustream.
     */
    public void writeTo(final PacketOutputStream os) {
        if (escapedArray == null) escapeForText();
        os.write(escapedArray, 0, position);
    }

    /**
     * Send escaped String to outputStream, without checking outputStream buffer capacity.
     *
     * @param os outpustream.
     */
    public void writeUnsafeTo(final PacketOutputStream os) {
        if (escapedArray == null) escapeForText();
        os.buffer.put(escapedArray, 0, position);
    }

    public long getApproximateTextProtocolLength() {
        escapeForText();
        return position;
    }

    public void writeBinary(final PacketOutputStream writeBuffer) {
        writeBuffer.writeStringLength(string);
    }

    public MariaDbType getMariaDbType() {
        return MariaDbType.VARCHAR;
    }

    @Override
    public String toString() {
        if (string != null) {
            if (string.length() < 1024) {
                return "'" + string + "'";
            } else {
                return "'" + string.substring(0, 1024) + "...'";
            }
        } else {
            if (position > 1024) {
                return new String(escapedArray, 0, 1024) + "...'";
            } else {
                return new String(escapedArray, 0, position);
            }
        }
    }

    /**
     * Encode char array to UTF-8 byte array, with SQL escape.
     * (this permit to create an array with string length * 3 with UTF-8 byte value directly escape.
     *  so doesn't have to make a array shrink when size is known, to escape value. This buffer can directly be put
     *  to outputBuffer with offset 0, length the return end position)
     **/
    private void escapeForText() {
        char[] chars;
        try {
            chars = (char[]) charsFieldValue.get(string);
        } catch (IllegalAccessException e) {
            chars = string.toCharArray();
        }
        string = null;
        int charsLength = chars.length;
        int charsOffset = 0;
        position = 0;

        escapedArray = new byte[(charsLength * 3) + 2];
        escapedArray[position++] = ParameterWriter.QUOTE;

        int maxCharIndex = charsOffset + charsLength;
        int maxLength = Math.min(charsLength, escapedArray.length);

        // ASCII only optimized loop
        if (noBackslashEscapes) {
            while (position < maxLength && chars[charsOffset] < '\u0080') {
                if (chars[charsOffset] == '\'') {
                    escapedArray[position++] = (byte) 0x27;
                }
                escapedArray[position++] = (byte) chars[charsOffset++];
            }
        } else {
            while (position < maxLength && chars[charsOffset] < '\u0080') {
                switch (chars[charsOffset]) {
                    case '\'':
                        escapedArray[position++] = (byte) 0x5c;
                        escapedArray[position++] = (byte) chars[charsOffset++];
                        break;
                    case '\\':
                    case '"':
                    case 0:
                        escapedArray[position++] = (byte) 0x5c;
                        escapedArray[position++] = (byte) chars[charsOffset++];
                        break;
                    default:
                        escapedArray[position++] = (byte) chars[charsOffset++];
                }
            }
        }

        while (charsOffset < maxCharIndex) {
            char currChar = chars[charsOffset++];
            if (currChar < 0x80) {
                switch (currChar) {
                    case '\'':
                        if (noBackslashEscapes) {
                            escapedArray[position++] = (byte) 0x27;
                        } else {
                            escapedArray[position++] = (byte) 0x5c;
                        }
                        escapedArray[position++] = (byte) currChar;
                        break;
                    case '\\':
                    case '"':
                    case 0:
                        if (!noBackslashEscapes) {
                            escapedArray[position++] = (byte) 0x5c;
                        }
                        escapedArray[position++] = (byte) currChar;
                        break;
                    default:
                        escapedArray[position++] = (byte) currChar;
                }

            } else if (currChar < 0x800) {
                escapedArray[position++] = (byte) (0xc0 | (currChar >> 6));
                escapedArray[position++] = (byte) (0x80 | (currChar & 0x3f));
            } else if (Character.isSurrogate(currChar)) {
                //see https://en.wikipedia.org/wiki/Universal_Character_Set_characters#Surrogates
                int surrogatePairs = parseSurrogatePair(currChar, chars, charsOffset - 1, maxCharIndex);
                if (surrogatePairs < 0) {
                    //if malformed, replace by filler
                    escapedArray[position++] = (byte) 63;
                } else {
                    escapedArray[position++] = (byte) (0xf0 | ((surrogatePairs >> 18)));
                    escapedArray[position++] = (byte) (0x80 | ((surrogatePairs >> 12) & 0x3f));
                    escapedArray[position++] = (byte) (0x80 | ((surrogatePairs >> 6) & 0x3f));
                    escapedArray[position++] = (byte) (0x80 | (surrogatePairs & 0x3f));
                    charsOffset++;
                }
            } else {
                escapedArray[position++] = (byte) (0xe0 | ((currChar >> 12)));
                escapedArray[position++] = (byte) (0x80 | ((currChar >> 6) & 0x3f));
                escapedArray[position++] = (byte) (0x80 | (currChar & 0x3f));
            }
        }

        escapedArray[position++] = ParameterWriter.QUOTE;
    }

    private static int parseSurrogatePair(char currChar, char[] chars, int offset, int maxCharIndex) {
        assert chars[offset] == currChar;
        if (Character.isHighSurrogate(currChar)) {
            if (maxCharIndex - offset < 2) {
                return -1;
            } else {
                char nextCodePoint = chars[offset + 1];
                if (Character.isLowSurrogate(nextCodePoint)) {
                    return Character.toCodePoint(currChar, nextCodePoint);
                } else {
                    return -1;
                }
            }
        } else if (Character.isLowSurrogate(currChar)) {
            return -1;
        } else {
            return currChar;
        }
    }

    public boolean isLongData() {
        return false;
    }

}
