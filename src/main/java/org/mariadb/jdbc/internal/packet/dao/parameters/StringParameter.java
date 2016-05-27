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


public class StringParameter extends NotLongDataParameterHolder {
    private String string;
    private boolean noBackslashEscapes;
    private byte[] escapeValueUtf8 = null;
    private int endPos;

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
        if (escapeValueUtf8 == null) escapeForText();
        os.write(ParameterWriter.QUOTE);
        os.write(escapeValueUtf8, 0, endPos);
        os.write(ParameterWriter.QUOTE);
    }

    /**
     * Send escaped String to outputStream, without checking outputStream buffer capacity.
     *
     * @param os outpustream.
     */
    public void writeUnsafeTo(final PacketOutputStream os) {
        if (escapeValueUtf8 == null) escapeForText();
        os.writeUnsafe(ParameterWriter.QUOTE);
        os.writeUnsafe(escapeValueUtf8, 0, endPos);
        os.writeUnsafe(ParameterWriter.QUOTE);
    }

    public long getApproximateTextProtocolLength() {
        escapeForText();
        return endPos + 2;
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
            if (endPos > 1024) {
                return "'" + new String(escapeValueUtf8, 0, 1024) + "...'";
            } else {
                return "'" + new String(escapeValueUtf8, 0, endPos) + "'";
            }
        }
    }

    private void escapeForText() {
        char[] chars = string.toCharArray();
        string = null;
        int charLength = chars.length;
        escapeValueUtf8 = new byte[charLength * 3];
        endPos = ParameterWriter.encodeUtf8Escaped(chars, 0, charLength, escapeValueUtf8, noBackslashEscapes);
    }
}
