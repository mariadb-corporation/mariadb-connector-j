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

package org.mariadb.jdbc.internal.common.query.parameters;

import org.mariadb.jdbc.internal.common.packet.PacketOutputStream;
import org.mariadb.jdbc.internal.mysql.MariaDbType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;


public class StreamParameter extends LongDataParameterHolder {
    InputStream is;
    long length;
    boolean noBackslashEscapes;
    ArrayList<byte[]> readArrays = null;

    /**
     * Constructor.
     * @param is stream to write
     * @param length max length to write (if null the whole stream will be send)
     * @param noBackslashEscapes must backslash be escape
     */
    public StreamParameter(InputStream is, long length, boolean noBackslashEscapes) {
        this.is = is;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public StreamParameter(InputStream is, boolean noBackSlashEscapes) {
        this(is, Long.MAX_VALUE, noBackSlashEscapes);
    }

    /**
     * Write stream in text format.
     * @param os database outputStream
     * @throws IOException if any error occur when reader stream
     */
    public void writeTo(final OutputStream os) throws IOException {
        if (readArrays != null) {
            ParameterWriter.writeBytesArray(os, readArrays, noBackslashEscapes);
        } else {
            if (length == Long.MAX_VALUE) {
                ParameterWriter.write(os, is, noBackslashEscapes);
            } else {
                ParameterWriter.write(os, is, length, noBackslashEscapes);
            }
        }
    }

    /**
     * Return approximated data calculated length.
     *
     * @return approximated data length.
     * @throws IOException if error reading stream
     */
    public long getApproximateTextProtocolLength() throws IOException {
        if (length == Long.MAX_VALUE) {
            readArrays = new ArrayList<>();
            int length = 0;
            int len;
            byte[] buffer = new byte[1024];
            while ((len = is.read(buffer)) >= 0) {
                readArrays.add(buffer);
                buffer = new byte[1024];
                length += len;
            }
            is = null;
            //length * 2 due to escape done after
            return 2 + length * 2;

        } else {
            return length;
        }
    }

    /**
     * Write stream in binary format.
     * @param os database outputStream
     * @throws IOException if any error occur when reader stream
     */
    public void writeBinary(PacketOutputStream os) throws IOException {
        if (length == Long.MAX_VALUE) {
            os.sendStream(is);
        } else {
            os.sendStream(is, length);
        }

    }


    public String toString() {
        return "<Stream> " + is;
    }

    public MariaDbType getMariaDbType() {
        return MariaDbType.BLOB;
    }

}