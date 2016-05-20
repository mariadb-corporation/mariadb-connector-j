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

import java.io.*;
import java.util.ArrayList;


public class ReaderParameter extends LongDataParameterHolder {
    private Reader reader;
    private ArrayList<char[]> readArrays = null;
    private long length;
    private boolean noBackslashEscapes;

    /**
     * Constructor.
     * @param reader reader to write
     * @param length max length to write (can be null)
     * @param noBackslashEscapes must backslash be escape
     */
    public ReaderParameter(Reader reader, long length, boolean noBackslashEscapes) {
        this.reader = reader;
        this.length = length;
        this.noBackslashEscapes = noBackslashEscapes;
    }

    public ReaderParameter(Reader reader, boolean noBackslashEscapes) {
        this(reader, Long.MAX_VALUE, noBackslashEscapes);
    }

    /**
     * Write reader to database in text format.
     *
     * @param os database outputStream
     * @throws IOException if any error occur when reading reader
     */
    public void writeTo(final PacketOutputStream os) throws IOException {
        if (readArrays != null) {
            ParameterWriter.write(os, readArrays, noBackslashEscapes);
        } else {
            if (length == Long.MAX_VALUE) {
                ParameterWriter.write(os, reader, noBackslashEscapes);
            } else {
                ParameterWriter.write(os, reader, length, noBackslashEscapes);
            }
        }
    }

    /**
     * Write reader to database in text format without checking buffer size.
     *
     * @param os database outputStream
     * @throws IOException if any error occur when reading reader
     */
    public void writeUnsafeTo(final PacketOutputStream os) throws IOException {
        if (readArrays != null) {
            ParameterWriter.writeUnsafe(os, readArrays, noBackslashEscapes);
        } else {
            if (length == Long.MAX_VALUE) {
                ParameterWriter.writeUnsafe(os, reader, noBackslashEscapes);
            } else {
                ParameterWriter.write(os, reader, length, noBackslashEscapes);
            }
        }
    }

    /**
     * Return approximated data calculated length for rewriting queries
     *
     * @return approximated data length.
     * @throws IOException if error reading stream
     */
    public long getApproximateTextProtocolLength() throws IOException {
        if (length == Long.MAX_VALUE) {
            readArrays = new ArrayList<>();
            int length = 0;
            int len;
            char[] buffer = new char[1024];
            while ((len = reader.read(buffer)) >= 0) {
                readArrays.add(buffer);
                buffer = new char[1024];
                length += len;
            }
            reader = null;
            //length * 2 due to escape done after
            return 2 + length * 2;

        } else {
            return length;
        }
    }


    /**
     * Write reader to database in binary format.
     * @param os database outputStream
     * @throws IOException if any error occur when reading reader
     */
    public void writeBinary(final PacketOutputStream os) throws IOException {
        if (length == Long.MAX_VALUE) {
            os.sendStream(reader);
        } else {
            os.sendStream(reader, length);
        }
    }

    public MariaDbType getMariaDbType() {
        return MariaDbType.BLOB;
    }


    public String toString() {
        return "<Buffer> " + reader;
    }

    public boolean isLongData() {
        return true;
    }


}
