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

import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;


public class SerializableParameter implements ParameterHolder {
    private Object object;
    private boolean noBackSlashEscapes;
    private byte[] loadedStream = null;

    public SerializableParameter(Object object, boolean noBackslashEscapes) throws IOException {
        this.object = object;
        this.noBackSlashEscapes = noBackslashEscapes;
    }

    /**
     * Write object to buffer for text protocol.
     *
     * @param os the stream to write to
     * @throws IOException if error reading stream
     */
    public void writeTo(final PacketOutputStream os) throws IOException {
        if (loadedStream == null) writeObjectToBytes();
        ParameterWriter.write(os, loadedStream, noBackSlashEscapes);
    }

    /**
     * Write object to buffer for text protocol without checking buffer size.
     *
     * @param os the stream to write to
     * @throws IOException if error reading stream
     */
    public void writeUnsafeTo(final PacketOutputStream os) throws IOException {
        if (loadedStream == null) writeObjectToBytes();
        ParameterWriter.writeUnsafe(os, loadedStream, noBackSlashEscapes);
    }

    private void writeObjectToBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        loadedStream = baos.toByteArray();
        object = null;
    }

    /**
     * Return approximated data calculated length.
     *
     * @return approximated data length.
     * @throws IOException if error reading stream
     */
    public long getApproximateTextProtocolLength() throws IOException {
        writeObjectToBytes();
        return loadedStream.length;
    }


    /**
     * Write data in binary format to buffer.
     *
     * @param os buffer
     * @throws IOException exception
     */
    public void writeBinary(final PacketOutputStream os) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        os.write(baos.toByteArray());
    }

    @Override
    public String toString() {
        if (loadedStream != null) {
            return "<Serializable:" + new String(loadedStream) + ">";
        } else {
            return "<Serializable:" + object.toString() + ">";
        }
    }

    public MariaDbType getMariaDbType() {
        return MariaDbType.BLOB;
    }

    public boolean isLongData() {
        return true;
    }

    public boolean isNullData() {
        return false;
    }

}