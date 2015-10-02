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

package org.mariadb.jdbc.internal.common.packet;


import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ResultPacket {
    ByteBuffer byteBuffer;

    public ResultPacket(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public abstract ResultType getResultType();

    public enum ResultType {
        OK, ERROR, EOF, RESULTSET, LOCALINFILE, FIELD
    }


    public long getLengthEncodedBinary() {
        /*if (byteBuffer.remaining() == 0) {
            return 0;
        }*/
        final byte type = byteBuffer.get();

        if ((type & 0xff) == 251) {
            return -1;
        }
        if ((type & 0xff) == 252) {
            return (long) 0xffff & byteBuffer.getShort();
        }
        if ((type & 0xff) == 253) {
            return 0xffffff & read24bitword();
        }
        if ((type & 0xff) == 254) {
            return byteBuffer.getLong();
        }
        if ((type & 0xff) <= 250) {
            return (long) 0xff & type;
        }

        return 0;
    }

    public byte[] getLengthEncodedBytes() throws IOException {
        if (byteBuffer.remaining() == 0) return new byte[0];
        final long encLength = getLengthEncodedBinary();
        if (encLength == -1) {
            return null;
        }
        final byte[] tmpBuf = new byte[(int) encLength];
        byteBuffer.get(tmpBuf);
        return tmpBuf;
    }

    public String getStringLengthEncodedBytes() throws IOException {
        if (byteBuffer.remaining() == 0) return null;
        final long encLength = getLengthEncodedBinary();
        if (encLength == 0) return "";
        if (encLength != -1) {
            final byte[] tmpBuf = new byte[(int) encLength];
            byteBuffer.get(tmpBuf);
            return new String(tmpBuf);
        }
        return null;
    }

    public short readShort() {
        return byteBuffer.getShort();
    }


    public int read24bitword() {
        final byte[] tmpArr = new byte[3];
        byteBuffer.get(tmpArr);
        return (tmpArr[0] & 0xff) + ((tmpArr[1] & 0xff) << 8) + ((tmpArr[2] & 0xff) << 16);
    }

    public long readLong() {
        return byteBuffer.getLong();
    }
}
