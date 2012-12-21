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

import org.mariadb.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;


public class ErrorPacket extends ResultPacket {
    private final short errorNumber;
    private final byte sqlStateMarker;
    private final byte[] sqlState;
    private final String message;


    public ErrorPacket(final RawPacket rawPacket) throws IOException {
        final Reader reader = new Reader(rawPacket);
        reader.readByte();
        this.errorNumber = reader.readShort();
        this.sqlStateMarker = reader.readByte();
        if (sqlStateMarker == '#') {
            this.sqlState = reader.readRawBytes(5);
            this.message = reader.readString("UTF-8");
        }
        else {
            // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
            byte[] msgBuf = new byte[reader.getRemainingSize()+1];
            msgBuf[0] = sqlStateMarker;
            int cnt = 1;
            while(reader.getRemainingSize() > 0) {
                byte b = reader.readByte();
                if(b == 0)
                    break;
                msgBuf[cnt++] = b;
            }

            this.message = new String(msgBuf, "UTF-8");
            this.sqlState = "HY000".getBytes("UTF-8");
        }
    }

    public String getMessage() {
        return message;
    }

    public ResultType getResultType() {
        return ResultType.ERROR;
    }

    public byte getPacketSeq() {
        return 0;
        //return packetSeq;
    }

    public short getErrorNumber() {
        return errorNumber;
    }

    public String getSqlState() {
        return new String(sqlState);
    }

    public byte getSqlStateMarker() {
        return sqlStateMarker;
    }
}
