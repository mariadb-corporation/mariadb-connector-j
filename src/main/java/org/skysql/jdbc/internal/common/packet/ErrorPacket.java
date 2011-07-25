/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc.internal.common.packet;

import org.skysql.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 4:20:30 PM
 */
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
        this.sqlState = reader.readRawBytes(5);
        this.message = reader.readString("UTF-8");
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
