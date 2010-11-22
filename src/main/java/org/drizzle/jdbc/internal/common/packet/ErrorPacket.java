/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet;

import org.drizzle.jdbc.internal.common.packet.buffer.Reader;

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
