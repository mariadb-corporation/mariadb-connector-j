/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

import org.drizzle.jdbc.internal.drizzle.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.drizzle.packet.buffer.Reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:20:30 PM

 */
public class ErrorPacket extends ResultPacket {
    private final byte fieldCount;
    private final short errorNumber;
    private final byte sqlStateMarker;
    private final byte[] sqlState;
    private final String message;
    private final byte packetSeq=0;

    public ErrorPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        this.fieldCount = reader.readByte();
        this.errorNumber = reader.readShort();
        this.sqlStateMarker = reader.readByte();
        this.sqlState = reader.readRawBytes(5);
        this.message= reader.readString("ASCII");
    }
    public ErrorPacket(byte[] rawBytes) {
        this.fieldCount = rawBytes[0];
        this.errorNumber = ReadUtil.readShort(rawBytes,1);
        this.sqlStateMarker = rawBytes[3];
        this.sqlState = Arrays.copyOfRange(rawBytes,4,5+4);
        this.message= new String(rawBytes,9,rawBytes.length-9);
    }

    public String getMessage() {
        return message;
    }
    public ResultType getResultType() {
        return ResultType.ERROR;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }
}
