/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet;

import org.drizzle.jdbc.internal.common.ServerStatus;
import org.drizzle.jdbc.internal.common.packet.buffer.LengthEncodedBinary;
import org.drizzle.jdbc.internal.common.packet.buffer.LengthEncodedBytes;
import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:40 PM
 */
public class OKPacket extends ResultPacket {
    private final byte fieldCount;
    private final long affectedRows;
    private final long insertId;
    private final Set<ServerStatus> serverStatus;
    private final short warnings;
    private final String message;
    private final byte packetSeqNum;

    public OKPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeqNum = reader.getPacketSeq();
        fieldCount = reader.readByte();
        affectedRows = reader.getLengthEncodedBinary();
        insertId = reader.getLengthEncodedBinary();
        serverStatus = ServerStatus.getServerStatusSet(reader.readShort());
        warnings = reader.readShort();
        message = reader.readString("ASCII");
    }

    public OKPacket(byte[] rawBytes) {
        int readBytes = 0;
        packetSeqNum = /*rawBytes[readBytes++];*/ 0;
        fieldCount = rawBytes[readBytes++];
        LengthEncodedBinary leb = ReadUtil.getLengthEncodedBinary(rawBytes, readBytes);
        affectedRows = leb.getValue();
        readBytes += leb.getLength();
        leb = ReadUtil.getLengthEncodedBinary(rawBytes, readBytes);
        insertId = leb.getValue();
        readBytes += leb.getLength();
        serverStatus = ServerStatus.getServerStatusSet(ReadUtil.readShort(rawBytes, readBytes));
        readBytes += 2;
        warnings = ReadUtil.readShort(rawBytes, readBytes);
        readBytes += 2;
        LengthEncodedBytes lebytes = new LengthEncodedBytes(rawBytes, readBytes);
        if (lebytes.getLength() > 0)
            message = new String(lebytes.getBytes());
        else
            message = "";
    }

    public ResultType getResultType() {
        return ResultType.OK;
    }

    public byte getPacketSeq() {
        return packetSeqNum;
    }

    @Override
    public String toString() {
        return "affectedRows = " + affectedRows + "&insertId = " + insertId + "&serverStatus=" + serverStatus + "&warnings=" + warnings + "&message=" + message;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getInsertId() {
        return insertId;
    }

    public Set<ServerStatus> getServerStatus() {
        return serverStatus;
    }

    public short getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }
}
