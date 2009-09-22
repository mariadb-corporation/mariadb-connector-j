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
import org.drizzle.jdbc.internal.common.packet.buffer.Reader;

import java.util.Set;
import java.io.IOException;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 4:23:40 PM
 */
public class OKPacket extends ResultPacket {
    private final byte fieldCount;
    private final long affectedRows;
    private final long insertId;
    private final Set<ServerStatus> serverStatus;
    private final short warnings;
    private final String message;
    private final byte packetSeqNum;


    public OKPacket(final RawPacket rawPacket) throws IOException {
        final Reader reader = new Reader(rawPacket);
        packetSeqNum = 0;
        fieldCount = reader.readByte();
        affectedRows = reader.getLengthEncodedBinary();
        insertId = reader.getLengthEncodedBinary();
        serverStatus = ServerStatus.getServerStatusSet(reader.readShort());
        warnings = reader.readShort();
        message = new String(reader.getLengthEncodedBytes());
    }

    public ResultType getResultType() {
        return ResultType.OK;
    }

    public byte getPacketSeq() {
        return packetSeqNum;
    }

    @Override
    public String toString() {
        return "affectedRows = " +
                affectedRows +
                "&insertId = " +
                insertId +
                "&serverStatus=" +
                serverStatus +
                "&warnings=" +
                warnings +
                "&message=" +
                message;
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
