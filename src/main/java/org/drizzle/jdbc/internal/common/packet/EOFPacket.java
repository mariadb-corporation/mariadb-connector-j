/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet;

import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 4:23:54 PM
 */
public class EOFPacket extends ResultPacket {
    private final byte packetSeq;
    private final short warningCount;
    private final short statusFlags;


    public EOFPacket(final byte[] rawBytes) {
        super();
        packetSeq = 0;
        warningCount = ReadUtil.readShort(rawBytes, 1);
        statusFlags = ReadUtil.readShort(rawBytes, 3);
    }

    public ResultType getResultType() {
        return ResultType.EOF;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }

    public short getWarningCount() {
        return warningCount;
    }

    public short getStatusFlags() {
        return statusFlags;
    }
}
