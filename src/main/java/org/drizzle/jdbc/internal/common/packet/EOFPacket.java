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
import org.drizzle.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 4:23:54 PM
 */
public class EOFPacket extends ResultPacket {
    private final byte packetSeq;
    private final short warningCount;
    private final short statusFlags;


    public EOFPacket(final RawPacket rawPacket) throws IOException {
        final Reader reader = new Reader(rawPacket);
        packetSeq = 0;
        reader.readByte();
        warningCount = reader.readShort();
        statusFlags = reader.readShort();
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
