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
import java.io.InputStream;

/**
 * .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:54 PM
 */
public class EOFPacket extends ResultPacket {
    private final byte packetSeq;
    private final short warningCount;
    private short statusFlags;

    public EOFPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeq = reader.getPacketSeq();
        byte packetType = reader.readByte();
        if (packetType != (byte) 0xfe)
            throw new IOException("Could not create EOF packet");
        warningCount = reader.readShort();
        reader.readShort();
    }

    public EOFPacket(byte[] rawBytes) {
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
}
