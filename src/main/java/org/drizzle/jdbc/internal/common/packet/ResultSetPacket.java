/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet;

import org.drizzle.jdbc.internal.common.packet.buffer.LengthEncodedBinary;
import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;

/**
 * . User: marcuse Date: Jan 21, 2009 Time: 10:40:03 PM
 */
public class ResultSetPacket extends ResultPacket {
    private final long fieldCount;

    public ResultSetPacket(final byte[] rawBytes) {
        super();
        final LengthEncodedBinary leb = ReadUtil.getLengthEncodedBinary(rawBytes, 0);
        fieldCount = leb.getValue();
    }

    public ResultType getResultType() {
        return ResultPacket.ResultType.RESULTSET;
    }

    public byte getPacketSeq() {
        return 0;
    }

    public long getFieldCount() {
        return fieldCount;
    }
}
