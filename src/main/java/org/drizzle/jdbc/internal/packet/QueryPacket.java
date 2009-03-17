package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.packet.DrizzlePacket;


/**
 * User: marcuse
 * Date: Jan 19, 2009
 * Time: 10:14:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryPacket implements DrizzlePacket {
    private final WriteBuffer buffer;
    public QueryPacket(String query) {
        buffer = new WriteBuffer();
        buffer.writeByte((byte)0x03).
            writeString(query);
    }
    public byte [] toBytes(byte queryNumber) {
        return buffer.toByteArrayWithLength(queryNumber);
    }
}