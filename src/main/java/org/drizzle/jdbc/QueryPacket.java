package org.drizzle.jdbc;

import org.drizzle.jdbc.packet.buffer.WriteBuffer;


/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 19, 2009
 * Time: 10:14:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryPacket {
    WriteBuffer buffer;
    public QueryPacket(String query) {
        buffer = new WriteBuffer();
        buffer.writeByte((byte)0x03);
        buffer.writeString(query);
    }
    public byte [] toBytes() {
        return buffer.toByteArrayWithLength((byte)2);
    }
}