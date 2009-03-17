package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 14, 2009
 * Time: 10:14:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class PingPacket implements DrizzlePacket {
    private final WriteBuffer buffer = new WriteBuffer();
    public PingPacket() {
        buffer.writeByte((byte)12);
    }
    public byte [] toBytes(byte commandNumber){
        return buffer.toByteArrayWithLength(commandNumber);
    }
}
