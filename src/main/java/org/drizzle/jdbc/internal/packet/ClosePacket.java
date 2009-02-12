package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 10, 2009
 * Time: 9:40:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class ClosePacket implements DrizzlePacket {
    private WriteBuffer writeBuffer;

    public ClosePacket() {
        this.writeBuffer = new WriteBuffer();
        writeBuffer.writeByte((byte)0x01);
    }
    public byte [] toBytes(byte queryNumber){
        return writeBuffer.toByteArrayWithLength(queryNumber);
    }
}
