package org.drizzle.jdbc.internal.packet.commands;

import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.packet.CommandPacket;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 14, 2009
 * Time: 10:14:13 PM

 */
public class PingPacket implements CommandPacket {
    private final WriteBuffer buffer = new WriteBuffer();
    public PingPacket() {
        buffer.writeByte((byte)6);
    }


    public void send(OutputStream os) throws IOException {
        byte [] buff = buffer.toByteArrayWithLength((byte)0);
        for(byte b:buff)
            os.write(b);
        os.flush();
    }
}
