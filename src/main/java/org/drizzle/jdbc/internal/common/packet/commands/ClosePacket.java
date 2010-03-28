/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet.commands;

import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * . User: marcuse Date: Feb 10, 2009 Time: 9:40:08 PM
 */
public class ClosePacket implements CommandPacket {
    private final WriteBuffer writeBuffer;

    public ClosePacket() {
        this.writeBuffer = new WriteBuffer(6);
        writeBuffer.writeByte((byte) 0x01);
    }

    public void send(final OutputStream os) throws IOException {
        os.write(writeBuffer.getLengthWithPacketSeq((byte) 0));
        os.write(writeBuffer.getBuffer(),0,writeBuffer.getLength());
        os.flush();
    }
}
