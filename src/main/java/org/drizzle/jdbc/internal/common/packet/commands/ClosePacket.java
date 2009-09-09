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
        this.writeBuffer = new WriteBuffer();
        writeBuffer.writeByte((byte) 0x01);
    }

    public void send(final OutputStream os) throws IOException {
        final byte[] buff = writeBuffer.toByteArrayWithLength((byte) 0);
        for (final byte b : buff) {
            os.write(b);
        }
        os.flush();
    }
}
