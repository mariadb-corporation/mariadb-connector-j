/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet.commands;

import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * . User: marcuse Date: Feb 14, 2009 Time: 10:14:13 PM
 */
public class PingPacket implements CommandPacket {
    private final WriteBuffer buffer = new WriteBuffer();

    public PingPacket() {
        buffer.writeByte((byte) 6);
    }


    public void send(final OutputStream os) throws IOException {
        final byte[] buff = buffer.toByteArrayWithLength((byte) 0);
        for (final byte b : buff) {
            os.write(b);
        }
        os.flush();
    }
}
