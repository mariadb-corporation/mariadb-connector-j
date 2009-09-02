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
 * .
 * User: marcuse
 * Date: Jan 20, 2009
 * Time: 10:50:47 PM
 */
public class SelectDBPacket implements CommandPacket {
    private final WriteBuffer buffer = new WriteBuffer();

    public SelectDBPacket(final String database) {
        buffer.writeByte((byte) 0x02);
        buffer.writeString(database);
    }

    public void send(final OutputStream outputStream) throws IOException {
        final byte[] buff = buffer.toByteArrayWithLength((byte) 0);
        outputStream.write(buff);
//        for(byte b: buff)
//            outputStream.write(b);
        outputStream.flush();
    }
}