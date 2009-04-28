/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet.commands;

import org.drizzle.jdbc.internal.drizzle.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.drizzle.packet.CommandPacket;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Jan 20, 2009
 * Time: 10:50:47 PM

 */
public class SelectDBPacket implements CommandPacket {
    private final WriteBuffer buffer = new WriteBuffer();
    public SelectDBPacket(String database) {
        buffer.writeByte((byte)0x02);
        buffer.writeString(database);
    }

    public void send(OutputStream os) throws IOException {
        byte [] buff = buffer.toByteArrayWithLength((byte) 0);
        os.write(buff);
//        for(byte b: buff)
//            os.write(b);
        os.flush();
    }
}