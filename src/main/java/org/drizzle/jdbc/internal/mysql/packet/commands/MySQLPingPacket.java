/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql.packet.commands;

import org.drizzle.jdbc.internal.drizzle.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.drizzle.packet.CommandPacket;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 14, 2009
 * Time: 10:14:13 PM

 */
public class MySQLPingPacket implements CommandPacket {
    private final WriteBuffer buffer = new WriteBuffer();
    public MySQLPingPacket() {
        buffer.writeByte((byte)0x0e);
    }


    public void send(OutputStream os) throws IOException {
        byte [] buff = buffer.toByteArrayWithLength((byte)0);
        for(byte b:buff)
            os.write(b);
        os.flush();
    }
}