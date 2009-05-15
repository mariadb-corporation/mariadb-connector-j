/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql.packet.commands;

import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;

import java.io.OutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Apr 18, 2009
 * Time: 10:12:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLBinlogDumpPacket implements CommandPacket {
    private final WriteBuffer writeBuffer;
    public MySQLBinlogDumpPacket(int startPos, String filename) {
        writeBuffer = new WriteBuffer();
        writeBuffer.writeByte((byte) 18);
        writeBuffer.writeInt(startPos);
        writeBuffer.writeShort((short) 0);
        writeBuffer.writeInt(0);
        writeBuffer.writeString(filename);
    }

    public void send(OutputStream os) throws IOException {
        byte [] buff = writeBuffer.toByteArrayWithLength((byte)0);
        for(byte b:buff)
            os.write(b);
        os.flush();
    }
}
