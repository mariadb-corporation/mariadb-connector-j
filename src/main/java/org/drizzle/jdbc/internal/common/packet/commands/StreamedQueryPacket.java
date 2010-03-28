/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet.commands;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import static org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer.intToByteArray;
import org.drizzle.jdbc.internal.common.query.Query;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;


/**
 * User: marcuse Date: Jan 19, 2009 Time: 10:14:32 PM
 */
public class StreamedQueryPacket implements CommandPacket {
    private final Query query;


    public StreamedQueryPacket(final Query query) {
        this.query = query;

    }

    public void send(final OutputStream ostream) throws IOException, QueryException {
        byte[] byteHeader = Arrays.copyOf(intToByteArray(query.length() + 1), 5);
        byteHeader[3] = (byte) 0;
        byteHeader[4] = (byte) 0x03;
        ostream.write(byteHeader);
        query.writeTo(ostream);
        ostream.flush();
    }
}