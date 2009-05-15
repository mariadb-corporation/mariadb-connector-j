/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet;

import org.drizzle.jdbc.internal.common.PacketFetcher;
import org.drizzle.jdbc.internal.common.packet.ReadAheadInputStream;

import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Apr 1, 2009
 * Time: 8:26:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class SyncPacketFetcher implements PacketFetcher {
    private InputStream inputStream;

    public SyncPacketFetcher(InputStream is) {
        this.inputStream = new ReadAheadInputStream(is);
    }
    public RawPacket getRawPacket() throws IOException {
        return new RawPacket(inputStream);
    }

    public void close() throws IOException {
        inputStream.close();
    }

    public void awaitTermination() {
    }

}
