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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Apr 1, 2009 Time: 8:26:44 AM To change this template use File |
 * Settings | File Templates.
 */
public class SyncPacketFetcher implements PacketFetcher {

    private final InputStream inputStream;

    public SyncPacketFetcher(final InputStream is) {
        this.inputStream = new BufferedInputStream(is);
    }

    public RawPacket getRawPacket() throws IOException {
        return RawPacket.nextPacket(inputStream);
    }

    public void close() throws IOException {
        inputStream.close();
    }
}
