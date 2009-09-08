/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.common.packet.RawPacket;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Apr 1, 2009 Time: 8:26:16 AM To change this template use File |
 * Settings | File Templates.
 */
public interface PacketFetcher {
    RawPacket getRawPacket() throws IOException;

    void start();

    void close() throws IOException;

    void awaitTermination();
}
