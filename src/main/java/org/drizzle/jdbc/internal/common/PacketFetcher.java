package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.drizzle.packet.RawPacket;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Apr 1, 2009
 * Time: 8:26:16 AM
 * To change this template use File | Settings | File Templates.
 */
public interface PacketFetcher {
    RawPacket getRawPacket();
    void close() throws IOException;
}
