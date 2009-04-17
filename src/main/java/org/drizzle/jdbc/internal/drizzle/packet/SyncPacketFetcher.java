package org.drizzle.jdbc.internal.drizzle.packet;

import org.drizzle.jdbc.internal.common.PacketFetcher;
import org.drizzle.jdbc.internal.drizzle.packet.ReadAheadInputStream;

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
    public RawPacket getRawPacket() {
        try {
            return new RawPacket(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        inputStream.close();
    }
}
