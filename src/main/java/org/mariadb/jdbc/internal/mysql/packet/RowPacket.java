package org.mariadb.jdbc.internal.mysql.packet;

import org.mariadb.jdbc.internal.common.PacketFetcher;
import org.mariadb.jdbc.internal.common.ValueObject;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by diego_000 on 08/10/2015.
 */
public interface RowPacket {
    ValueObject[] getRow(PacketFetcher packetFetcher, ByteBuffer buffer) throws IOException;
}
