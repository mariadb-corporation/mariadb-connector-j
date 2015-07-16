 /*
*/

package org.mariadb.jdbc.internal.common.packet;

import org.mariadb.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;


/**
 * Class to represent a more packet as transferred over the wire. First we got 3 bytes specifying the actual length, then
 * one byte packet sequence number and then n bytes with user data.
 */
public final class MoreDataPacket extends ResultPacket {
    private final byte[] data;
    private final byte seq;

    public MoreDataPacket(final RawPacket rawPacket) throws IOException {
        seq = (byte) rawPacket.getPacketSeq();
        Reader reader = new Reader(rawPacket);
        reader.skipByte();
        data = reader.readRawBytes();
    }

    public ResultType getResultType() {
        return ResultType.MOREDATA;
    }

    public byte[] getData() {
        return data;
    }

    public byte getPacketSeq() {
        return seq;
    }
}
