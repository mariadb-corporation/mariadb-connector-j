package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;

import java.io.IOException;
import java.io.InputStream;

/**
 .
 * User: marcuse
 * Date: Jan 21, 2009
 * Time: 10:40:03 PM

 */
public class ResultSetPacket extends ResultPacket {
    private final long fieldCount;
    private final byte packetSeq;
    public ResultSetPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeq = reader.getPacketSeq();
        fieldCount = reader.getLengthEncodedBinary();
    }

    public ResultType getResultType() {
        return ResultPacket.ResultType.RESULTSET;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }

    public long getFieldCount() {
        return fieldCount;
    }
}
