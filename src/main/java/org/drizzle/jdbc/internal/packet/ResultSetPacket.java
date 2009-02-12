package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 21, 2009
 * Time: 10:40:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultSetPacket extends ResultPacket {
    private long fieldCount;
    private long extra;
    private byte packetSeq;
    public ResultSetPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeq = reader.getPacketSeq();
        fieldCount = reader.getLengthEncodedBinary();
    }

    public ResultType getResultType() {
        return ResultPacket.ResultType.RESULTSET;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public byte getPacketSeq() {
        return packetSeq;
    }

    public long getFieldCount() {
        return fieldCount;
    }
}
