package org.drizzle.jdbc.packet;

import org.drizzle.jdbc.packet.buffer.ReadBuffer;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

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
    public ResultSetPacket(ReadBuffer readBuffer) throws IOException {
        packetSeq = readBuffer.getPacketSeq();
        fieldCount = readBuffer.getLengthEncodedBinary();
        extra = readBuffer.getLengthEncodedBinary();

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
