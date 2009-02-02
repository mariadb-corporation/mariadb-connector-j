package org.drizzle.jdbc.packet;

import org.drizzle.jdbc.packet.buffer.ReadBuffer;

import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class OKPacket extends ResultPacket {
    private final byte fieldCount;
    private final long affectedRows;
    private final long insertId;
    private final int serverStatus;
    private final int warnings;
    private final String message;
    private final byte packetSeqNum;

    public OKPacket(ReadBuffer readBuffer) throws IOException {
        packetSeqNum = readBuffer.getPacketSeq();
        fieldCount = readBuffer.readByte();
        affectedRows = readBuffer.getLengthEncodedBinary();
        insertId = readBuffer.getLengthEncodedBinary();
        serverStatus = readBuffer.readInt();
        warnings = readBuffer.readInt();
        message = readBuffer.readString("ASCII");
    }

    public ResultType getResultType() {
        return ResultType.OK;
    }

    public byte getPacketSeq() {
        return packetSeqNum;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String toString() {
        return "affectedRows = "+affectedRows+"&insertId = "+insertId+"&serverStatus="+serverStatus+"&warnings="+warnings+"&message="+message;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getInsertId() {
        return insertId;
    }

    public int getServerStatus() {
        return serverStatus;
    }

    public int getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }
}
