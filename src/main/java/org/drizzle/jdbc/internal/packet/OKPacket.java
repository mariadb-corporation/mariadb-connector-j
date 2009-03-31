package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;
import org.drizzle.jdbc.internal.ServerStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:40 PM

 */
public class OKPacket extends ResultPacket {
    private final byte fieldCount;
    private final long affectedRows;
    private final long insertId;
    private final Set<ServerStatus> serverStatus;
    private final short warnings;
    private final String message;
    private final byte packetSeqNum;

    public OKPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        packetSeqNum = reader.getPacketSeq();
        fieldCount = reader.readByte();
        affectedRows = reader.getLengthEncodedBinary();
        insertId = reader.getLengthEncodedBinary();
        serverStatus = ServerStatus.getServerStatusSet(reader.readShort());
        warnings = reader.readShort();
        message = reader.readString("ASCII");
    }
    public OKPacket(byte [] rawBytes) {
        int readBytes = 0;
        packetSeqNum = rawBytes[readBytes++];
        fieldCount = rawBytes[readBytes++];
        long encodedBinaryLength = ReadUtil.getLengthEncodedByteLength(rawBytes,readBytes++);
        affectedRows = ReadUtil.getLengthEncodedBinary(rawBytes,readBytes++);
        readBytes+=encodedBinaryLength;
        encodedBinaryLength = ReadUtil.getLengthEncodedByteLength(rawBytes, readBytes);
        readBytes++;
        insertId = ReadUtil.getLengthEncodedBinary(rawBytes, readBytes);
        readBytes+=encodedBinaryLength;
        serverStatus = ServerStatus.getServerStatusSet(ReadUtil.readShort(rawBytes,readBytes));
        readBytes+=2;
        warnings = ReadUtil.readShort(rawBytes,readBytes);
        readBytes+=2;
        if(readBytes < rawBytes.length)
            message = new String(rawBytes, readBytes, rawBytes.length);
        else
            message="";

    }
    public ResultType getResultType() {
        return ResultType.OK;
    }

    public byte getPacketSeq() {
        return packetSeqNum;
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

    public Set<ServerStatus> getServerStatus() {
        return serverStatus;
    }

    public short getWarnings() {
        return warnings;
    }

    public String getMessage() {
        return message;
    }
}
