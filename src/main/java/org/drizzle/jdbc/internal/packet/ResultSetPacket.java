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

    public ResultSetPacket(byte [] rawBytes) {
        packetSeq = rawBytes[0];
        byte leBinLength = ReadUtil.getLengthEncodedByteLength(rawBytes,1);
        switch(leBinLength) {
            case -1:
                System.out.println("a");
                fieldCount = 0;
                break;
            case 0:
                System.out.println("b");
                fieldCount = rawBytes[0];
                break;
            case 2:
                System.out.println("c");

                fieldCount = ReadUtil.readShort(rawBytes,2);
                break;
            case 3:
                System.out.println("d");

                fieldCount = ReadUtil.read24bitword(rawBytes,2);
                break;
            case 8:
                System.out.println("e");

                fieldCount = ReadUtil.readLong(rawBytes,2);
                break;
            default:
                System.out.println("f");

                fieldCount=0;
        }
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
