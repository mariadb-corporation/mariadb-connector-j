package org.mariadb.jdbc.internal.common.packet;


import org.mariadb.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;

public class LocalInfilePacket extends ResultPacket {
    private long fieldCount;
    private String fileName;

    public LocalInfilePacket(RawPacket rawPacket) throws IOException {
        Reader reader = new Reader(rawPacket);
        fieldCount = reader.getLengthEncodedBinary();
        if (fieldCount != -1)
            throw new AssertionError("field count must be -1");
        fileName = reader.readString("UTF-8");
    }

    public String getFileName() {
        return fileName;
    }

    public String toString() {
        return fileName;
    }

    public ResultType getResultType() {
        return ResultType.LOCALINFILE;
    }

    public byte getPacketSeq() {
        return 0;
    }
}
