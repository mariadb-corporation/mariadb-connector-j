package org.drizzle.jdbc.packet;

import org.drizzle.jdbc.packet.buffer.ReadBuffer;

import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:20:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class ErrorPacket extends ResultPacket {
    private byte fieldCount;
    private int errorNumber;
    private byte sqlStateMarker;
    private byte[] sqlState;
    private String message;

    public ErrorPacket(ReadBuffer readBuffer) throws IOException {
        this.fieldCount = readBuffer.readByte();
        this.errorNumber = readBuffer.readInt();
        this.sqlStateMarker = readBuffer.readByte();
        this.sqlState = readBuffer.readRawBytes(5);
        this.message= readBuffer.readString("ASCII");
        System.out.println(message);
        System.out.println(errorNumber);
        System.out.println(fieldCount);
    }

    public ResultType getResultType() {
        return ResultType.ERROR;
    }
}
