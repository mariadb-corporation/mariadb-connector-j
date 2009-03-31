package org.drizzle.jdbc.internal.packet;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 31, 2009
 * Time: 2:06:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class RawPacket {
    private final byte [] rawBytes;
    private final int packetId;

    public RawPacket(InputStream is, int packetId) throws IOException {
        int length = this.readLength(is);
        int packetSeq = this.readPacketSeq(is);
        this.rawBytes = new byte[length];
        this.packetId=packetId;
        for(int i=0;i<length;i++)
            rawBytes[i]= (byte) is.read();
    }

    private  byte readPacketSeq(InputStream reader) throws IOException {
        return (byte)reader.read();
    }
    private int readLength(InputStream reader) throws IOException {
        byte [] lengthBuffer = new byte[3];
        for(int i =0 ;i<3;i++)
            lengthBuffer[i]= (byte) reader.read();
        return  (lengthBuffer[0] & 0xff)+(lengthBuffer[1]<<8) + (lengthBuffer[2]<<16);
    }
    public byte[] getRawBytes() {
        return rawBytes;
    }

    public int getPacketId() {
        return packetId;
    }

    public void debugPacket(){
        System.out.printf("%d: [",packetId);
        for(byte b:rawBytes)
            System.out.printf("0x%x ",b);
        System.out.printf("]\n");
    }
}
