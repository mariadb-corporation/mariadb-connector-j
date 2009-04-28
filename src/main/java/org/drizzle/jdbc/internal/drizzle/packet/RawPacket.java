/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

import java.io.IOException;
import java.io.InputStream;

/**
 * User: marcuse
 * Date: Mar 31, 2009
 * Time: 2:06:47 PM
 */
public class RawPacket {
    private final byte [] rawBytes;

    public RawPacket(InputStream is) throws IOException {
        int length = this.readLength(is);
        int packetSeq = this.readPacketSeq(is);
        this.rawBytes = new byte[length];
        for(int i=0;i<length;i++)
            rawBytes[i]= (byte) is.read();
    }
    public RawPacket() {
        this.rawBytes=null;
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
}
