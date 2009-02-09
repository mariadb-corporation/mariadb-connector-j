package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadBuffer;

import java.io.InputStream;
import java.io.IOException;

/**
 * Greeting from drizzle looks like this
 * User: marcuse
 * Date: Jan 15, 2009
 * Time: 3:18:11 PM
 */
public class GreetingReadPacket {

    private final String serverVersion;
    private final byte protocolVersion;
    private final long serverThreadID;
    private byte[] seed1 = new byte[8];
    private byte[] seed2 = new byte[13];
    private final short serverCapabilities  ;
    private final byte serverLanguage;
    private final short serverStatus;
    private final ReadBuffer readBuffer;

    public GreetingReadPacket(InputStream reader) throws IOException {
        readBuffer=new ReadBuffer(reader);
        protocolVersion = readBuffer.readByte();
        serverVersion = readBuffer.readString("ASCII");
        serverThreadID = readBuffer.readInt();
        seed1 = readBuffer.readRawBytes(8);
        readBuffer.skipByte();
        serverCapabilities = readBuffer.readShort();
        serverLanguage = readBuffer.readByte();
        serverStatus = readBuffer.readShort();
        readBuffer.skipBytes(13);
        seed2=readBuffer.readRawBytes(13);
    }
    @Override
    public String toString(){
        return protocolVersion+":"+
                serverVersion+":"+
                serverThreadID+":"+
                seed1+":"+
                serverCapabilities+":"+
                serverLanguage+":"+
                serverStatus;
    }


    public String getServerVersion() {
        return serverVersion;
    }


    public byte getProtocolVersion() {
        return protocolVersion;
    }


    public long getServerThreadID() {
        return serverThreadID;
    }



    public byte[] getSeed1() {
        return seed1;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }



    public byte getServerLanguage() {
        return serverLanguage;
    }



    public int getServerStatus() {
        return serverStatus;
    }


    public byte[] getSeed2() {
        return seed2;
    }

}
