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

    private String seed;
    private String serverVersion;
    private int protocolVersion;
    private long serverThreadID;
    private byte[] seed1 = new byte[8];
    private byte[] seed2 = new byte[13];
    private int serverCapabilities  ;
    private byte serverLanguage;
    private int serverStatus;
    private ReadBuffer readBuffer;

    public GreetingReadPacket(InputStream reader) throws IOException {
        readBuffer=new ReadBuffer(reader);
        protocolVersion = readBuffer.readByte();
        serverVersion = readBuffer.readString("ASCII");
        serverThreadID = readBuffer.readLong();
        seed1 = readBuffer.readRawBytes(8);
        readBuffer.skipByte();
        serverCapabilities = readBuffer.readInt();
        serverLanguage = readBuffer.readByte();
        serverStatus = readBuffer.readInt();
        readBuffer.skipBytes(13);
        seed2=readBuffer.readRawBytes(13);
/*        System.out.println("PROTOCOL_VERSION: "+protocolVersion);
        System.out.println("SERVER_VERSION: "+ serverVersion);
        System.out.println("SERVER_TID: "+serverThreadID);
        System.out.println("SERVER_CAPABILITIES: "+(serverCapabilities & 32));
        System.out.println("SERVER_LANGUAGE: "+serverLanguage);
        System.out.println("SERVER_STATUS: "+serverStatus);*/
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

    public String getSeed() {
        return seed;
    }

    public void setSeed(String seed) {
        this.seed = seed;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public long getServerThreadID() {
        return serverThreadID;
    }

    public void setServerThreadID(int serverThreadID) {
        this.serverThreadID = serverThreadID;
    }

    public byte[] getSeed1() {
        return seed1;
    }

    public void setSeed1(byte[] seed1) {
        this.seed1 = seed1;
    }

    public int getServerCapabilities() {
        return serverCapabilities;
    }

    public void setServerCapabilities(int serverCapabilities) {
        this.serverCapabilities = serverCapabilities;
    }

    public byte getServerLanguage() {
        return serverLanguage;
    }

    public void setServerLanguage(byte serverLanguage) {
        this.serverLanguage = serverLanguage;
    }

    public int getServerStatus() {
        return serverStatus;
    }

    public void setServerStatus(int serverStatus) {
        this.serverStatus = serverStatus;
    }

    public byte[] getSeed2() {
        return seed2;
    }

    public void setSeed2(byte[] seed2) {
        this.seed2 = seed2;
    }
}
