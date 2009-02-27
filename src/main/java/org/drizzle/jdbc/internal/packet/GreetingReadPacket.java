package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.Reader;
import org.drizzle.jdbc.internal.ServerStatus;
import org.drizzle.jdbc.internal.ServerCapabilities;

import java.io.InputStream;
import java.io.IOException;
import java.util.Set;

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
    private final Set<ServerCapabilities> serverCapabilities ;
    private final byte serverLanguage;
    private final Set<ServerStatus> serverStatus;

    public GreetingReadPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        protocolVersion = reader.readByte();
        serverVersion = reader.readString("ASCII");
        serverThreadID = reader.readInt();
        seed1 = reader.readRawBytes(8);
        reader.skipByte();
        serverCapabilities = ServerCapabilities.getServerCapabilitiesSet(reader.readShort());
        serverLanguage = reader.readByte();
        serverStatus = ServerStatus.getServerStatusSet(reader.readShort());
        reader.skipBytes(13);
        seed2=reader.readRawBytes(13);
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

    public Set<ServerCapabilities> getServerCapabilities() {
        return serverCapabilities;
    }



    public byte getServerLanguage() {
        return serverLanguage;
    }



    public Set<ServerStatus> getServerStatus() {
        return serverStatus;
    }


    public byte[] getSeed2() {
        return seed2;
    }

}
