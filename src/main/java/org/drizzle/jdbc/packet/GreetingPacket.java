package org.drizzle.jdbc.packet;

import java.io.InputStream;
import java.io.IOException;

/**
 * Greeting from drizzle looks like this
 * [3 bytes length][1 byte 0][1 byte protocolVersion][String serverVersion][1 byte 0][String seed] 
 * User: marcuse
 * Date: Jan 15, 2009
 * Time: 3:18:11 PM
 */
public class GreetingPacket extends AbstractPacket {
    private String seed;
    private String serverVersion;
    private int protocolVersion;
    private int serverThreadID;
    public GreetingPacket(InputStream reader) throws IOException {
        super(reader);
        protocolVersion = readByte();
        serverVersion = readString("ASCII");
        serverThreadID = readInt();
        seed = readString("ASCII");
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

    public int getServerThreadID() {
        return serverThreadID;
    }

    public void setServerThreadID(int serverThreadID) {
        this.serverThreadID = serverThreadID;
    }
}
