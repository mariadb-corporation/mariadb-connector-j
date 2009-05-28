/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql.packet;

import org.drizzle.jdbc.internal.common.ServerStatus;
import org.drizzle.jdbc.internal.common.packet.buffer.Reader;
import org.drizzle.jdbc.internal.mysql.MySQLServerCapabilities;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Set;

/**
 * Greeting from drizzle looks like this
 * User: marcuse
 * Date: Jan 15, 2009
 * Time: 3:18:11 PM
 */
public class MySQLGreetingReadPacket {
    private final String serverVersion;
    private final byte protocolVersion;
    private final long serverThreadID;
    //private final byte[] seed1;
    //private final byte[] seed2;
    private final Set<MySQLServerCapabilities> serverCapabilities;
    private final byte serverLanguage;
    private final Set<ServerStatus> serverStatus;
    private final byte[] seed;

    public MySQLGreetingReadPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        protocolVersion = reader.readByte();
        serverVersion = reader.readString("ASCII");
        serverThreadID = reader.readInt();
        byte[] seed1 = reader.readRawBytes(8);
        reader.skipByte();
        serverCapabilities = MySQLServerCapabilities.getServerCapabilitiesSet(reader.readShort());
        serverLanguage = reader.readByte();
        serverStatus = ServerStatus.getServerStatusSet(reader.readShort());
        reader.skipBytes(13);
        byte[] seed2 = reader.readRawBytes(12);
        seed = Arrays.copyOf(seed1, seed1.length + seed2.length);
        System.arraycopy(seed2, 0, seed, seed1.length, seed2.length);
        reader.readByte(); // seems the seed is null terminated
    }

    @Override
    public String toString() {
        return protocolVersion + ":" +
                serverVersion + ":" +
                serverThreadID + ":" +
                seed + ":" +
                serverCapabilities + ":" +
                serverLanguage + ":" +
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

    public byte[] getSeed() {
        return seed;
    }

    public Set<MySQLServerCapabilities> getServerCapabilities() {
        return serverCapabilities;
    }

    public byte getServerLanguage() {
        return serverLanguage;
    }

    public Set<ServerStatus> getServerStatus() {
        return serverStatus;
    }
}