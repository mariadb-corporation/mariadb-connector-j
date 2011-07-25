/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc.internal.mysql.packet;

import org.skysql.jdbc.internal.common.ServerStatus;
import org.skysql.jdbc.internal.common.Utils;
import org.skysql.jdbc.internal.common.packet.buffer.Reader;
import org.skysql.jdbc.internal.common.packet.RawPacket;
import org.skysql.jdbc.internal.mysql.MySQLServerCapabilities;

import java.io.IOException;
import java.util.Set;

/**
 * Greeting from drizzle looks like this User: marcuse Date: Jan 15, 2009 Time: 3:18:11 PM
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

    public MySQLGreetingReadPacket(final RawPacket rawPacket) throws IOException {
        final Reader reader = new Reader(rawPacket);
        protocolVersion = reader.readByte();
        serverVersion = reader.readString("ASCII");
        serverThreadID = reader.readInt();
        final byte[] seed1 = reader.readRawBytes(8);
        reader.skipByte();
        serverCapabilities = MySQLServerCapabilities.getServerCapabilitiesSet(reader.readShort());
        serverLanguage = reader.readByte();
        serverStatus = ServerStatus.getServerStatusSet(reader.readShort());
        reader.skipBytes(13);
        final byte[] seed2 = reader.readRawBytes(12);
        seed = Utils.copyWithLength(seed1, seed1.length + seed2.length);
        System.arraycopy(seed2, 0, seed, seed1.length, seed2.length);
        reader.readByte(); // seems the seed is null terminated
    }

    @Override
    public String toString() {
        return protocolVersion + ":" +
                serverVersion + ":" +
                serverThreadID + ":" +
                new String(seed) + ":" +
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