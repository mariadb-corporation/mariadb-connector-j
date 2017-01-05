/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDB Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson , Stephane Giron

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.packet.send;

import org.mariadb.jdbc.internal.packet.Packet;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.result.ErrorPacket;
import org.mariadb.jdbc.internal.packet.send.gssapi.GssapiAuth;
import org.mariadb.jdbc.internal.packet.send.gssapi.StandardGssapiAuthentication;
import org.mariadb.jdbc.internal.packet.send.gssapi.WindowsNativeSspiAuthentication;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.buffer.Buffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.sql.SQLException;

public class SendGssApiAuthPacket extends AbstractAuthSwitchSendResponsePacket implements InterfaceAuthSwitchSendResponsePacket {
    private ReadPacketFetcher packetFetcher;

    public SendGssApiAuthPacket(ReadPacketFetcher packetFetcher, String password, byte[] authData, int packSeq) {
        super(packSeq, authData, password);
        this.packetFetcher = packetFetcher;
    }

    /**
     * Send native password stream.
     *
     * @param os database socket
     * @throws IOException if a connection error occur
     */
    public void send(OutputStream os) throws IOException, SQLException {
        Buffer buffer = new Buffer(authData);
        final PacketOutputStream writer = (PacketOutputStream) os;
        final String serverPrincipalName = buffer.readString(Charset.forName("UTF-8"));
        String mechanisms = buffer.readString(Charset.forName("UTF-8"));
        if (mechanisms.equals("")) mechanisms = "Kerberos";

        GssapiAuth gssapiAuth = getAuthenticationMethod();
        gssapiAuth.authenticate(writer, serverPrincipalName, mechanisms);
    }


    @Override
    public void handleResultPacket(ReadPacketFetcher packetFetcher) throws SQLException, IOException {
        try {
            Buffer buffer = packetFetcher.getReusableBuffer();
            if (buffer.getByteAt(0) == Packet.ERROR) {
                ErrorPacket ep = new ErrorPacket(buffer);
                String message = ep.getMessage();
                throw new SQLException("Could not connect: " + message, ep.getSqlState(), ep.getErrorNumber());
            }
        } catch (EOFException e) {
            throw new SQLException("Authentication exception", "28000", 1045, e);
        }
    }

    /**
     * Get authentication method according to classpath.
     * Windows native authentication is using Waffle-jna.
     *
     * @return authentication method
     */
    private GssapiAuth getAuthenticationMethod() {
        try {
            //Waffle-jna has jna as dependency, so if not available on classpath, just use standard authentication
            Class platformClass = Class.forName("com.sun.jna.Platform");
            @SuppressWarnings("unchecked")
            Method method = platformClass.getMethod("isWindows");
            Boolean isWindows = (Boolean) method.invoke(platformClass);
            if (isWindows.booleanValue()) {
                try {
                    Class.forName("waffle.windows.auth.impl.WindowsAuthProviderImpl");
                    return new WindowsNativeSspiAuthentication(packetFetcher, packSeq);
                } catch (ClassNotFoundException cle) {
                    //waffle not in the classpath
                }
            }
        } catch (Exception cle) {
            //jna jar's are not in classpath
        }
        return new StandardGssapiAuthentication(packetFetcher, packSeq);
    }

}

