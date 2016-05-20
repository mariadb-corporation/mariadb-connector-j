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

package org.mariadb.jdbc.internal.packet.send.gssapi;

import org.ietf.jgss.*;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.*;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class StandardGssapiAuthentication extends GssapiAuth {
    public StandardGssapiAuthentication(ReadPacketFetcher packetFetcher, int packSeq) {
        super(packetFetcher, packSeq);
    }

    @Override
    public void authenticate(final PacketOutputStream writer, final String serverPrincipalName, String mechanisms)
            throws QueryException, IOException {
        if ("".equals(serverPrincipalName)) {
            throw new QueryException("No principal name defined on server. "
                    + "Please set server variable \"gssapi-principal-name\"", 0, "28000");
        }

        if (System.getProperty("java.security.auth.login.config") == null) {
            final File jaasConfFile;
            try {
                jaasConfFile = File.createTempFile("jaas.conf", null);
                final PrintStream bos = new PrintStream(new FileOutputStream(jaasConfFile));
                bos.print(String.format(
                        "Krb5ConnectorContext {\n"
                                + "com.sun.security.auth.module.Krb5LoginModule required "
                                + "useTicketCache=true "
                                + "debug=true "
                                + "renewTGT=true "
                                + "doNotPrompt=true; };"
                ));
                bos.close();
                jaasConfFile.deleteOnExit();
            } catch (final IOException ex) {
                throw new IOError(ex);
            }

            System.setProperty("java.security.auth.login.config", jaasConfFile.getCanonicalPath());
        }
        try {
            LoginContext loginContext = new LoginContext("Krb5ConnectorContext");
            // attempt authentication
            loginContext.login();
            final Subject mySubject = loginContext.getSubject();
            if (!mySubject.getPrincipals().isEmpty()) {
                try {
                    PrivilegedExceptionAction<Void> action = new PrivilegedExceptionAction<Void>() {
                        @Override
                        public Void run() throws Exception {
                            try {
                                Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");

                                GSSManager manager = GSSManager.getInstance();
                                GSSName peerName = manager.createName(serverPrincipalName, GSSName.NT_USER_NAME);
                                GSSContext context =
                                        manager.createContext(peerName,
                                                krb5Mechanism,
                                                null,
                                                GSSContext.DEFAULT_LIFETIME);
                                context.requestMutualAuth(true);

                                byte[] inToken = new byte[0];
                                byte[] outToken;
                                while (!context.isEstablished()) {

                                    outToken = context.initSecContext(inToken, 0, inToken.length);

                                    // Send a token to the peer if one was generated by acceptSecContext
                                    if (outToken != null) {
                                        writer.startPacket(packSeq);
                                        writer.write(outToken);
                                        writer.finishPacket();
                                    }
                                    if (!context.isEstablished()) {
                                        Buffer buffer = packetFetcher.getReusableBuffer();
                                        packSeq = packetFetcher.getLastPacketSeq() + 1;
                                        inToken = buffer.readRawBytes(buffer.remaining());
                                    }
                                }

                            } catch (GSSException le) {
                                throw new QueryException("GSS-API authentication exception", 1045, "28000", le);
                            }
                            return null;
                        }
                    };
                    Subject.doAs(mySubject, action);
                } catch (PrivilegedActionException exception) {
                    throw new QueryException("GSS-API authentication exception", 1045, "28000", exception);
                }
            } else {
                throw new QueryException("GSS-API authentication exception : no credential cache not found.", 0, "28000");
            }

        } catch (LoginException le) {
            throw new QueryException("GSS-API authentication exception", 1045, "28000", le);
        }
    }
}
