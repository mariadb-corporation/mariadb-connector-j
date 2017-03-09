/*
MariaDB Client for Java

Copyright (c) 2016 Monty Program Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson

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


package org.mariadb.jdbc.internal.protocol.authentication;

import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.send.*;

import java.sql.SQLException;

public class DefaultAuthenticationProvider {
    public static final String MYSQL_NATIVE_PASSWORD = "mysql_native_password";
    public static final String MYSQL_OLD_PASSWORD = "mysql_old_password";
    public static final String MYSQL_CLEAR_PASSWORD = "mysql_clear_password";
    public static final String GSSAPI_CLIENT = "auth_gssapi_client";
    public static final String DIALOG = "dialog";

    /**
     * Process AuthenticationSwitch.
     * @param packetFetcher             packet fetcher
     * @param plugin                    plugin name
     * @param password                  password
     * @param authData                  auth data
     * @param seqNo                     packet sequence number
     * @param passwordCharacterEncoding password character encoding
     * @return authentication response according to parameters
     * @throws SQLException if error occur.
     */
    public static InterfaceAuthSwitchSendResponsePacket processAuthPlugin(ReadPacketFetcher packetFetcher, String plugin, String password,
                                                                          byte[] authData, int seqNo, String passwordCharacterEncoding)
            throws SQLException {
        switch (plugin) {
            case MYSQL_NATIVE_PASSWORD:
                return new SendNativePasswordAuthPacket(password, authData, seqNo, passwordCharacterEncoding);
            case MYSQL_OLD_PASSWORD:
                return new SendOldPasswordAuthPacket(password, authData, seqNo, passwordCharacterEncoding);
            case MYSQL_CLEAR_PASSWORD:
                return new SendClearPasswordAuthPacket(password, authData, seqNo, passwordCharacterEncoding);
            case DIALOG:
                return new SendPamAuthPacket(packetFetcher, password, authData, seqNo, passwordCharacterEncoding);
            case GSSAPI_CLIENT:
                return new SendGssApiAuthPacket(packetFetcher, password, authData, seqNo, passwordCharacterEncoding);
            default:
                throw new SQLException("Client does not support authentication protocol requested by server. "
                        + "Consider upgrading MariaDB client. plugin was = " + plugin, "08004", 1251);
        }
    }

}
