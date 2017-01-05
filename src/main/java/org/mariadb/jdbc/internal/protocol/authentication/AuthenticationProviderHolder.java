package org.mariadb.jdbc.internal.protocol.authentication;

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

import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.send.InterfaceAuthSwitchSendResponsePacket;

import java.sql.SQLException;

/**
 * Provider to handle plugin authentication.
 * This can allow library users to override our default Authentication provider.
 */
public class AuthenticationProviderHolder {

    /**
     * The default provider will construct a new pool on every request.
     */
    public static AuthenticationProvider DEFAULT_PROVIDER = new AuthenticationProvider() {
        @Override
        public InterfaceAuthSwitchSendResponsePacket processAuthPlugin(ReadPacketFetcher packetFetcher, String plugin, String password,
                                                                       byte[] authData, int seqNo) throws SQLException {
            return DefaultAuthenticationProvider.processAuthPlugin(packetFetcher, plugin, password, authData, seqNo);
        }
    };

    private static volatile AuthenticationProvider currentProvider = null;

    /**
     * Get the currently set {@link AuthenticationProvider} from set invocations via
     * {@link #setAuthenticationProvider(AuthenticationProvider)}.
     * If none has been set a default provider will be provided (never a {@code null} result).
     *
     * @return Provider to get an AuthenticationProvider
     */
    public static AuthenticationProvider getAuthenticationProvider() {
        AuthenticationProvider result = currentProvider;
        if (result == null) {
            return DEFAULT_PROVIDER;
        } else {
            return result;
        }
    }

    /**
     * Change the current set authentication provider.  This provider will be provided in future requests
     * to {@link #getAuthenticationProvider()}.
     *
     * @param newProvider New provider to use, or {@code null} to use the default provider
     */
    public static void setAuthenticationProvider(AuthenticationProvider newProvider) {
        currentProvider = newProvider;
    }

    /**
     * Provider to handle authentication.
     */
    public interface AuthenticationProvider {
        public InterfaceAuthSwitchSendResponsePacket processAuthPlugin(ReadPacketFetcher packetFetcher, String plugin, String password,
                                                                       byte[] authData, int seqNo) throws SQLException;
    }


}
