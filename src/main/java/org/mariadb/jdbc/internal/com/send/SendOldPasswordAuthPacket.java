/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

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

package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.util.Utils;

import java.io.IOException;

public class SendOldPasswordAuthPacket extends AbstractAuthSwitchSendResponsePacket implements InterfaceAuthSwitchSendResponsePacket {

    public SendOldPasswordAuthPacket(String password, byte[] authData, int packSeq, String passwordCharacterEncoding) {
        super(packSeq, authData, password, passwordCharacterEncoding);
    }

    /**
     * Send password stream.
     * @param pos database socket
     * @throws IOException if a connection error occur
     */
    public void send(PacketOutputStream pos) throws IOException {

        if (password == null || password.equals("")) {
            pos.writeEmptyPacket(packSeq);
            return;
        }
        pos.startPacket(packSeq);
        byte[] seed = Utils.copyWithLength(authData, 8);
        pos.write(cryptOldFormatPassword(password, new String(seed)));
        pos.write((byte) 0x00);
        pos.flush();
    }


    private byte[] cryptOldFormatPassword(String password, String seed) {
        byte[] result = new byte[seed.length()];

        if ((password == null) || (password.length() == 0)) {
            return new byte[0];
        }

        long[] seedHash = hashPassword(seed);
        long[] passHash = hashPassword(password);

        RandStruct randSeed = new RandStruct(seedHash[0] ^ passHash[0],
                seedHash[1] ^ passHash[1]);

        for (int i = 0; i < seed.length(); i++) {
            result[i] = (byte) Math.floor((random(randSeed) * 31) + 64);
        }
        byte extra = (byte) Math.floor(random(randSeed) * 31);
        for (int i = 0; i < seed.length(); i++) {
            result[i] ^= extra;
        }
        return result;
    }

    private double random(RandStruct rand) {
        rand.seed1 = (rand.seed1 * 3 + rand.seed2) % rand.maxValue;
        rand.seed2 = (rand.seed1 + rand.seed2 + 33) % rand.maxValue;
        return (double) rand.seed1 / rand.maxValue;
    }

    private long[] hashPassword(String password) {
        long nr = 1345345333L;
        long nr2 = 0x12345671L;
        long add = 7;

        for (int i = 0; i < password.length(); i++) {
            char currChar = password.charAt(i);
            if (currChar == ' ' || currChar == '\t') {
                continue;
            }

            long tmp = currChar;
            nr ^= (((nr & 63) + add) * tmp) + (nr << 8);
            nr2 += (nr2 << 8) ^ nr;
            add += tmp;
        }
        return new long[]{nr & 0x7FFFFFFF, nr2 & 0x7FFFFFFF};
    }

    private class RandStruct {
        final long maxValue = 0x3FFFFFFFL;
        long seed1;
        long seed2;

        public RandStruct(long seed1, long seed2) {
            this.seed1 = seed1 % maxValue;
            this.seed2 = seed2 % maxValue;
        }
    }
}
