/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.com.send;


import org.mariadb.jdbc.internal.com.send.ed25519.math.GroupElement;
import org.mariadb.jdbc.internal.com.send.ed25519.math.ed25519.Ed25519ScalarOps;
import org.mariadb.jdbc.internal.com.send.ed25519.spec.EdDSANamedCurveTable;
import org.mariadb.jdbc.internal.com.send.ed25519.spec.EdDSAParameterSpec;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Arrays;

public class SendEd25519PasswordAuthPacket extends AbstractAuthSwitchSendResponsePacket implements InterfaceAuthSwitchSendResponsePacket {

    public SendEd25519PasswordAuthPacket(String password, byte[] authData, int packSeq, String passwordCharacterEncoding) {
        super(packSeq, authData, password, passwordCharacterEncoding);
    }

    /**
     * Send native password stream.
     *
     * @param pos database socket
     * @throws IOException if a connection error occur
     */
    public void send(PacketOutputStream pos) throws IOException {
        try {

            if (password == null || password.isEmpty()) {
                pos.writeEmptyPacket(packSeq);
                return;
            }

            pos.startPacket(packSeq);
            pos.write(ed25519SignWithPassword(password, authData, passwordCharacterEncoding));
            pos.flush();
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("Could not use SHA-512, failing", e);
        } catch (InvalidKeyException invalidKey) {
            throw new IOException("authentication error with Ed25519", invalidKey);
        } catch (SignatureException e) {
            throw new IOException("signature error using Ed25519", e);
        }
    }

    private static byte[] ed25519SignWithPassword(final String password, final byte[] seed, String passwordCharacterEncoding)
            throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, UnsupportedEncodingException {

        if (password == null || password.isEmpty()) return new byte[0];

        byte[] bytePwd;
        if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
            bytePwd = password.getBytes(passwordCharacterEncoding);
        } else {
            bytePwd = password.getBytes();
        }

        MessageDigest hash = MessageDigest.getInstance("SHA-512");

        int mlen = seed.length;
        byte[] sm = new byte[64 + mlen];

        byte[] az = hash.digest(bytePwd);
        az[0] &= 248;
        az[31] &= 63;
        az[31] |= 64;

        System.arraycopy(seed, 0, sm, 64, mlen);
        System.arraycopy(az, 32, sm, 32, 32);

        byte[] buff = Arrays.copyOfRange(sm, 32, 96);
        hash.reset();
        byte[] nonce = hash.digest(buff);

        Ed25519ScalarOps scalar = new Ed25519ScalarOps();

        EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName("Ed25519");
        GroupElement elementAvalue = spec.getB().scalarMultiply(az);
        byte[] elementAarray = elementAvalue.toByteArray();
        System.arraycopy(elementAarray, 0, sm, 32, elementAarray.length);

        nonce = scalar.reduce(nonce);
        GroupElement elementRvalue = spec.getB().scalarMultiply(nonce);
        byte[] elementRarray = elementRvalue.toByteArray();
        System.arraycopy(elementRarray, 0, sm, 0, elementRarray.length);

        hash.reset();
        byte[] hram = hash.digest(sm);
        hram = scalar.reduce(hram);
        byte[] tt = scalar.multiplyAndAdd(hram, az, nonce);
        System.arraycopy(tt, 0, sm, 32, tt.length);

        return Arrays.copyOfRange(sm, 0, 64);
    }

}
