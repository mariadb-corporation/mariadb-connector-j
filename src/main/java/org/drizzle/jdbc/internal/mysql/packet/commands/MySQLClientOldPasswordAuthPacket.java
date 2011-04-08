/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Stephane Giron
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

package org.drizzle.jdbc.internal.mysql.packet.commands;

import java.io.IOException;
import java.io.OutputStream;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;

public class MySQLClientOldPasswordAuthPacket implements CommandPacket
{
    private final WriteBuffer writeBuffer;
    private int packSeq = 0;

    public MySQLClientOldPasswordAuthPacket(String password, byte[] seed, int packSeq)
    {
        writeBuffer = new WriteBuffer();
        this.packSeq  = packSeq;
        
        byte[] oldPassword = cryptOldFormatPassword(password, new String(seed));
        writeBuffer.writeByteArray(oldPassword).writeByte((byte) 0x00);
    }

    public int send(OutputStream os) throws IOException, QueryException
    {
        os.write(writeBuffer.getLengthWithPacketSeq((byte) packSeq));
        os.write(writeBuffer.getBuffer(), 0, writeBuffer.getLength());
        os.flush();
        return packSeq;
    }
    
    
    private byte[] cryptOldFormatPassword(String password, String seed)
    {
        byte[] result = new byte[seed.length()];

        if ((password == null) || (password.length() == 0))
        {
            return new byte[0];
        }

        long[] seedHash = hashPassword(seed);
        long[] passHash = hashPassword(password);

        randStruct randSeed = new randStruct(seedHash[0] ^ passHash[0],
                seedHash[1] ^ passHash[1]);

        for (int i = 0; i < seed.length(); i++)
        {
            result[i] = (byte) Math.floor((random(randSeed) * 31) + 64);
        }
        byte extra = (byte) Math.floor(random(randSeed) * 31);
        for (int i = 0; i < seed.length(); i++)
        {
            result[i] ^= extra;
        }
        return result;
    }

    private double random(randStruct rand)
    {
        rand.seed1 = (rand.seed1 * 3 + rand.seed2) % rand.maxValue;
        rand.seed2 = (rand.seed1 + rand.seed2 + 33) % rand.maxValue;
        return (double)rand.seed1 / rand.maxValue;
    }

    private long[] hashPassword(String password)
    {
        long nr = 1345345333L, nr2 = 0x12345671L, add = 7;
        for (int i = 0; i < password.length(); i++)
        {
            char currChar = password.charAt(i);
            if (currChar == ' ' || currChar == '\t')
                continue;

            long tmp = currChar;
            nr ^= (((nr & 63) + add) * tmp) + (nr << 8);
            nr2 += (nr2 << 8) ^ nr;
            add += tmp;
        }
        return new long[] {nr & 0x7FFFFFFF, nr2 & 0x7FFFFFFF};
    }
    
    private class randStruct
    {
        long seed1, seed2;
        final long maxValue= 0x3FFFFFFFL;

        public randStruct(long seed1, long seed2)
        {
            this.seed1 = seed1 % maxValue;
            this.seed2 = seed2 % maxValue;
        }
    }
}
