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

package org.mariadb.jdbc.internal.com.read.resultset.rowprotocol;

public class TextRowProtocol extends RowProtocol {

    public TextRowProtocol(int maxFieldSize) {
        super(maxFieldSize);
    }

    /**
     * Set length and pos indicator to asked index.
     *
     * @param newIndex index (0 is first).
     * @return true if value is null
     */
    public boolean setPosition(int newIndex) {
        if (index != newIndex) {
            if (index == -1 || index > newIndex) {
                pos = 0;
                index = 0;
            } else {
                index++;
                if (length != NULL_LENGTH) pos += length;
            }

            for (; index <= newIndex; index++) {
                if (index != newIndex) {
                    int type = this.buf[this.pos++] & 0xff;
                    switch (type) {
                        case 251:
                            break;
                        case 252:
                            pos += 2 + (0xffff & (((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8))));
                            break;
                        case 253:
                            pos += 3 + (0xffffff & ((buf[pos] & 0xff)
                                    + ((buf[pos + 1] & 0xff) << 8)
                                    + ((buf[pos + 2] & 0xff) << 16)));
                            break;
                        case 254:
                            pos += 8 + ((buf[pos] & 0xff)
                                    + ((long) (buf[pos + 1] & 0xff) << 8)
                                    + ((long) (buf[pos + 2] & 0xff) << 16)
                                    + ((long) (buf[pos + 3] & 0xff) << 24)
                                    + ((long) (buf[pos + 4] & 0xff) << 32)
                                    + ((long) (buf[pos + 5] & 0xff) << 40)
                                    + ((long) (buf[pos + 6] & 0xff) << 48)
                                    + ((long) (buf[pos + 7] & 0xff) << 56));
                            break;
                        default:
                            pos += type;
                    }
                } else {
                    int type = this.buf[this.pos++] & 0xff;
                    switch (type) {
                        case 251:
                            length = NULL_LENGTH;
                            return true;
                        case 252:
                            length = 0xffff & ((buf[pos++] & 0xff)
                                    + ((buf[pos++] & 0xff) << 8));
                            break;
                        case 253:
                            length = 0xffffff & ((buf[pos++] & 0xff)
                                    + ((buf[pos++] & 0xff) << 8)
                                    + ((buf[pos++] & 0xff) << 16));
                            break;
                        case 254:
                            length = (int) ((buf[pos++] & 0xff)
                                    + ((long) (buf[pos++] & 0xff) << 8)
                                    + ((long) (buf[pos++] & 0xff) << 16)
                                    + ((long) (buf[pos++] & 0xff) << 24)
                                    + ((long) (buf[pos++] & 0xff) << 32)
                                    + ((long) (buf[pos++] & 0xff) << 40)
                                    + ((long) (buf[pos++] & 0xff) << 48)
                                    + ((long) (buf[pos++] & 0xff) << 56));
                            break;
                        default:
                            length = type;
                    }
                    return false;
                }
            }
        }
        return length == NULL_LENGTH;
    }

}