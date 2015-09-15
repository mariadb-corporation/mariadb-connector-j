/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

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

package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.internal.common.packet.RawPacket;
import org.mariadb.jdbc.internal.common.packet.buffer.Reader;
import org.mariadb.jdbc.internal.common.queryresults.ColumnFlags;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Types;

public class MySQLColumnInformation {
    RawPacket buffer;
    private short charsetNumber;
    private long length;
    private MySQLType type;
    private byte decimals;
    private short flags;

    // This array stored character length for every collation id up to collation id 256
    // It is generated from the information schema using
    // "select  id, maxlen from information_schema.character_sets, information_schema.collations
    // where character_sets.character_set_name = collations.character_set_name order by id"
    private static final int[] maxCharlen = {
            0, 2, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 3, 2, 1, 1,
            1, 0, 1, 2, 1, 1, 1, 1,
            2, 1, 1, 1, 2, 1, 1, 1,
            1, 3, 1, 2, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 4, 4, 1,
            1, 1, 1, 1, 1, 1, 4, 4,
            0, 1, 1, 1, 4, 4, 0, 1,
            1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 0, 1, 1, 1,
            1, 1, 1, 3, 2, 2, 2, 2,
            2, 1, 2, 3, 1, 1, 1, 2,
            2, 3, 3, 1, 0, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4,
            4, 0, 0, 0, 0, 0, 0, 0,
            2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 2, 2, 2, 2,
            2, 2, 2, 2, 0, 2, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 2,
            4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            3, 3, 3, 3, 3, 3, 3, 3,
            3, 3, 3, 3, 3, 3, 3, 3,
            3, 3, 3, 3, 0, 3, 4, 4,
            0, 0, 0, 0, 0, 0, 0, 3,
            4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 0, 4, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0
    };

    public static MySQLColumnInformation create(String name, MySQLType type) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            for (int i = 0; i < 4; i++) {
                baos.write(new byte[]{1, 0}); // catalog, empty string
            }
            for (int i = 0; i < 2; i++) {
                baos.write(new byte[]{(byte) name.length()});
                baos.write(name.getBytes());
            }
            baos.write(0xc);
            baos.write(new byte[]{33, 0});  /* charset  = UTF8 */
            int len = 1;

            /* Sensible predefined length - since we're dealing with I_S here, most char fields are 64 char long */
            switch (type.getSqlType()) {
                case Types.VARCHAR:
                case Types.CHAR:
                    len = 64 * 3; /* 3 bytes per UTF8 char */
                    break;
                case Types.SMALLINT:
                    len = 5;
                    break;
                case Types.NULL:
                    len = 0;
                    break;
                default:
                    len = 1;
                    break;
            }
            baos.write(new byte[]{(byte) len, 0, 0, 0});  /*  length */
            baos.write(MySQLType.toServer(type.getSqlType()).getType());
            baos.write(new byte[]{0, 0});   /* flags */
            baos.write(0); /* decimals */
            baos.write(new byte[]{0, 0});   /* filler */
            return new MySQLColumnInformation(new RawPacket(ByteBuffer.wrap(baos.toByteArray()).order(ByteOrder.LITTLE_ENDIAN), 0));
        } catch (IOException ioe) {
            throw new RuntimeException("unexpected condition", ioe);
        }
    }


    public MySQLColumnInformation(RawPacket buffer) throws IOException {
        this.buffer = buffer;
        buffer.getByteBuffer().mark();
        Reader reader = new Reader(buffer);

        /*
        lenenc_str     catalog
        lenenc_str     schema
        lenenc_str     table
        lenenc_str     org_table
        lenenc_str     name
        lenenc_str     org_name
        lenenc_int     length of fixed-length fields [0c]
        2              character set
        4              column length
        1              type
        2              flags
        1              decimals
        2              filler [00] [00]

         */
        reader.skipLengthEncodedBytes();  /* catalog */
        reader.skipLengthEncodedBytes();  /* db */
        reader.skipLengthEncodedBytes();  /* table */
        reader.skipLengthEncodedBytes();  /* original table */
        reader.skipLengthEncodedBytes();  /* name */
        reader.skipLengthEncodedBytes();  /* org_name */
        reader.skipBytes(1);
        charsetNumber = reader.readShort();
        length = reader.readInt();
        type = MySQLType.fromServer(reader.readByte() & 0xff);
        flags = reader.readShort();
        decimals = reader.readByte();


        int sqlType = type.getSqlType();

        if ((sqlType == Types.BLOB || sqlType == Types.VARBINARY || sqlType == Types.BINARY || sqlType == Types.LONGVARBINARY)
                && !isBinary()) {
           /* MySQL Text datatype */
            type = MySQLType.VARCHAR;
        }
    }


    private String getString(int idx) {
        try {
            buffer.getByteBuffer().reset();
            buffer.getByteBuffer().mark();
            Reader reader = new Reader(buffer);
            for (int i = 0; i < idx; i++) {
                reader.skipLengthEncodedBytes();
            }
            return new String(reader.getLengthEncodedBytes(), "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("this does not happen", e);
        }
    }

    public String getCatalog() {
        return null;
    }

    public String getDb() {
        return getString(1);
    }

    public String getTable() {
        return getString(2);
    }

    public String getOriginalTable() {
        return getString(3);
    }

    public String getName() {
        return getString(4);
    }

    public String getOriginalName() {
        return getString(5);
    }

    public short getCharsetNumber() {
        return charsetNumber;
    }

    public long getLength() {
        return length;
    }

    public int getDisplaySize() {
        int t = type.getSqlType();
        if (t == Types.VARCHAR || t == Types.CHAR) {
            int maxWidth = maxCharlen[charsetNumber & 0xff];
            if (maxWidth == 0)
                maxWidth = 1;

            return (int) length / maxWidth;

        }
        return (int) length;
    }

    public byte getDecimals() {
        return decimals;
    }

    public MySQLType getType() {
        return type;
    }

    public short getFlags() {
        return flags;
    }

    public boolean isSigned() {
        return ((flags & ColumnFlags.UNSIGNED) == 0);
    }

    public boolean isNotNull() {
        return ((this.flags & 1) > 0);
    }

    public boolean isPrimaryKey() {
        return ((this.flags & 2) > 0);
    }

    public boolean isUniqueKey() {
        return ((this.flags & 4) > 0);
    }

    public boolean isMultipleKey() {
        return ((this.flags & 8) > 0);
    }

    public boolean isBlob() {
        return ((this.flags & 16) > 0);
    }

    public void setUnsigned() {
        this.flags |= 32;
    }

    public boolean isZeroFill() {
        return ((this.flags & 64) > 0);
    }

    // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle like string), but have the binary flag
    public boolean isBinary() {
        return (getCharsetNumber() == 63);
    }
}