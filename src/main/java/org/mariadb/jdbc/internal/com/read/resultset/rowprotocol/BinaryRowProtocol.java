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

package org.mariadb.jdbc.internal.com.read.resultset.rowprotocol;

import org.mariadb.jdbc.internal.com.read.resultset.ColumnInformation;

public class BinaryRowProtocol extends RowProtocol {
    private final ColumnInformation[] columnInformation;
    private final int columnInformationLength;

    /**
     * Constructor.
     *
     * @param columnInformation       column information.
     * @param columnInformationLength number of columns
     * @param maxFieldSize            max field size
     */
    public BinaryRowProtocol(ColumnInformation[] columnInformation, int columnInformationLength, int maxFieldSize) {
        super(maxFieldSize);
        this.columnInformation = columnInformation;
        this.columnInformationLength = columnInformationLength;
    }

    /**
     * Set length and pos indicator to asked index.
     *
     * @param newIndex index (0 is first).
     * @return true if value is null
     * @see <a href="https://mariadb.com/kb/en/mariadb/resultset-row/">Resultset row protocol documentation</a>
     */
    public boolean setPosition(int newIndex) {

        //check NULL-Bitmap that indicate if field is null
        if ((buf[1 + (newIndex + 2) / 8] & (1 << ((newIndex + 2) % 8))) != 0) {
            return true;
        }

        //if not must parse data until reading the desired field
        if (index != newIndex) {
            int internalPos = this.pos;
            if (index == -1 || index > newIndex) {
                //if there wasn't previous non-null read field, or if last field was after searched index,
                // position is set on first field position.
                index = 0;
                internalPos = 1 + (columnInformationLength + 9) / 8; // 0x00 header + NULL-Bitmap length
            } else {
                //start at previous non-null field position if was before searched index
                index++;
                internalPos += length;
            }

            for (; index <= newIndex; index++) {
                if ((buf[1 + (index + 2) / 8] & (1 << ((index + 2) % 8))) == 0) {
                    if (index != newIndex) {
                        //skip bytes
                        switch (columnInformation[index].getColumnType()) {
                            case BIGINT:
                            case DOUBLE:
                                internalPos += 8;
                                break;

                            case INTEGER:
                            case MEDIUMINT:
                            case FLOAT:
                                internalPos += 4;
                                break;

                            case SMALLINT:
                            case YEAR:
                                internalPos += 2;
                                break;

                            case TINYINT:
                                internalPos += 1;
                                break;

                            default:
                                int type = this.buf[internalPos++] & 0xff;
                                switch (type) {

                                    case 251:
                                        break;

                                    case 252:
                                        internalPos += 2 + (0xffff & (((buf[internalPos] & 0xff) + ((buf[internalPos + 1] & 0xff) << 8))));
                                        break;

                                    case 253:
                                        internalPos += 3 + (0xffffff & ((buf[internalPos] & 0xff)
                                                + ((buf[internalPos + 1] & 0xff) << 8)
                                                + ((buf[internalPos + 2] & 0xff) << 16)));
                                        break;

                                    case 254:
                                        internalPos += 8 + ((buf[internalPos] & 0xff)
                                                + ((long) (buf[internalPos + 1] & 0xff) << 8)
                                                + ((long) (buf[internalPos + 2] & 0xff) << 16)
                                                + ((long) (buf[internalPos + 3] & 0xff) << 24)
                                                + ((long) (buf[internalPos + 4] & 0xff) << 32)
                                                + ((long) (buf[internalPos + 5] & 0xff) << 40)
                                                + ((long) (buf[internalPos + 6] & 0xff) << 48)
                                                + ((long) (buf[internalPos + 7] & 0xff) << 56));
                                        break;

                                    default:
                                        internalPos += type;
                                        break;
                                }
                                break;
                        }
                    } else {
                        //read asked field position and length
                        switch (columnInformation[index].getColumnType()) {
                            case BIGINT:
                            case DOUBLE:
                                this.pos = internalPos;
                                length = 8;
                                return false;

                            case INTEGER:
                            case MEDIUMINT:
                            case FLOAT:
                                this.pos = internalPos;
                                length = 4;
                                return false;

                            case SMALLINT:
                            case YEAR:
                                this.pos = internalPos;
                                length = 2;
                                return false;

                            case TINYINT:
                                this.pos = internalPos;
                                length = 1;
                                return false;

                            default:
                                //field with variable length
                                int type = this.buf[internalPos++] & 0xff;
                                switch (type) {
                                    case 251:
                                        //null length field
                                        //must never occur
                                        //null value are set in NULL-Bitmap, not send with a null length indicator.
                                        throw new IllegalStateException("null data is encoded in binary protocol but NULL-Bitmap is not set");

                                    case 252:
                                        //length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
                                        length = 0xffff & ((buf[internalPos++] & 0xff)
                                                + ((buf[internalPos++] & 0xff) << 8));
                                        this.pos = internalPos;
                                        return false;

                                    case 253:
                                        //length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
                                        length = 0xffffff & ((buf[internalPos++] & 0xff)
                                                + ((buf[internalPos++] & 0xff) << 8)
                                                + ((buf[internalPos++] & 0xff) << 16));
                                        this.pos = internalPos;
                                        return false;

                                    case 254:
                                        //length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
                                        length = (int) ((buf[internalPos++] & 0xff)
                                                + ((long) (buf[internalPos++] & 0xff) << 8)
                                                + ((long) (buf[internalPos++] & 0xff) << 16)
                                                + ((long) (buf[internalPos++] & 0xff) << 24)
                                                + ((long) (buf[internalPos++] & 0xff) << 32)
                                                + ((long) (buf[internalPos++] & 0xff) << 40)
                                                + ((long) (buf[internalPos++] & 0xff) << 48)
                                                + ((long) (buf[internalPos++] & 0xff) << 56));
                                        this.pos = internalPos;
                                        return false;

                                    default:
                                        //length is encoded on 1 bytes (is then less than 251)
                                        length = type;
                                        this.pos = internalPos;
                                        return false;

                                }
                        }
                    }
                }
            }
        }
        return length == NULL_LENGTH;
    }

}