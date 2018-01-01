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

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;


public class MariaDbBlob implements Blob, Serializable {
    private static final long serialVersionUID = 8557003556592493381L;

    protected byte[] data;
    protected int offset;
    protected int length;

    /**
     * Creates an empty blob.
     */
    public MariaDbBlob() {
        data = new byte[0];
        offset = 0;
        length = 0;
    }

    /**
     * Creates a blob with content.
     *
     * @param bytes the content for the blob.
     */
    public MariaDbBlob(byte[] bytes) {
        if (bytes == null) {
            throw new NullPointerException("byte array is null");
        }
        data = bytes;
        offset = 0;
        length = bytes.length;
    }

    /**
     * Creates a blob with content.
     *
     * @param bytes     the content for the blob.
     * @param offset    offset
     * @param length    length
     */
    public MariaDbBlob(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            throw new NullPointerException("byte array is null");
        }
        data = bytes;
        this.offset = offset;
        this.length = Math.min(bytes.length - offset, length);
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException {
        out.writeInt(length);
        if (length > 0) {
            out.write(data, offset, length);
            offset += length;
            length = 0;
        }
    }

    private void readObject(ObjectInputStream in)
            throws IOException {
        offset = 0;
        length = in.readInt();
        data = new byte[length];
        if (length > 0) {
            in.readFully(data, 0, length);
        }
    }

    /**
     * Returns the number of bytes in the <code>BLOB</code> value designated by this <code>Blob</code> object.
     *
     * @return length of the <code>BLOB</code> in bytes
     * @throws SQLException if there is an error accessing the length of the <code>BLOB</code>
     */
    public long length() throws SQLException {
        return length;
    }

    /**
     * Retrieves all or part of the <code>BLOB</code> value that this <code>Blob</code> object represents, as an array
     * of bytes.  This <code>byte</code> array contains up to <code>length</code> consecutive bytes starting at position
     * <code>pos</code>.
     *
     * @param pos    the ordinal position of the first byte in the <code>BLOB</code> value to be extracted; the first
     *               byte is at position 1
     * @param length the number of consecutive bytes to be copied; the value for length must be 0 or greater
     * @return a byte array containing up to <code>length</code> consecutive bytes from the <code>BLOB</code> value
     * designated by this <code>Blob</code> object, starting with the byte at position <code>pos</code>
     * @throws SQLException if there is an error accessing the <code>BLOB</code> value; if pos is less than 1
     *                      or length is less than 0
     * @see #setBytes
     * @since 1.2
     */
    public byte[] getBytes(final long pos, final int length) throws SQLException {
        if (pos < 1) {
            throw ExceptionMapper.getSqlException("Pos starts at 1");
        }
        final int offset = this.offset + (int) (pos - 1);
        byte[] result = new byte[length];
        System.arraycopy(data, offset, result, 0, Math.min(this.length - (int) (pos - 1), length));
        return result;
    }

    /**
     * Retrieves the <code>BLOB</code> value designated by this <code>Blob</code> instance as a stream.
     *
     * @return a stream containing the <code>BLOB</code> data
     * @throws SQLException if something went wrong
     * @see #setBinaryStream
     */
    public InputStream getBinaryStream() throws SQLException {
        return getBinaryStream(1, length);
    }

    /**
     * Returns an <code>InputStream</code> object that contains a partial <code>Blob</code> value, starting  with the
     * byte specified by pos, which is length bytes in length.
     *
     * @param pos    the offset to the first byte of the partial value to be retrieved. The first byte in the
     *               <code>Blob</code> is at position 1
     * @param length the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial <code>Blob</code> value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the number of bytes in the
     *                      <code>Blob</code> or if pos + length is greater than the number of bytes in the
     *                      <code>Blob</code>
     */
    public InputStream getBinaryStream(final long pos, final long length) throws SQLException {
        if (pos < 1) {
            throw ExceptionMapper.getSqlException("Out of range (position should be > 0)");
        }
        if (pos - 1 > this.length) {
            throw ExceptionMapper.getSqlException("Out of range (position > stream size)");
        }
        if (pos + length - 1 > this.length) {
            throw ExceptionMapper.getSqlException("Out of range (position + length - 1 > streamSize)");
        }

        return new ByteArrayInputStream(data, this.offset + (int) pos - 1, (int) length);
    }

    /**
     * Retrieves the byte position at which the specified byte array <code>pattern</code> begins within the
     * <code>BLOB</code> value that this <code>Blob</code> object represents.  The search for <code>pattern</code>
     * begins at position <code>start</code>.
     *
     * @param pattern the byte array for which to search
     * @param start   the position at which to begin searching; the first position is 1
     * @return the position at which the pattern appears, else -1
     */
    public long position(final byte[] pattern, final long start) throws SQLException {
        if (start < 1) {
            throw ExceptionMapper.getSqlException("Start should be > 0, first position is 1.");
        }
        if (start > this.length) {
            throw ExceptionMapper.getSqlException("Start should be <= " + this.length);
        }
        final long actualStart = offset + start - 1;
        for (int i = (int) actualStart; i < offset + this.length; i++) {
            if (data[i] == pattern[0]) {
                boolean isEqual = true;
                for (int j = 1; j < pattern.length; j++) {
                    if (i + j >= offset + this.length) {
                        return -1;
                    }
                    if (data[i + j] != pattern[j]) {
                        isEqual = false;
                    }
                }
                if (isEqual) {
                    return i + 1 - offset;
                }
            }
        }
        return -1;
    }

    /**
     * Retrieves the byte position in the <code>BLOB</code> value designated by this <code>Blob</code> object at which
     * <code>pattern</code> begins.  The search begins at position <code>start</code>.
     *
     * @param pattern the <code>Blob</code> object designating the <code>BLOB</code> value for which to search
     * @param start   the position in the <code>BLOB</code> value at which to begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     */
    public long position(final Blob pattern, final long start) throws SQLException {
        return position(pattern.getBytes(1, (int) pattern.length()), start);
    }

    /**
     * Writes the given array of bytes to the <code>BLOB</code> value that this <code>Blob</code> object represents,
     * starting at position <code>pos</code>, and returns the number of bytes written. The array of bytes will overwrite
     * the existing bytes in the <code>Blob</code> object starting at the position <code>pos</code>.  If the end of the
     * <code>Blob</code> value is reached while writing the array of bytes, then the length of the <code>Blob</code>
     * value will be increased to accommodate the extra bytes.
     *
     * @param pos   the position in the <code>BLOB</code> object at which to start writing; the first position is 1
     * @param bytes the array of bytes to be written to the <code>BLOB</code> value that this <code>Blob</code> object
     *              represents
     * @return the number of bytes written
     * @see #getBytes
     */
    public int setBytes(final long pos, final byte[] bytes) throws SQLException {
        if (pos < 1) {
            throw ExceptionMapper.getSqlException("pos should be > 0, first position is 1.");
        }

        final int arrayPos = (int) pos - 1;

        if (length > arrayPos + bytes.length) {

            System.arraycopy(bytes, 0, data, offset + arrayPos, bytes.length);

        } else {

            byte[] newContent = new byte[arrayPos + bytes.length];
            if ( Math.min(arrayPos, length) > 0) {
                System.arraycopy(data, this.offset, newContent, 0, Math.min(arrayPos, length));
            }
            System.arraycopy(bytes, 0, newContent, arrayPos, bytes.length);
            data = newContent;
            length = arrayPos + bytes.length;
            offset = 0;

        }
        return bytes.length;
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the <code>BLOB</code> value that this
     * <code>Blob</code> object represents and returns the number of bytes written. Writing starts at position
     * <code>pos</code> in the <code>BLOB</code> value; <code>len</code> bytes from the given byte array are written.
     * The array of bytes will overwrite the existing bytes in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached while writing the array of bytes, then
     * the length of the <code>Blob</code> value will be increased to accommodate the extra bytes.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the <code>BLOB</code>
     * value then the behavior is undefined. Some JDBC drivers may throw a <code>SQLException</code> while other drivers
     * may support this operation.
     *
     * @param pos    the position in the <code>BLOB</code> object at which to start writing; the first position is 1
     * @param bytes  the array of bytes to be written to this <code>BLOB</code> object
     * @param offset the offset into the array <code>bytes</code> at which to start reading the bytes to be set
     * @param len    the number of bytes to be written to the <code>BLOB</code> value from the array of bytes
     *               <code>bytes</code>
     * @return the number of bytes written
     * @throws SQLException if there is an error accessing the <code>BLOB</code> value or if pos is less than
     *                      1
     * @see #getBytes
     */
    public int setBytes(final long pos,
                        final byte[] bytes,
                        final int offset,
                        final int len) throws SQLException {

        if (pos < 1) {
            throw ExceptionMapper.getSqlException("pos should be > 0, first position is 1.");
        }

        final int arrayPos = (int) pos - 1;
        final int byteToWrite = Math.min(bytes.length - offset, len);

        if (length > arrayPos + byteToWrite) {

            System.arraycopy(bytes, offset, data, this.offset + arrayPos, byteToWrite);

        } else {

            byte[] newContent = new byte[arrayPos + byteToWrite];
            if ( Math.min(arrayPos, length) > 0 ) {
                System.arraycopy(data, this.offset, newContent, 0, Math.min(arrayPos, length));
            }
            System.arraycopy(bytes, offset, newContent, arrayPos, byteToWrite);
            data = newContent;
            length = arrayPos + byteToWrite;
            this.offset = 0;
        }

        return byteToWrite;
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code> value that this <code>Blob</code> object
     * represents.  The stream begins at position <code>pos</code>. The  bytes written to the stream will overwrite the
     * existing bytes in the <code>Blob</code> object starting at the position <code>pos</code>.  If the end of the
     * <code>Blob</code> value is reached while writing to the stream, then the length of the <code>Blob</code> value
     * will be increased to accommodate the extra bytes.
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the <code>BLOB</code>
     * value then the behavior is undefined. Some JDBC drivers may throw a <code>SQLException</code> while other drivers
     * may support this operation.
     *
     * @param pos the position in the <code>BLOB</code> value at which to start writing; the first position is 1
     * @return a <code>java.io.OutputStream</code> object to which data can be written
     * @throws SQLException if there is an error accessing the <code>BLOB</code> value or if pos is less than
     *                      1
     * @see #getBinaryStream
     * @since 1.4
     */
    public OutputStream setBinaryStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw ExceptionMapper.getSqlException("Invalid position in blob");
        }
        if (offset > 0) {
            byte[] tmp = new byte[length];
            System.arraycopy(data, offset, tmp, 0, length);
            data = tmp;
            offset = 0;
        }
        return new BlobOutputStream(this, (int) (pos - 1) + offset);
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code> object represents to be <code>len</code> bytes
     * in length.
     *
     * @param len the length, in bytes, to which the <code>BLOB</code> value that this <code>Blob</code> object
     *            represents should be truncated
     * @throws SQLException if there is an error accessing the <code>BLOB</code> value or if len is less than
     *                      0
     */
    public void truncate(final long len) throws SQLException {
        if (len >= 0 && len < this.length) {
            this.length = (int) len;
        }
    }

    /**
     * This method frees the <code>Blob</code> object and releases the resources that it holds. The object is invalid
     * once the <code>free</code> method is called.
     * <p>
     * After <code>free</code> has been called, any attempt to invoke a method other than <code>free</code> will result
     * in a <code>SQLException</code> being thrown.  If <code>free</code> is called multiple times, the subsequent calls
     * to <code>free</code> are treated as a no-op.
     */
    public void free() {
        this.data = new byte[0];
        this.offset = 0;
        this.length = 0;
    }

}
