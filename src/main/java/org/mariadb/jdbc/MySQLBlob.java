/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
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

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.SQLExceptionMapper;
import org.mariadb.jdbc.internal.common.Utils;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Represents a Blob.
 * <p/>
 * User: marcuse Date: Feb 14, 2009 Time: 9:40:54 PM
 */
public  class MySQLBlob implements Blob, Serializable {
    /**
     * the actual blob content.
     */
    protected byte[] blobContent;
    /**
     * the size of the blob.
     */
    protected int actualSize;

    private void writeObject(java.io.ObjectOutputStream out)
         throws IOException {
        out.writeInt(actualSize);
        if(actualSize > 0) {
            out.write(blobContent, 0, actualSize);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
         throws IOException, ClassNotFoundException {
        actualSize = in.readInt();
        blobContent = new byte[actualSize];
        if (actualSize > 0) {
            in.readFully(blobContent, 0, actualSize);
        }
    }

    /**
     * How big the blob should be initially.
     */
    private static final int INITIAL_BLOB_CONTENT_SIZE = 100;

    /**
     * creates an empty blob.
     */
    public MySQLBlob() {
        blobContent = new byte[0];
    }

    /**
     * creates a blob with content.
     *
     * @param bytes the content for the blob.
     */
    public MySQLBlob(byte[] bytes) {
        if (bytes == null)
           throw new AssertionError("byte array is null");
        this.blobContent = bytes;
        this.actualSize = bytes.length;
    }



    /**
     * Returns the number of bytes in the <code>BLOB</code> value designated by this <code>Blob</code> object.
     *
     * @return length of the <code>BLOB</code> in bytes
     * @throws java.sql.SQLException if there is an error accessing the length of the <code>BLOB</code>
     */
    public long length() throws SQLException {
        return actualSize;
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
     *         designated by this <code>Blob</code> object, starting with the byte at position <code>pos</code>
     * @throws java.sql.SQLException if there is an error accessing the <code>BLOB</code> value; if pos is less than 1
     *                               or length is less than 0
     * @see #setBytes
     * @since 1.2
     */
    public byte[] getBytes(final long pos, final int length) throws SQLException {
        if (pos < 1) {
            throw SQLExceptionMapper.getSQLException("Pos starts at 1");
        }
        final int arrayPos = (int) (pos - 1);
        return Utils.copyRange(blobContent, arrayPos, arrayPos + length);
    }

    /**
     * Retrieves the <code>BLOB</code> value designated by this <code>Blob</code> instance as a stream.
     *
     * @return a stream containing the <code>BLOB</code> data
     * @throws SQLException if something went wrong
     * @see #setBinaryStream
     */
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(this.blobContent);
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
            throw SQLExceptionMapper.getSQLException("Start should be > 0, first position is 1.");
        }
        if (start > actualSize) {
            throw SQLExceptionMapper.getSQLException("Start should be <= " + actualSize);
        }
        final long actualStart = start - 1;
        for (int i = (int) actualStart; i < actualSize; i++) {
            if (blobContent[i] == pattern[0]) {
                boolean isEqual = true;
                for (int j = 1; j < pattern.length; j++) {
                    if (i + j >= actualSize) {
                        return -1;
                    }
                    if (blobContent[i + j] != pattern[j]) {
                        isEqual = false;
                    }
                }
                if (isEqual) {
                    return i + 1;
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
     * value will be increased to accomodate the extra bytes.
     * <p/>
     * <b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the <code>BLOB</code>
     * value then the behavior is undefined. Some JDBC drivers may throw a <code>SQLException</code> while other drivers
     * may support this operation.
     *
     * @param pos   the position in the <code>BLOB</code> object at which to start writing; the first position is 1
     * @param bytes the array of bytes to be written to the <code>BLOB</code> value that this <code>Blob</code> object
     *              represents
     * @return the number of bytes written
     * @see #getBytes
     * @since 1.4
     */
    public int setBytes(final long pos, final byte[] bytes) throws SQLException {
        final int arrayPos = (int) pos - 1;
        final int bytesWritten;

        if (blobContent == null) {
            this.blobContent = new byte[arrayPos + bytes.length];
            bytesWritten = blobContent.length;
            this.actualSize = bytesWritten;
        } else if (blobContent.length > arrayPos + bytes.length) {
            bytesWritten = bytes.length;
        } else {
            blobContent = Utils.copyWithLength(blobContent, arrayPos + bytes.length);
            actualSize = blobContent.length;
            bytesWritten = bytes.length;
        }

        System.arraycopy(bytes, 0, this.blobContent, arrayPos, bytes.length);

        return bytesWritten;
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the <code>BLOB</code> value that this
     * <code>Blob</code> object represents and returns the number of bytes written. Writing starts at position
     * <code>pos</code> in the <code>BLOB</code> value; <code>len</code> bytes from the given byte array are written.
     * The array of bytes will overwrite the existing bytes in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached while writing the array of bytes, then
     * the length of the <code>Blob</code> value will be increased to accomodate the extra bytes.
     * <p/>
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
     * @throws java.sql.SQLException if there is an error accessing the <code>BLOB</code> value or if pos is less than
     *                               1
     * @see #getBytes
     */
    public int setBytes(final long pos,
                        final byte[] bytes,
                        final int offset,
                        final int len) throws SQLException {
        int bytesWritten = 0;
        if (blobContent == null) {
            this.blobContent = new byte[(int) (pos + bytes.length) - (len - offset)];
            for (int i = (int) pos + offset; i < len; i++) {
                this.blobContent[((int) (pos + i))] = bytes[i];
                bytesWritten++;
            }
        } else if (this.blobContent.length < (pos + bytes.length) - (len - offset)) {
            for (int i = (int) pos + offset; i < len; i++) {
                this.blobContent[((int) (pos + i))] = bytes[i];
                bytesWritten++;
            }
        }
        this.actualSize += bytesWritten;
        return bytesWritten;
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code> value that this <code>Blob</code> object
     * represents.  The stream begins at position <code>pos</code>. The  bytes written to the stream will overwrite the
     * existing bytes in the <code>Blob</code> object starting at the position <code>pos</code>.  If the end of the
     * <code>Blob</code> value is reached while writing to the stream, then the length of the <code>Blob</code> value
     * will be increased to accomodate the extra bytes.
     * <p/>
     * <b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the <code>BLOB</code>
     * value then the behavior is undefined. Some JDBC drivers may throw a <code>SQLException</code> while other drivers
     * may support this operation.
     *
     * @param pos the position in the <code>BLOB</code> value at which to start writing; the first position is 1
     * @return a <code>java.io.OutputStream</code> object to which data can be written
     * @throws java.sql.SQLException if there is an error accessing the <code>BLOB</code> value or if pos is less than
     *                               1
     * @see #getBinaryStream
     * @since 1.4
     */
    public OutputStream setBinaryStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw SQLExceptionMapper.getSQLException("Invalid position in blob");
        }
        return new BlobOutputStream(this, (int)(pos-1));
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code> object represents to be <code>len</code> bytes
     * in length.
     * <p/>
     * <b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the <code>BLOB</code>
     * value then the behavior is undefined. Some JDBC drivers may throw a <code>SQLException</code> while other drivers
     * may support this operation.
     *
     * @param len the length, in bytes, to which the <code>BLOB</code> value that this <code>Blob</code> object
     *            represents should be truncated
     * @throws java.sql.SQLException if there is an error accessing the <code>BLOB</code> value or if len is less than
     *                               0
     */
    public void truncate(final long len) throws SQLException {
        this.blobContent = Utils.copyWithLength(this.blobContent, (int) len);
        this.actualSize = (int) len;
    }

    /**
     * This method frees the <code>Blob</code> object and releases the resources that it holds. The object is invalid
     * once the <code>free</code> method is called.
     * <p/>
     * After <code>free</code> has been called, any attempt to invoke a method other than <code>free</code> will result
     * in a <code>SQLException</code> being thrown.  If <code>free</code> is called multiple times, the subsequent calls
     * to <code>free</code> are treated as a no-op.
     * <p/>
     */
    public void free() {
        this.blobContent = null;
        this.actualSize = 0;
    }

    /**
     * Returns an <code>InputStream</code> object that contains a partial <code>Blob</code> value, starting  with the
     * byte specified by pos, which is length bytes in length.
     *
     * @param pos    the offset to the first byte of the partial value to be retrieved. The first byte in the
     *               <code>Blob</code> is at position 1
     * @param length the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial <code>Blob</code> value can be read.
     * @throws java.sql.SQLException if pos is less than 1 or if pos is greater than the number of bytes in the
     *                               <code>Blob</code> or if pos + length is greater than the number of bytes in the
     *                               <code>Blob</code>
     */
    public InputStream getBinaryStream(final long pos, final long length) throws SQLException {
        if (pos < 1 || pos > actualSize || pos + length > actualSize) {
            throw SQLExceptionMapper.getSQLException("Out of range");
        }

        return new ByteArrayInputStream(Utils.copyRange(blobContent,
                (int) pos,
                (int) length));
    }
}

/**
 * Output stream for the blob
 */
class BlobOutputStream  extends OutputStream{

    int pos;
    MySQLBlob blob;
    public BlobOutputStream(MySQLBlob blob, int pos) {
        this.blob = blob;
        this.pos = pos;
    }

    @Override
    public void write(int b) throws IOException {

       if(this.pos >= blob.blobContent.length){
           int newLen = Math.max(pos,2*blob.blobContent.length + 1);
           byte[] tmp = new byte[2*blob.blobContent.length + 1];
           System.arraycopy(blob.blobContent,0, tmp, 0, blob.blobContent.length);
           blob.blobContent = tmp;
       }
       blob.blobContent[pos] = (byte)b;
       pos++;
       if (pos > blob.actualSize) {
           blob.actualSize = pos;
       }
    }

    @Override
    public void write(byte [] buf, int off, int len) {
       if (pos + len  >= blob.blobContent.length) {
           int newLen = Math.max(2*(pos + len + 1),1024);
           byte[] tmp = new byte[newLen];
           System.arraycopy(blob.blobContent,0, tmp, 0, blob.blobContent.length);
           blob.blobContent = tmp;
       }
       System.arraycopy(buf, off, blob.blobContent, pos, len);
       pos+= len;
       if (pos > blob.actualSize) {
           blob.actualSize = pos;
       }
    }

    @Override
    public void write(byte[] buf) {
        write(buf, 0, buf.length);
    }
}