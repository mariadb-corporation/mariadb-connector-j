/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.Arrays;

/**
 * very untested and unused in general, use with greatest care
 * User: marcuse
 * Date: Feb 14, 2009
 * Time: 9:40:54 PM
 */
public class DrizzleBlob extends OutputStream implements Blob {
    private byte [] blobContent;
    private int actualSize;

    public DrizzleBlob(){
    }

    public DrizzleBlob(byte[] bytes) {
        this.blobContent=bytes;
        this.actualSize=bytes.length;
    }

    /**
     *
     * Writes the specified byte to this output stream. The general
     * contract for <code>write</code> is that one byte is written
     * to the output stream. The byte to be written is the eight
     * low-order bits of the argument <code>b</code>. The 24
     * high-order bits of <code>b</code> are ignored.
     * <p/>
     * Subclasses of <code>OutputStream</code> must provide an
     * implementation for this method.
     *
     * @param b the <code>byte</code>.
     * @throws java.io.IOException if an I/O error occurs. In particular,
     *                             an <code>IOException</code> may be thrown if the
     *                             output stream has been closed.
     */
    public void write(int b) throws IOException {
        if(this.blobContent == null)
            this.blobContent = new byte[100];
        
        if(this.blobContent.length == actualSize) {
            this.blobContent = Arrays.copyOf(this.blobContent, this.blobContent.length*2);
        }

        this.blobContent[actualSize++] = (byte) b;
    }

    /**
     * Returns the number of bytes in the <code>BLOB</code> value
     * designated by this <code>Blob</code> object.
     *
     * @return length of the <code>BLOB</code> in bytes
     * @throws java.sql.SQLException if there is an error accessing the
     *                               length of the <code>BLOB</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.2
     */
    public long length() throws SQLException {
        return actualSize;
    }

    /**
     * Retrieves all or part of the <code>BLOB</code>
     * value that this <code>Blob</code> object represents, as an array of
     * bytes.  This <code>byte</code> array contains up to <code>length</code>
     * consecutive bytes starting at position <code>pos</code>.
     *
     * @param pos    the ordinal position of the first byte in the
     *               <code>BLOB</code> value to be extracted; the first byte is at
     *               position 1
     * @param length the number of consecutive bytes to be copied; the value
     *               for length must be 0 or greater
     * @return a byte array containing up to <code>length</code>
     *         consecutive bytes from the <code>BLOB</code> value designated
     *         by this <code>Blob</code> object, starting with the
     *         byte at position <code>pos</code>
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> value; if pos is less than 1 or length is
     *                               less than 0
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @see #setBytes
     * @since 1.2
     */
    public byte[] getBytes(long pos, int length) throws SQLException {
        if(pos<1) throw new SQLException("Pos starts at 1");
        if(pos+length > actualSize) throw new SQLException("Out of bounds");
        return Arrays.copyOfRange(blobContent, (int)pos, (int) (pos + length));
    }

    /**
     * Retrieves the <code>BLOB</code> value designated by this
     * <code>Blob</code> instance as a stream.
     *
     * @return a stream containing the <code>BLOB</code> data
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> value
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @see #setBinaryStream
     * @since 1.2
     */
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(this.blobContent);
    }

    /**
     * Retrieves the byte position at which the specified byte array
     * <code>pattern</code> begins within the <code>BLOB</code>
     * value that this <code>Blob</code> object represents.  The
     * search for <code>pattern</code> begins at position
     * <code>start</code>.
     *
     * @param pattern the byte array for which to search
     * @param start   the position at which to begin searching; the
     *                first position is 1
     * @return the position at which the pattern appears, else -1
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> or if start is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.2
     */
    public long position(byte pattern[], long start) throws SQLException {
        if(start < 1) throw new SQLException("Start should be > 0, first position is 1...");
        if(start > actualSize) throw new SQLException("Start should be <= "+actualSize);
        long actualStart = start -1;
        for(int i= (int) actualStart;i<actualSize;i++) {
            if(blobContent[i] == pattern[0]){
                boolean isEqual=true;
                for(int j=1;j<pattern.length;j++) {
                    if(i+j >= actualSize) return -1;
                    if(blobContent[i+j]!=pattern[j]) {
                        isEqual=false;
                    }
                }
                if(isEqual) return i+1;
            }
        }
        return -1;
    }

    /**
     * Retrieves the byte position in the <code>BLOB</code> value
     * designated by this <code>Blob</code> object at which
     * <code>pattern</code> begins.  The search begins at position
     * <code>start</code>.
     *
     * @param pattern the <code>Blob</code> object designating
     *                the <code>BLOB</code> value for which to search
     * @param start   the position in the <code>BLOB</code> value
     *                at which to begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> value or if start is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.2
     */
    public long position(Blob pattern, long start) throws SQLException {
        return position(pattern.getBytes(1, (int) pattern.length()),start);
    }

    /**
     * Writes the given array of bytes to the <code>BLOB</code> value that
     * this <code>Blob</code> object represents, starting at position
     * <code>pos</code>, and returns the number of bytes written.
     * The array of bytes will overwrite the existing bytes
     * in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached
     * while writing the array of bytes, then the length of the <code>Blob</code>
     * value will be increased to accomodate the extra bytes.
     * <p/>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param pos   the position in the <code>BLOB</code> object at which
     *              to start writing; the first position is 1
     * @param bytes the array of bytes to be written to the <code>BLOB</code>
     *              value that this <code>Blob</code> object represents
     * @return the number of bytes written
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> value or if pos is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @see #getBytes
     * @since 1.4
     */
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        int bytesWritten=0;
        if(blobContent==null) {
            this.blobContent=new byte[(int) (pos+bytes.length)];
            for(int i= (int) pos;i<pos+bytes.length;i++) {
                this.blobContent[((int) (pos + i))] = bytes[i];
                bytesWritten++;
            }
        } else if(blobContent.length < pos+bytes.length) {
            for(int i= (int) pos;i<pos+bytes.length;i++) {
                this.blobContent[((int) (pos + i))] = bytes[i];
                bytesWritten++;
            }
        }
        this.actualSize+=bytesWritten;
        return bytesWritten;
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the
     * <code>BLOB</code> value that this <code>Blob</code> object represents
     * and returns the number of bytes written.
     * Writing starts at position <code>pos</code> in the <code>BLOB</code>
     * value; <code>len</code> bytes from the given byte array are written.
     * The array of bytes will overwrite the existing bytes
     * in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached
     * while writing the array of bytes, then the length of the <code>Blob</code>
     * value will be increased to accomodate the extra bytes.
     * <p/>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param pos    the position in the <code>BLOB</code> object at which
     *               to start writing; the first position is 1
     * @param bytes  the array of bytes to be written to this <code>BLOB</code>
     *               object
     * @param offset the offset into the array <code>bytes</code> at which
     *               to start reading the bytes to be set
     * @param len    the number of bytes to be written to the <code>BLOB</code>
     *               value from the array of bytes <code>bytes</code>
     * @return the number of bytes written
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> value or if pos is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @see #getBytes
     * @since 1.4
     */
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        int bytesWritten=0;
        if(blobContent==null) {
            this.blobContent=new byte[(int) (pos+bytes.length) - (len - offset)];
            for(int i= (int) pos+offset;i<len;i++) {
                this.blobContent[((int) (pos + i))] = bytes[i];
                bytesWritten++;
            }
        } else if(this.blobContent.length < (pos+bytes.length) - (len - offset)) {
            for(int i= (int) pos+offset;i<len;i++) {
                this.blobContent[((int) (pos + i))] = bytes[i];
                bytesWritten++;
            }
        }
        this.actualSize+=bytesWritten;
        return bytesWritten;
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code>
     * value that this <code>Blob</code> object represents.  The stream begins
     * at position <code>pos</code>.
     * The  bytes written to the stream will overwrite the existing bytes
     * in the <code>Blob</code> object starting at the position
     * <code>pos</code>.  If the end of the <code>Blob</code> value is reached
     * while writing to the stream, then the length of the <code>Blob</code>
     * value will be increased to accomodate the extra bytes.
     * <p/>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param pos the position in the <code>BLOB</code> value at which
     *            to start writing; the first position is 1
     * @return a <code>java.io.OutputStream</code> object to which data can
     *         be written
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> value or if pos is less than 1
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @see #getBinaryStream
     * @since 1.4
     */
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return new DrizzleBlob(Arrays.copyOfRange(blobContent, (int) pos,blobContent.length));
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code>
     * object represents to be <code>len</code> bytes in length.
     * <p/>
     * <b>Note:</b> If the value specified for <code>pos</code>
     * is greater then the length+1 of the <code>BLOB</code> value then the
     * behavior is undefined. Some JDBC drivers may throw a
     * <code>SQLException</code> while other drivers may support this
     * operation.
     *
     * @param len the length, in bytes, to which the <code>BLOB</code> value
     *            that this <code>Blob</code> object represents should be truncated
     * @throws java.sql.SQLException if there is an error accessing the
     *                               <code>BLOB</code> value or if len is less than 0
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.4
     */
    public void truncate(long len) throws SQLException {
        this.blobContent = Arrays.copyOf(this.blobContent, (int) len);
        this.actualSize= (int) len;
    }

    /**
     * This method frees the <code>Blob</code> object and releases the resources that
     * it holds. The object is invalid once the <code>free</code>
     * method is called.
     * <p/>
     * After <code>free</code> has been called, any attempt to invoke a
     * method other than <code>free</code> will result in a <code>SQLException</code>
     * being thrown.  If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     * <p/>
     *
     * @throws java.sql.SQLException if an error occurs releasing
     *                               the Blob's resources
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.6
     */
    public void free() throws SQLException {
        this.blobContent=null;
        this.actualSize=0;
    }

    /**
     * Returns an <code>InputStream</code> object that contains a partial <code>Blob</code> value,
     * starting  with the byte specified by pos, which is length bytes in length.
     *
     * @param pos    the offset to the first byte of the partial value to be retrieved.
     *               The first byte in the <code>Blob</code> is at position 1
     * @param length the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial <code>Blob</code> value can be read.
     * @throws java.sql.SQLException if pos is less than 1 or if pos is greater than the number of bytes
     *                               in the <code>Blob</code> or if pos + length is greater than the number of bytes
     *                               in the <code>Blob</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.6
     */
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(Arrays.copyOfRange(blobContent,(int)pos,(int)length));
    }
}
