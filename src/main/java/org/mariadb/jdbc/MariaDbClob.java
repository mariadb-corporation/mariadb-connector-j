package org.mariadb.jdbc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

public class MariaDbClob extends MariaDbBlob implements Clob, NClob, Serializable {
    private static final long serialVersionUID = -2006825230517923067L;

    public MariaDbClob(byte[] bytes) {
        super(bytes);
    }

    public MariaDbClob() {
        super();
    }

    /**
     * ToString implementation.
     * @return string value of blob content.
     */
    public String toString() {
        try {
            return new String(blobContent, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Get sub string.
     * @param pos position
     * @param length length of sub string
     * @return substring
     * @throws SQLException if position is not possible
     */
    public String getSubString(long pos, int length) throws SQLException {
        try {
            return toString().substring((int) pos - 1, (int) pos - 1 + length);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream() throws SQLException {
        return new StringReader(toString());
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        String sub = toString().substring((int) pos - 1, (int) pos - 1 + (int) length);
        return new StringReader(sub);
    }

    /**
     * Set character stream.
     * @param pos position
     * @return writer
     * @throws SQLException if position is invalid
     */
    public Writer setCharacterStream(long pos) throws SQLException {
        int bytePosition = utf8Position((int) pos - 1);
        OutputStream stream = setBinaryStream(bytePosition + 1);
        return new OutputStreamWriter(stream, StandardCharsets.UTF_8);
    }

    public InputStream getAsciiStream() throws SQLException {
        return getBinaryStream();
    }

    public long position(String searchstr, long start) throws SQLException {
        return toString().indexOf(searchstr, (int) start - 1);
    }

    public long position(Clob searchstr, long start) throws SQLException {
        return position(searchstr.toString(), start);
    }

    /**
     * Convert character position into byte position in UTF8 byte array.
     *
     * @param charPosition charPosition
     * @return byte position
     */
    private int utf8Position(int charPosition) {
        int pos = 0;
        for (int i = 0; i < charPosition; i++) {
            int content = blobContent[pos] & 0xff;
            if (content < 0x80) {
                pos += 1;
            } else if (content < 0xC2) {
                throw new AssertionError("invalid UTF8");
            } else if (content < 0xE0) {
                pos += 2;
            } else if (content < 0xF0) {
                pos += 3;
            } else if (content < 0xF8) {
                pos += 4;
            } else {
                throw new AssertionError("invalid UTF8");
            }
        }
        return pos;
    }

    /**
     * Set String.
     * @param pos position
     * @param str string
     * @return string length
     * @throws SQLException if UTF-8 convertion failed
     */
    public int setString(long pos, String str) throws SQLException {
        int bytePosition = utf8Position((int) pos - 1);
        try {
            super.setBytes(bytePosition + 1, str.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            //eat exception, UTF-8 is a known charset
        }
        return str.length();
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return setString(pos, str.substring(offset, offset + len));
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        return setBinaryStream(utf8Position((int) pos - 1) + 1);
    }

    /**
     * Return character length of the Clob. Assume UTF8 encoding.
     */
    @Override
    public long length() {
        long len = 0;
        for (int i = 0; i < actualSize; ) {
            int content = blobContent[i] & 0xff;
            if (content < 0x80) {
                i += 1;
            } else if (content < 0xC2) {
                throw new AssertionError("invalid UTF8");
            } else if (content < 0xE0) {
                i += 2;
            } else if (content < 0xF0) {
                i += 3;
            } else if (content < 0xF8) {
                i += 4;
            } else {
                throw new AssertionError("invalid UTF8");
            }
            len++;
        }
        return len;
    }
}
