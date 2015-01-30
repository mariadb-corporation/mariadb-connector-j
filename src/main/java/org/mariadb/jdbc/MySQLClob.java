package org.mariadb.jdbc;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

public class MySQLClob extends MySQLBlob implements Clob, NClob, Serializable {
	private static final long serialVersionUID = -2006825230517923067L;

	public String toString() {
      try {
        return new String(blobContent, "UTF-8");
      }
      catch(Exception e) {
          throw new AssertionError(e);
      }
    }

    public MySQLClob(byte[] bytes) {
        super(bytes);
    }
    public MySQLClob() {
        super();
    }

    public String getSubString(long pos, int length) throws SQLException {
        try {
            return toString().substring((int) pos - 1, (int) pos - 1 + length);
        }
        catch(Exception e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream() throws SQLException {
        return new StringReader(toString());
    }

    public InputStream getAsciiStream() throws SQLException {
        return getBinaryStream();
    }

    public long position(String searchstr, long start) throws SQLException {
        return toString().indexOf(searchstr, (int)start -1);
    }

    /**
     * Convert character position into byte position in UTF8 byte array.
     * @param charPosition
     * @return byte position
     */
    private int UTF8position(int charPosition) {
        int pos = 0;
        for(int i = 0; i < charPosition; i++)  {
            int c = blobContent[pos] & 0xff;
            if(c < 0x80) {
                pos += 1;
            }
            else if (c < 0xC2) {
                throw new AssertionError("invalid UTF8");
            }
            else if (c < 0xE0) {
                pos += 2;
            }
            else if (c < 0xF0) {
                pos += 3;
            }
            else if (c < 0xF8) {
                pos += 4;
            }
            else {
                throw new AssertionError("invalid UTF8");
            }
        }
        return pos;
    }

    public long position(Clob searchstr, long start) throws SQLException {
        return position(searchstr.toString(), start);
    }

    public int setString(long pos, String str) throws SQLException {
        int bytePosition = UTF8position((int)pos-1);
        super.setBytes(bytePosition+1, str.getBytes(Charset.forName("UTF-8")));
        return str.length();
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return setString(pos, str.substring(offset, offset+len));
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
       return setBinaryStream(UTF8position((int)pos-1)+1);
    }

    public Writer setCharacterStream(long pos) throws SQLException {
        int bytePosition  = UTF8position((int)pos-1);
        OutputStream stream = setBinaryStream(bytePosition+1);
        return new OutputStreamWriter(stream, Charset.forName("UTF-8"));
    }


    public Reader getCharacterStream(long pos, long length) throws SQLException {
       String sub = toString().substring((int)pos -1, (int)pos -1 + (int)length);
       return new StringReader(sub);
    }

    @Override
    /**
     * return character length of the Clob. Assume UTF8 encoding.
     */
    public long length() {
       long len = 0;
       for(int i = 0; i < actualSize;)  {
            int c = blobContent[i] & 0xff;
            if(c < 0x80) {
                i += 1;
            }
            else if (c < 0xC2) {
                throw new AssertionError("invalid UTF8");
            }
            else if (c < 0xE0) {
                i += 2;
            }
            else if (c < 0xF0) {
                i += 3;
            }
            else if (c < 0xF8) {
                i += 4;
            }
            else {
                throw new AssertionError("invalid UTF8");
            }
            len++;
        }
        return len;
    }
}
