package org.mariadb.jdbc.internal.mysql;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Types;
import java.util.EnumSet;
import java.util.Set;

import org.mariadb.jdbc.internal.common.ColumnInformation;
import org.mariadb.jdbc.internal.common.DataType;
import org.mariadb.jdbc.internal.common.packet.RawPacket;
import org.mariadb.jdbc.internal.common.packet.buffer.Reader;
import org.mariadb.jdbc.internal.common.queryresults.ColumnFlags;

public class MySQLColumnInformation implements ColumnInformation {
    RawPacket buffer;
    private short charsetNumber;
    private long length;
    private DataType type;
    private byte decimals;
    private Set<ColumnFlags> flags;

    public static MySQLColumnInformation create(String name, MySQLType.Type type) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            for (int i = 0; i < 4; i++) {
                baos.write(new byte[] {1,0}); // catalog, empty string
            }
            for (int i = 0 ; i < 2; i ++) {
                baos.write(new byte[] {(byte)name.length()});
                baos.write(name.getBytes());
            }
            baos.write(0xc);
            baos.write(new byte[]{33,0});  /* charset  = UTF8 */
            baos.write(new byte[]{1, 0 ,0, 0});  /*  length */
            baos.write(MySQLType.toServer(type.getSqlType()));
            baos.write(new byte[]{0,0});   /* flags */
            baos.write(0); /* decimals */
            baos.write(new byte[]{0,0});   /* filler */
            return new MySQLColumnInformation(new RawPacket(ByteBuffer.wrap(baos.toByteArray()).order(ByteOrder.LITTLE_ENDIAN),0));
        }  catch (IOException ioe) {
            throw new RuntimeException("unexpected condition",ioe);
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
        type = MySQLType.fromServer(reader.readByte());
        flags = parseFlags(reader.readShort());
        decimals = reader.readByte();


        int sqlType= type.getSqlType();

        if ((sqlType == Types.BLOB || sqlType == Types.VARBINARY || sqlType == Types.BINARY || sqlType == Types.LONGVARBINARY )
                && !isBinary()) {
           /* MySQL Text datatype */
           type = new MySQLType(MySQLType.Type.VARCHAR);
        }
    }

    private static Set<ColumnFlags> parseFlags(final short i) {
          final Set<ColumnFlags> retFlags = EnumSet.noneOf(ColumnFlags.class);
          for (final ColumnFlags fieldFlag : ColumnFlags.values()) {
              if ((i & fieldFlag.flag()) == fieldFlag.flag()) {
                  retFlags.add(fieldFlag);
              }
          }
          return retFlags;
    }

    private String getString(int idx) {
        try  {
            buffer.getByteBuffer().reset();
            buffer.getByteBuffer().mark();
            Reader reader = new Reader(buffer);
            for(int i = 0; i < idx ; i++) {
               reader.skipLengthEncodedBytes();
            }
            return new String(reader.getLengthEncodedBytes(),"UTF-8");
        }  catch (Exception e) {
            throw new RuntimeException("this does not happen",e);
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

    public DataType getType() {
        return type;
    }

    public byte getDecimals() {
        return decimals;
    }

    public Set<ColumnFlags> getFlags() {
        return flags;
    }

    public boolean isSigned() {
        return !flags.contains(ColumnFlags.UNSIGNED);
    }

    public boolean isBinary() {
       return (getCharsetNumber() == 63);
    }

}