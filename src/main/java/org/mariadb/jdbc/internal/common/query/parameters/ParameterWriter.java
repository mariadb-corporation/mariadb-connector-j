package org.mariadb.jdbc.internal.common.query.parameters;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Helper class for serializing query parameters
 */
public class ParameterWriter {
    static final byte[] BINARY_INTRODUCER = {'_','b','i','n','a','r','y',' ','\''};
    static final Charset UTF8 = Charset.forName("UTF-8");
    static final byte[] QUOTE = {'\''};

    private static void writeBytesEscaped(OutputStream out, byte[] bytes, int count, boolean noBackslashEscapes)
    throws IOException {
        if (noBackslashEscapes) {
            for (int i = 0; i < count; i++) {
                byte b = bytes[i];
                switch(b) {
                case '\'':
                    out.write('\'');
                    out.write(b);
                    break;
                default:
                    out.write(b);
                }
            }
        } else {
            for (int i = 0; i < count; i++) {
                byte b = bytes[i];
                switch(b) {
                case '\\':
                case '\'':
                case '"':
                case 0:
                    out.write('\\');
                    out.write(b);
                    break;
                default:
                    out.write(b);
                }
            }
        }
    }

    private static void writeBytesEscaped(OutputStream out, byte[] bytes, boolean noBackslashEscapes) throws IOException {
        writeBytesEscaped(out, bytes, bytes.length, noBackslashEscapes);
    }

    public static void write(OutputStream out, byte[] bytes, boolean noBackslashEscapes) throws IOException {
        out.write(BINARY_INTRODUCER);
        writeBytesEscaped(out, bytes, noBackslashEscapes);
        out.write(QUOTE);
    }

    public static void write(OutputStream out, String s, boolean noBackslashEscapes) throws IOException {
        byte[] bytes = s.getBytes(UTF8);
        out.write(QUOTE);
        writeBytesEscaped(out,bytes, noBackslashEscapes);
        out.write(QUOTE);
    }

    public static void write(OutputStream out, InputStream is, boolean noBackslashEscapes, boolean isText) throws IOException {
        if (isText) {
            out.write(QUOTE);
        } else {
            out.write(BINARY_INTRODUCER);
        }
        byte[] buffer = new byte[1024];
        int len;
        while ((len = is.read(buffer)) >= 0) {
            writeBytesEscaped(out, buffer, len, noBackslashEscapes);
        }
        out.write(QUOTE);
    }

    public static void write(OutputStream out, InputStream is, long length, boolean noBackslashEscapes, boolean isText) throws IOException {
        if (isText) {
            out.write(QUOTE);
        } else {
            out.write(BINARY_INTRODUCER);
        }
        byte[] buffer = new byte[1024];
        long bytesLeft = length;
        int len;

        for (;;) {
            int bytesToRead = (int)Math.min(bytesLeft, buffer.length);
            if(bytesToRead == 0)
                break;
            len = is.read(buffer,0, bytesToRead);
            if (len <= 0)
                break;
            writeBytesEscaped(out, buffer, len, noBackslashEscapes);
            bytesLeft -= len;
        }
        out.write(QUOTE);
    }

    public static void write(OutputStream out, java.io.Reader reader, boolean noBackslashEscapes) throws IOException {
        out.write(QUOTE);
        char[] buffer = new char[1024];
        int len;
        while ((len = reader.read(buffer)) >= 0) {
            writeBytesEscaped(out, new String(buffer,0, len).getBytes(UTF8), noBackslashEscapes);
        }
        out.write(QUOTE);
    }

    public static void write(OutputStream out, java.io.Reader reader, long length, boolean noBackslashEscapes)
    throws IOException {
        out.write(QUOTE);
        char[] buffer = new char[1024];
        long charsLeft = length;
        int len;

        for (;;) {
            int charsToRead = (int)Math.min(charsLeft, buffer.length);
            if(charsToRead == 0)
                break;
            len = reader.read(buffer,0, charsToRead);
            if (len <= 0)
                break;
            byte[] bytes = new String(buffer, 0, len).getBytes(UTF8);
            writeBytesEscaped(out, bytes, bytes.length, noBackslashEscapes);
            charsLeft -= len;
        }
        out.write(QUOTE);
    }

    public static void write(OutputStream out, int i) throws IOException {
        out.write(String.valueOf(i).getBytes());
    }

    public static void write(OutputStream out, long l) throws IOException {
        out.write(String.valueOf(l).getBytes());
    }

    public static void write(OutputStream out, double d) throws IOException {
        out.write(String.valueOf(d).getBytes());
    }

    public static void write(OutputStream out, BigDecimal bd) throws IOException {
        out.write(bd.toPlainString().getBytes());
    }

    public static void writeDate(OutputStream out, java.util.Date date, Calendar calendar) throws IOException {
        out.write(QUOTE);
        String dateString;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        if (calendar != null) {
            sdf.setCalendar(calendar);
        }
        dateString = sdf.format(date);
        out.write(dateString.getBytes());
        out.write(QUOTE);
    }

    static void formatMicroseconds(OutputStream out, int microseconds, boolean writeFractionalSeconds) throws IOException {
        if (microseconds == 0 || !writeFractionalSeconds)
            return;
        out.write('.');
        int factor = 100000;
        while(microseconds > 0) {
            int dig = microseconds / factor;
            out.write('0' + dig);
            microseconds -= dig*factor;
            factor /= 10;
        }
    }


    public static void writeTimestamp(OutputStream out, Timestamp ts, Calendar calendar, boolean writeFractionalSeconds)
    throws IOException {
        out.write(QUOTE);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (calendar != null) {
            sdf.setCalendar(calendar);
        }
        String dateString = sdf.format(ts);
        out.write(dateString.getBytes());
        formatMicroseconds(out, ts.getNanos() / 1000, writeFractionalSeconds);
        out.write(QUOTE);
    }


    public static void writeTime(OutputStream out, Time time, Calendar calendar, boolean writeFractionalSeconds)
    throws IOException {
        out.write(QUOTE);
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        if (calendar != null) {
            sdf.setCalendar(calendar);
        }
        String dateString = sdf.format(time);
        out.write(dateString.getBytes());
        int microseconds =  (int)(time.getTime()%1000) * 1000;
        formatMicroseconds(out, microseconds, writeFractionalSeconds);
        out.write(QUOTE);
    }

    public static void writeObject(OutputStream out, Object o, boolean noBackslashEscapes)throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        write(out,baos.toByteArray(), noBackslashEscapes);
    }

}
