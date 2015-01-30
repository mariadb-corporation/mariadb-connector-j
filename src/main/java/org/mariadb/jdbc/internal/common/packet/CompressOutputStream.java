package org.mariadb.jdbc.internal.common.packet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;

public class CompressOutputStream extends OutputStream {

    private static final int MIN_COMPRESSION_SIZE = 16*1024;
    private static final int MAX_PACKET_LENGTH = 16*1024*1024-1;
    private static final float MIN_COMPRESSION_RATIO =0.9f;

    OutputStream baseStream;
    byte header[] = new byte[7];
    int seqNo = 0;

    public CompressOutputStream (OutputStream baseStream) {
        this.baseStream = baseStream;
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException{
        if (len > MAX_PACKET_LENGTH) {
            for(;;) {
               int bytesToWrite= Math.min(len, MAX_PACKET_LENGTH);
               write(bytes, off, bytesToWrite);
               off += bytesToWrite;
               len -= bytesToWrite;
               if(len == 0)
                   return;
            }
        }

        int compressedLength = len;
        int uncompressedLength = 0;

        if (bytes.length > MIN_COMPRESSION_SIZE) {
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              DeflaterOutputStream deflater = new DeflaterOutputStream(baos);
              deflater.write(bytes, off, len);
              deflater.finish();
              deflater.close();
              byte[] compressedBytes = baos.toByteArray();
              baos.close();
              if (compressedBytes.length < (int)(MIN_COMPRESSION_RATIO *len)) {
                 compressedLength = compressedBytes.length;
                 uncompressedLength = len;
                 bytes = compressedBytes;
                 off = 0;
              }
        }

        header[0] = (byte)(compressedLength & 0xff);
        header[1] = (byte)((compressedLength >> 8) & 0xff);
        header[2] = (byte)((compressedLength >> 16) & 0xff);
        header[3] = (byte) seqNo++;
        header[4] = (byte)(uncompressedLength & 0xff);
        header[5] = (byte)((uncompressedLength >> 8) & 0xff);
        header[6] = (byte)((uncompressedLength >> 16) & 0xff);

        baseStream.write(header);
        baseStream.write(bytes, off, compressedLength);

    }
    @Override
    public void write(byte[] bytes) throws IOException{
        if (bytes.length < 3)
          throw new AssertionError("Invalid call, at least 3 byte writes are required");
        write(bytes, 0, bytes.length);

    }
    @Override
    public void write(int b) throws IOException {
        throw new AssertionError("Invalid call, at least 3 byte writes are required");
    }

    @Override
    public void flush() throws IOException {
        baseStream.flush();
        seqNo = 0;
    }
}
