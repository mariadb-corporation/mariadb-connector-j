package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class ByteBufStream implements ByteBuf {
    
    private final InputStream is;
    
    private final int pos;
    
    public ByteBufStream(InputStream is, int pos) {
        this.is = is;
        this.pos = pos;
    }
    
    public ByteBufStream(InputStream is) throws IOException {
        this.is = is;
        this.pos = is.available();
    }
    
    @Override
    public int pos() {
        return pos;
    }
    
    @Override
    public int remaining() {
        return 0;
    }
    
    @Override
    public void recycle() {
    }
    
    @Override
    public void writeTo(OutputStream os) throws IOException {
        byte[] buf = new byte[2048];
        int read = pos;
        int len;
        while (read > 0) {
            len = is.read(buf, 0, Math.min(read, 2048));
            if (read != -1) {
                os.write(buf, 0, len);
            }
            read -= len;
        }
    }
    
    @Override
    public byte[] array() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long address() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void free() {
    }

    @Override
    public int pos(int inc) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void pos(long address) {
        throw new UnsupportedOperationException();
    }
    
}
