package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.OutputStream;

final class ByteBufArray implements ByteBuf {
    
    public static final int DEFAULT_PAGE = 1024;
    
    private final byte[] array;
    
    protected int pos;
    
    private final int off; 
    
    public ByteBufArray() {
        array = new byte[DEFAULT_PAGE];
        pos = 0;
        off = 0;
    }
    
    public ByteBufArray(byte[] src, int off, int len) {
        array = src;
        pos = len;
        this.off = off;
    }
    
    public int remaining() {
        return DEFAULT_PAGE - pos;
    }
    
    @Override
    public void recycle() {
        pos = 0;
    }
    
    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(array, off, pos);
    }

    @Override
    public int pos() {
        return pos;
    }

    @Override
    public void incPos(int len) {
        pos += len;
    }

    @Override
    public byte[] array() {
        return array;
    }

    @Override
    public long address() {
        return 0;
    }

    @Override
    public void free() {
    }
    
}
