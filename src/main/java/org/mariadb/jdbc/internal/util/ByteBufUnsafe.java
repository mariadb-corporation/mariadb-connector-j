package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.OutputStream;

import sun.misc.Unsafe;

@SuppressWarnings("all")
public final class ByteBufUnsafe implements ByteBuf {
    
    private static final Unsafe UNSAFE = UnsafeUtil.unsafe();
    
    private final long adr;
    
    private final int max;
    private int pos;
    
    private static final int DEFAULT_PAGE = 4096;
    
    private final byte[] cache;
    
    public ByteBufUnsafe() {
        adr = UNSAFE.allocateMemory(DEFAULT_PAGE);
        pos = 0;
        max = DEFAULT_PAGE;
        cache = new byte[DEFAULT_PAGE];
    }
    
    public ByteBufUnsafe(int index) {
        max = DEFAULT_PAGE * (index+1);
        adr = UNSAFE.allocateMemory(max);
        pos = 0;
        cache = new byte[DEFAULT_PAGE];
    }
    
    @Override
    public int pos() {
        return pos;
    }
    
    @Override
    public int remaining() {
        return max - pos;
    }
    
    @Override
    public void recycle() {
        pos = 0;
    }
    
    @Override
    public void writeTo(OutputStream os) throws IOException {
        final byte[] values = cache;
        if (pos < DEFAULT_PAGE) {
            UNSAFE.copyMemory(null, adr, values, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, pos);
            os.write(values, 0, pos);
        } else {
            int total = pos;
            long adr = this.adr;
            int read;
            while (total > 0) {
                read = Math.min(total, DEFAULT_PAGE);
                UNSAFE.copyMemory(null, adr, values, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, read);
                os.write(values, 0, read);
                total -= read;
                adr += read;
            }
        }
    }
    
    @Override
    public byte[] array() {
        final byte[] values;
        if (max == DEFAULT_PAGE) {
            values = cache;
        } else {
            values = new byte[pos];    
        }
        UNSAFE.copyMemory(null, adr, values, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, pos);
        return values;
    }
    
    @Override
    public long address() {
        return adr;
    }
    
    @Override
    public void free() {
        UNSAFE.freeMemory(adr);
    }

    @Override
    public int pos(int inc) {
        int pos = this.pos;
        this.pos = pos + inc;
        return pos;
    }
    
    @Override
    public void pos(long address) {
        this.pos = (int) (address - this.adr);
    }

}
