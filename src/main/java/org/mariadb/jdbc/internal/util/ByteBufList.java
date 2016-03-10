package org.mariadb.jdbc.internal.util;

final class ByteBufList {
    
    final ByteBufUnsafe first = new ByteBufUnsafe();
    
    private ByteBuf[] elem = new ByteBuf[8];
    
    private int elemIdx = 0;
    
    public void recylce() {
        first.recycle();
        elemIdx = 0;
        for (int i = 0; i < elem.length; i++) {
            if (elem[i] == null) {
                break;
            }
            elem[i].free();
            elem[i] = null;
        }
        if (elem.length > 16) {
            elem = new ByteBuf[8];
        }
    }
    
    public int size() {
        return 1 + elemIdx;
    }
    
    public ByteBuf get(int idx) {
        if (idx == 0) {
            return first;
        } else {
            return elem[idx - 1];
        }
    }
    
    public void add(ByteBuf buf) {
        if (elemIdx == elem.length) {
            ByteBuf[] tmp = new ByteBuf[elem.length * 2];
            System.arraycopy(elem, 0, tmp, 0, elem.length);
            elem = tmp;
        }
        elem[elemIdx++] = buf;
        
    }

    @Override
    protected void finalize() throws Throwable {
        this.first.free();
        recylce();
        super.finalize();
    }
    
}
