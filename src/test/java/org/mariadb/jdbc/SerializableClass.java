package org.mariadb.jdbc;

import java.io.Serializable;

public class SerializableClass implements Serializable {
    private static final long serialVersionUID = 4625959895106940108L;
    private final String val;
    private final int val2;

    public SerializableClass(String val, int val2) {
        this.val = val;
        this.val2 = val2;
    }

    public String getVal() {
        return val;
    }

    public int getVal2() {
        return val2;
    }
}