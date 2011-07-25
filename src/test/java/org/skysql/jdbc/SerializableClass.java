package org.skysql.jdbc;

import java.io.Serializable;

public class SerializableClass implements Serializable {
        private final String val;
        private final int val2;
        public SerializableClass(String v, int v2) {
            this.val=v;
            this.val2=v2;
        }

        public String getVal() {
            return val;
        }

        public int getVal2() {
            return val2;
        }
    }