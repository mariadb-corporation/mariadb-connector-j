package org.drizzle.jdbc;

import java.io.Serializable;

public class SerializableClass implements Serializable {
        private String val;
        private int val2;
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