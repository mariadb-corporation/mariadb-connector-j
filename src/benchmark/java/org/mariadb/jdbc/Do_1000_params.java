// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Do_1000_params extends Common {

    private static final String sql;
    static {
        StringBuilder sb = new StringBuilder("do ?");
        for (int i = 1; i < 1000; i++) {
            sb.append(",?");
        }
        sql = sb.toString();
    }

    @Benchmark
    public int text(MyState state) throws Throwable {
        return run(state.connectionText);
    }

    @Benchmark
    public int binary(MyState state) throws Throwable {
        return run(state.connectionBinary);
    }

    private int run(Connection con) throws Throwable {

        try (PreparedStatement st = con.prepareStatement(sql)) {
            for (int i = 1; i <= 1000; i++) {
                st.setInt(i, i);
            }
            return st.executeUpdate();
        }
    }
}
