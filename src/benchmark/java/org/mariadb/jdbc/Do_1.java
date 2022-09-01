// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.ResultSet;
import java.sql.Statement;

public class Do_1 extends Common {

    @Benchmark
    public int run(MyState state) throws Throwable {
        try (Statement st = state.connectionText.createStatement()) {
            return st.executeUpdate("DO 1");
        }
    }

}
