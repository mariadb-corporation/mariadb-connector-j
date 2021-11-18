// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.ResultSet;
import java.sql.Statement;

public class LoadData extends Common {

    @Benchmark
    public void run(MyState state) throws Throwable {
        try (Statement stmt = state.connectionText.createStatement()) {
            stmt.execute("TRUNCATE `infile`");
            stmt.executeUpdate(
                    "LOAD DATA LOCAL INFILE '"
                            + state.loadDataFile.getCanonicalPath()
                            + "' "
                            + "INTO TABLE `infile` "
                            + "COLUMNS TERMINATED BY ',' ENCLOSED BY '\\\"' ESCAPED BY '\\\\' "
                            + "LINES TERMINATED BY '\\n' (`a`, `b`)");
        }
    }
}
