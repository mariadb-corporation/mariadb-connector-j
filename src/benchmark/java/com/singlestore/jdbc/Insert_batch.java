// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Insert_batch extends Common {

    static final List<String> chars = new ArrayList<>();
    static {
        chars.addAll(Arrays.asList("123456789abcdefghijklmnop\\Z".split("")));
        chars.add("😎");
        chars.add("🌶");
        chars.add("🎤");
        chars.add("🥂");
    }

    static public String randomString(int length) {
        StringBuilder result = new StringBuilder();
        for (int i = length; i > 0; --i) result.append(chars.get((int) Math.round(Math.random() * (chars.size() - 1))));
        return result.toString();
    }

    @Benchmark
    public int[] binary(MyState state) throws Throwable {
        return run(state.connectionBinary);
    }

    @Benchmark
    public int[] rewrite(MyState state) throws Throwable {
        return run(state.connectionTextRewrite);
    }

    @Benchmark
    public void binaryNoPipeline(MyState state) throws Throwable {
        run(state.connectionBinaryNoPipeline);
    }

    private int[] run(Connection con) throws Throwable {
        String s = randomString(100);
        try (PreparedStatement prep = con.prepareStatement("INSERT INTO perfTestTextBatch(t0) VALUES (?)")) {
            for (int i = 0; i < 100; i++) {
                prep.setString(1, s);
                prep.addBatch();
            }
            return prep.executeBatch();
        }
    }
}
