package com.singlestore.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Update_batch
{
    static final List<String> chars = new ArrayList<>();

    static {
        chars.addAll(Arrays.asList("123456789abcdefghijklmnopZ".split("")));
    }

    static public String randomString(int length)
    {
        StringBuilder result = new StringBuilder();
        for (int i = length; i > 0; --i) {
            result.append(chars.get((int) Math.round(Math.random() * (chars.size() - 1))));
        }
        return result.toString();
    }

    @Benchmark
    public int[] text(Common.MyState state)
            throws Throwable
    {
        return run(state.connectionText);
    }

    @Benchmark
    public int[] binary(Common.MyState state)
            throws Throwable
    {
        return run(state.connectionBinary);
    }

    private int[] run(Connection con)
            throws Throwable
    {
        String s = randomString(30);
        int currentThreadId = (int) Thread.currentThread().getId();
        try (PreparedStatement prep = con.prepareStatement("UPDATE perfTestUpdateBatch SET f1 = f1 + 1, f2 = ?, f3 = ?, f4 = ?, f5 = ?, f6 = ?, f7 = ?, f8 = ?, f9 = ? WHERE id = ?")) {
            for (int i = 0; i < 100; i++) {
                prep.setString(1, s);
                prep.setString(2, s);
                prep.setString(3, s);
                prep.setString(4, s);
                prep.setString(5, s);
                prep.setString(6, s);
                prep.setString(7, s);
                prep.setString(8, s);
                prep.setInt(9, currentThreadId * i);
                prep.addBatch();
            }
            return prep.executeBatch();
        }
    }
}
