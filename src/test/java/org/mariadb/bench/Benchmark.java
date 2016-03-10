package org.mariadb.bench;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.JUnitCore;
import org.mariadb.jdbc.BigQueryTest;
import org.mariadb.jdbc.BlobTest;
import org.mariadb.jdbc.BooleanTest;
import org.mariadb.jdbc.MultiTest;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

public class Benchmark {
    
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();
  

    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 10)
    @Test
    public void test01() {
        JUnitCore.runClasses(MultiTest.class, BigQueryTest.class, BlobTest.class, BooleanTest.class);
    }
}
