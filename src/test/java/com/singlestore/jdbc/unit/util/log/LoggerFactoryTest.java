// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.util.log;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.util.log.ConsoleLogger;
import com.singlestore.jdbc.util.log.JdkLogger;
import com.singlestore.jdbc.util.log.Loggers;
import com.singlestore.jdbc.util.log.Slf4JLogger;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class LoggerFactoryTest {

  @AfterAll
  public static void drop() throws SQLException {
    System.clearProperty(Loggers.TEST_ENABLE_SLF4J);
    System.clearProperty(Loggers.FALLBACK_PROPERTY);
    System.clearProperty(Loggers.CONSOLE_DEBUG_PROPERTY);
    Loggers.init();
  }

  @Test
  public void defaultSlf4j() {
    System.clearProperty(Loggers.TEST_ENABLE_SLF4J);
    System.clearProperty(Loggers.FALLBACK_PROPERTY);
    Loggers.init();
    assertTrue(Loggers.getLogger("test") instanceof Slf4JLogger);
  }

  @Test
  public void commonLogger() {
    System.setProperty(Loggers.TEST_ENABLE_SLF4J, "false");
    System.setProperty(Loggers.FALLBACK_PROPERTY, "JDK");
    Loggers.init();
    assertTrue(Loggers.getLogger("test") instanceof JdkLogger);
  }

  @Test
  public void consoleLogger() {
    System.setProperty(Loggers.TEST_ENABLE_SLF4J, "false");
    System.clearProperty(Loggers.FALLBACK_PROPERTY);
    Loggers.init();
    assertTrue(Loggers.getLogger("test") instanceof ConsoleLogger);
    System.setProperty(Loggers.CONSOLE_DEBUG_PROPERTY, "true");
    Loggers.init();
    assertTrue(Loggers.getLogger("test") instanceof ConsoleLogger);
  }
}
