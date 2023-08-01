// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.util.log;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.log.ConsoleLogger;
import org.mariadb.jdbc.util.log.JdkLogger;
import org.mariadb.jdbc.util.log.Loggers;
import org.mariadb.jdbc.util.log.Slf4JLogger;

public class LoggerFactoryTest {

  @AfterAll
  public static void drop() {
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
