package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.Version;
import org.mariadb.jdbc.util.VersionFactory;

class VersionFactoryTest {

  @Test
  public void testGetInstance() {
    Version actual = VersionFactory.getInstance();

    assertNotNull(actual);
  }
}
