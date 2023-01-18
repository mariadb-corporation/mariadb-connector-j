package org.mariadb.jdbc.unit.util;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.Version;
import org.mariadb.jdbc.util.VersionFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class VersionFactoryTest {

    @Test
    public void testGetInstance() {
        Version actual = VersionFactory.getInstance();

        assertNotNull(actual);
    }

}