// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.plugin.credential;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.plugin.CredentialPlugin;

/**
 * The AWS-IAM plugin needs software.amazon.awssdk:rds. Its dependency check must look for a class
 * of that artifact, not of software.amazon.awssdk:auth, which an application may have on its
 * classpath for other reasons (CONJ-1360).
 */
public class AwsIamCredentialPluginTest {

  private static final String PLUGIN =
      "org.mariadb.jdbc.plugin.credential.aws.AwsIamCredentialPlugin";

  /** Hides the rds artifact, and defines the plugin's package itself so its lookups go through. */
  private static final class WithoutRdsLoader extends ClassLoader {
    WithoutRdsLoader() {
      super(AwsIamCredentialPluginTest.class.getClassLoader());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.startsWith("software.amazon.awssdk.services.rds.")) {
        throw new ClassNotFoundException(name);
      }
      if (name.startsWith("org.mariadb.jdbc.plugin.credential.aws.")) {
        synchronized (getClassLoadingLock(name)) {
          Class<?> c = findLoadedClass(name);
          if (c == null) {
            try (InputStream in =
                getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
              if (in == null) throw new ClassNotFoundException(name);
              ByteArrayOutputStream bytes = new ByteArrayOutputStream();
              byte[] buf = new byte[8192];
              int read;
              while ((read = in.read(buf)) != -1) bytes.write(buf, 0, read);
              byte[] b = bytes.toByteArray();
              c = defineClass(name, b, 0, b.length);
            } catch (IOException e) {
              throw new ClassNotFoundException(name, e);
            }
          }
          return c;
        }
      }
      return super.loadClass(name, resolve);
    }
  }

  @Test
  public void rdsArtifactMissingReportedAsSqlException() throws Exception {
    // software.amazon.awssdk:auth stays visible, only the rds classes are gone
    CredentialPlugin plugin =
        (CredentialPlugin)
            new WithoutRdsLoader().loadClass(PLUGIN).getDeclaredConstructor().newInstance();
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/db?user=u");

    SQLException e =
        assertThrows(
            SQLException.class, () -> plugin.initialize(conf, "u", conf.addresses().get(0)));
    assertTrue(e.getMessage().contains("software.amazon.awssdk:rds"), e.getMessage());
  }
}
