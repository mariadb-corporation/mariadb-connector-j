package org.mariadb.jdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionFactory {
  private static Version instance = null;

  // use getShape method to get object of type shape
  public static Version getInstance() {
    if (instance == null) {
      synchronized (VersionFactory.class) {
        if (instance == null) {
          String tmpVersion = "5.5.0";
          try (InputStream inputStream =
              Version.class.getClassLoader().getResourceAsStream("mariadb.properties")) {
            if (inputStream == null) {
              System.out.println("property file 'mariadb.properties' not found in the classpath");
            }
            Properties prop = new Properties();
            prop.load(inputStream);
            tmpVersion = prop.getProperty("version");
          } catch (IOException e) {
            e.printStackTrace();
          }
          instance = parse(tmpVersion);
        }
      }
    }
    return instance;
  }

  public static Version parse(String version) {
    int major = 0;
    int minor = 0;
    int patch = 0;
    String qualif = "";

    int length = version.length();
    char car;
    int offset = 0;
    int type = 0;
    int val = 0;
    for (; offset < length; offset++) {
      car = version.charAt(offset);
      if (car < '0' || car > '9') {
        switch (type) {
          case 0:
            major = val;
            break;
          case 1:
            minor = val;
            break;
          case 2:
            patch = val;
            qualif = version.substring(offset);
            offset = length;
            break;
          default:
            break;
        }
        type++;
        val = 0;
      } else {
        val = val * 10 + car - 48;
      }
    }

    if (type == 2) {
      patch = val;
    }

    return new Version(version, major, minor, patch, qualif);
  }
}
