import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Minimal JDBC test for GraalVM native image validation.
 * Verifies that the MariaDB connector works correctly when compiled to a native binary.
 */
public class NativeImageTest {

  private static String env(String key, String defaultValue) {
    String val = System.getenv(key);
    return (val != null && !val.isEmpty()) ? val : defaultValue;
  }

  public static void main(String[] args) throws Exception {
    String host = env("TEST_DB_HOST", "127.0.0.1");
    String port = env("TEST_DB_PORT", "3306");
    String db = env("TEST_DB_DATABASE", "testj");
    String user = env("TEST_DB_USER", "root");
    String password = env("TEST_DB_PASSWORD", "");

    String url = "jdbc:mariadb://" + host + ":" + port + "/" + db
        + "?user=" + user + "&password=" + password;
    System.out.println("Connecting to jdbc:mariadb://" + host + ":" + port + "/" + db
        + " as user=" + user);

    try (Connection conn = DriverManager.getConnection(url)) {
      System.out.println("Connected: " + conn.getMetaData().getDatabaseProductName()
          + " " + conn.getMetaData().getDatabaseProductVersion());

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS native_image_test");
        stmt.execute("CREATE TABLE native_image_test (id INT PRIMARY KEY, val VARCHAR(100))");
      }

      try (PreparedStatement ps = conn.prepareStatement(
          "INSERT INTO native_image_test (id, val) VALUES (?, ?)")) {
        ps.setInt(1, 1);
        ps.setString(2, "hello from native image");
        ps.executeUpdate();
      }

      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT id, val FROM native_image_test WHERE id = ?")) {
        ps.setInt(1, 1);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) throw new AssertionError("No row found");
          int id = rs.getInt(1);
          String val = rs.getString(2);
          if (id != 1 || !"hello from native image".equals(val)) {
            throw new AssertionError("Unexpected result: id=" + id + ", val=" + val);
          }
          System.out.println("Read back: id=" + id + ", val=" + val);
        }
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE native_image_test");
      }

      System.out.println("Native image test PASSED");
    }
  }
}
