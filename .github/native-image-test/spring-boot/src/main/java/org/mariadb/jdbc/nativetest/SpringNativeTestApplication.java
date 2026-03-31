package org.mariadb.jdbc.nativetest;

import java.sql.ResultSet;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class SpringNativeTestApplication {

  public static void main(String[] args) {
    SpringApplication.run(SpringNativeTestApplication.class, args);
  }

  @Bean
  CommandLineRunner runner(JdbcTemplate jdbc) {
    return args -> {
      jdbc.execute("DROP TABLE IF EXISTS native_spring_test");
      jdbc.execute("CREATE TABLE native_spring_test (id INT PRIMARY KEY, val VARCHAR(100))");

      jdbc.update("INSERT INTO native_spring_test (id, val) VALUES (?, ?)",
          1, "hello from spring native");

      String val = jdbc.query(
          "SELECT val FROM native_spring_test WHERE id = ?",
          (ResultSet rs) -> rs.next() ? rs.getString(1) : null,
          1);

      if (!"hello from spring native".equals(val)) {
        throw new AssertionError("Unexpected result: " + val);
      }
      System.out.println("Read back: val=" + val);

      jdbc.execute("DROP TABLE native_spring_test");
      System.out.println("Spring Boot native image test PASSED");

      // Exit explicitly since CommandLineRunner keeps the context alive
      System.exit(0);
    };
  }
}
