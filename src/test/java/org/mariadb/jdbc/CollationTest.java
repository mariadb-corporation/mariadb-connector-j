package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

public class CollationTest extends BaseTest {
	
	@Test
	public void emoji() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&useUnicode=yes&useConfigs=maxPerformance");
		conn.createStatement().execute("SET @@character_set_server = 'utf8mb4'");
		conn.createStatement().execute("SET NAMES utf8mb4");
		conn.createStatement().execute("DROP TABLE IF EXISTS emojiTest");
		conn.createStatement().execute("CREATE TABLE emojiTest (id int unsigned, field longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci)");
		PreparedStatement ps = conn.prepareStatement("INSERT INTO emojiTest (id, field) VALUES (1, ?)");
		byte[] emoji = new byte[] {(byte)0xF0, (byte)0x9F, (byte)0x98, (byte)0x84};
		ps.setBytes(1, emoji);
		ps.execute();
		ps = conn.prepareStatement("SELECT field FROM emojiTest");
		ResultSet rs = ps.executeQuery();
		assertTrue(rs.next());
		// compare to the Java representation of UTF32
		assertEquals("\uD83D\uDE04", rs.getString(1));
	}
}
