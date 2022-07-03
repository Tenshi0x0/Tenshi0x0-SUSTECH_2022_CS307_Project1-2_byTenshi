package com.pgsqlproject2.pgproj2;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@SpringBootTest
class Pgproj2ApplicationTests {

	@Autowired
	DataSource ds;

	@Test
	void contextLoads() throws SQLException {
		for (int i = 1; i <= 20; i++) {
			Connection connection = ds.getConnection();
			System.out.println(i + " connected!");
//			connection.close();
		}
	}

}

