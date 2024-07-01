package com.ibm.wd.connector.jdbc;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WDDriverTest {
    @Disabled
    @Test
    public void testExecDriver() throws ClassNotFoundException, SQLException {
        final List<String> actualResult = new ArrayList<>();
        final List<String> expectedResult = Arrays.asList("a", "e", "h");

        String url = "jdbc:wd://https://cp4d.wd/api";

        Class.forName("com.ibm.wd.connector.jdbc.WDDriver");
        try (Connection con = DriverManager.getConnection(url); Statement st = con.createStatement()) {
            st.execute("INSERT DUMMY SQL");
            try (ResultSet rs = st.executeQuery("SELECT * FROM projectA.collectionB")) {
                while (rs.next()) {
                    String resultOfFirstIndex = rs.getString(1);
                    System.out.println("rs[1]=" + resultOfFirstIndex);
                    actualResult.add(resultOfFirstIndex);
                }
            }
        }

        assertEquals(expectedResult, actualResult);
    }
}
