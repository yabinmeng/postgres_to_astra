package com.example.utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreSqlConnUtil {
    // PostgreSQL server connection settings
    final static String postgreSQLSrv =  "10.101.33.137:5432";
    final static String postgreSQLUsrName = "postgres";

    // Connect to PostgreSQL server
    public static Connection getConnection(String database) {
        Connection conn = null;
        try {
            String connUrl = "jdbc:postgresql://" + postgreSQLSrv + "/" + database;
            conn = DriverManager.getConnection(connUrl, postgreSQLUsrName, "");
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        if (conn != null) {
            System.out.println("   --> Connection established.");
        }
        else {
            System.out.println("   --> Connection failed.");
        }

        return conn;
    }

    // Close connection to PostgreSQL
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }
    }
}
