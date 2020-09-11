package com.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.example.utilities.AstraConnUtil;
import com.example.utilities.PostgreSqlConnUtil;
import org.json.simple.JSONObject;

import java.sql.*;

public class LoadDvdData {
    // This is the PostgreSQL database name we're loading data from
    final static String pDBName = "dvdrental";

    // This is the C* keyspace name we're unloading data to
    final static String cKSName = "testks";

    public static void main(String[] args) {
        // Establish connection to PostgreSQL
        System.out.println(">> Connecting to PostgreSQL database(" + pDBName + ")...");
        Connection pConn = PostgreSqlConnUtil.getConnection(pDBName);
        if (pConn == null) {
            System.exit(10);
        }

        // Connect to Astra using Stargate Rest API
        System.out.println("\n>> Connecting to Astra keyspace(" + pDBName + ")...");
        String authToken = AstraConnUtil.getAccessToken();
        System.out.println("   --> AuthToken: " + authToken);


        // In this example, we're demonstrating writing data to DataStax Astra using a new schemaless
        // API called "Stargate Document API"
        // ----
        // Once we have established connections to both databases, we're going to load data from one
        // table called "actor" in PostgreSQL and land it into DataStax Astra.
        // ----
        // In PostgreSQL, "actor" table has the following schema:
        //   actor_id	    integer (PK
        //   first_name	    character varying
        //   last_name	    character varying
        //   last_update	timestamp without time zone
        // ---
        // In DataStax Astra, we're keeping a similar schema with "last_update" column is dropped

        String tableName = "actor";
        String loadActorTblSQLString = "select * from "  +tableName;


        System.out.println("\n>> Loading table \"" + tableName + "\" from PostgresSQL into Astra keyspace(" + pDBName + ")...");
        try {
            Statement sqlStmt = pConn.createStatement();

            ResultSet rs = sqlStmt.executeQuery(loadActorTblSQLString);
            ResultSetMetaData rsMeta = rs.getMetaData();

            while (rs.next()) {
                // Convert each fetched row to a JSON object
                JSONObject jsonObject = new JSONObject();

                for (int i=1; i<rsMeta.getColumnCount(); i++) {
                    String colName = rsMeta.getColumnName(i);

                    // skip "last_update"
                    if (! colName.equalsIgnoreCase("last_update")) {
                        jsonObject.put(colName, rs.getObject(i));
                    }
                }

               AstraConnUtil.WriteDocument(authToken, tableName, jsonObject);

                // For testing purpose, only process 1 record
                break;
            }

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        // Close connections to both PostgreSQL and DataStax Astra
        if (pConn != null) {
            PostgreSqlConnUtil.closeConnection(pConn);
        }

        System.exit(0);
    }
}
