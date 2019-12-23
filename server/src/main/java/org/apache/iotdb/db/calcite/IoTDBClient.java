package org.apache.iotdb.db.calcite;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import java.sql.*;
import java.util.Properties;

public class IoTDBClient {
  public static void main(String[] args) {
    try{
      Class.forName("org.apache.calcite.jdbc.Driver");
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection =
              connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("IoTDBSchema", new IoTDBSchema("IoTDBSchema"));
      calciteConnection.setSchema("IoTDBSchema");
      Statement stmt = calciteConnection.createStatement();
      String sql = "SELECT * FROM \"ROOT.LN\"";
      ResultSet rs = stmt.executeQuery(sql);

      while (rs.next()) {
        for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++){
          System.out.print(rs.getMetaData().getColumnName(i) + ":" + rs.getObject(i));
          System.out.print(" | ");
        }
        System.out.println();
      }

      rs.close();
      stmt.close();
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return;
  }
}
