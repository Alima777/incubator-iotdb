package org.apache.iotdb.db.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class IoTDBAdapterTest {
  @Test
  public void testIoTDBSchema() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    Schema schema = new IoTDBSchema("IoTDBTest");
    rootSchema.add("test", schema);
    Set<String> tableNames = schema.getTableNames();

    for (String tableName : tableNames) {
      System.out.println(tableName);
    }
  }
}
