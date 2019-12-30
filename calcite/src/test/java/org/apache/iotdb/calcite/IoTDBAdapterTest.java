package org.apache.iotdb.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.Sources;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class IoTDBAdapterTest {

  CalciteConnection calciteConnection;
  @Before
  public void setUp() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    String jsonFile = Sources.of(IoTDBAdapterTest.class.getResource("/model.json")).file().getAbsolutePath();
    Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + jsonFile);
    calciteConnection = connection.unwrap(CalciteConnection.class);
  }

  @Test
  public void testIoTDBAdapter() {
    try{
      Statement stmt = calciteConnection.createStatement();
      String sql = "SELECT * FROM \"root.ln\"";
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
      calciteConnection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
