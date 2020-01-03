package org.apache.iotdb.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;
import org.apache.iotdb.calcite.utils.EnvironmentUtils;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class IoTDBAdapterTest {

  public static final ImmutableMap<String, String> MODEL =
          ImmutableMap.of("model",
                  Sources.of(IoTDBAdapterTest.class.getResource("/model.json"))
                          .file().getAbsolutePath());
  public static final String MODEL_STRING =
          Sources.of(IoTDBAdapterTest.class.getResource("/model.json")).file().getAbsolutePath();
  private static IoTDB daemon;
  private static String[] sqls = new String[]{

          "SET STORAGE GROUP TO root.vehicle",
          "SET STORAGE GROUP TO root.other",

          "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
          "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
          "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

          "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",

          "CREATE TIMESERIES root.other.d1.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",

          "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
          "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
          "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
          "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
          "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
          "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
          "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
          "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",

          "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
          "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
          "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
          "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
          "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
          "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
          "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
          "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",

          "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
          "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
          "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
          "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
          "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
          "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
          "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",

          "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
          "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
          "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
          "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
          "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",

          "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
          "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",

          "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
          "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",

          "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
          "insert into root.vehicle.d0(timestamp,s4) values(100, true)",

          "insert into root.other.d1(timestamp,s0) values(2, 3.14)",};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void selectTest() throws ClassNotFoundException, SQLException {
    String[] retArray = new String[]{
            "1,root.vehicle.d0,101,1101,null,null,null,",
            "2,root.vehicle.d0,10000,40000,2.22,null,null,",
            "3,root.vehicle.d0,null,null,3.33,null,null,",
            "4,root.vehicle.d0,null,null,4.44,null,null,",
            "50,root.vehicle.d0,10000,50000,null,null,null,",
            "60,root.vehicle.d0,null,null,null,aaaaa,null,",
            "70,root.vehicle.d0,null,null,null,bbbbb,null,",
            "80,root.vehicle.d0,null,null,null,ccccc,null,",
            "100,root.vehicle.d0,99,199,null,null,true,",
            "101,root.vehicle.d0,99,199,null,ddddd,null,",
            "102,root.vehicle.d0,80,180,10.0,fffff,null,",
            "103,root.vehicle.d0,99,199,null,null,null,",
            "104,root.vehicle.d0,90,190,null,null,null,",
            "105,root.vehicle.d0,99,199,11.11,null,null,",
            "106,root.vehicle.d0,99,null,null,null,null,",
            "1000,root.vehicle.d0,22222,55555,1000.11,null,null,",
            "946684800000,root.vehicle.d0,null,100,null,good,null,",
            "1,root.vehicle.d1,999,null,null,null,null,",
            "1000,root.vehicle.d1,888,null,null,null,null,",
    };

    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection connection = DriverManager
            .getConnection("jdbc:calcite:model=" + MODEL_STRING);
         Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(
              "select * from \"root.vehicle\"");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          header.append(resultSetMetaData.getColumnName(i)).append(",");
        }
        Assert.assertEquals("time,device,s0,s1,s2,s3,s4,", header.toString());
        Assert.assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
        Assert.assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
        Assert.assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(3));
        Assert.assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(4));
        Assert.assertEquals(Types.REAL, resultSetMetaData.getColumnType(5));
        Assert.assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(6));
        Assert.assertEquals(Types.BOOLEAN, resultSetMetaData.getColumnType(7));

        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getObject(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        Assert.assertEquals(19, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

}
