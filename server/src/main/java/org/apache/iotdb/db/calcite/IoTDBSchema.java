package org.apache.iotdb.db.calcite;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.sql.SQLException;
import java.util.*;

public class IoTDBSchema extends AbstractSchema {
  private Map<String, Table> tableMap;
  final String name;

  /**
   * Create a IoTDB schema
   * @param name Name of schema
   */
  public IoTDBSchema(String name){
    super();
    this.name = name;
  }

  RelProtoDataType getRelDataType(String storageGroup) throws SQLException, PathException {
    final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    // add itime, deviceid columns in relational table
    fieldInfo.add("itime", typeFactory.createSqlType(toType(TSDataType.INT64)));
    fieldInfo.add("deviceid", typeFactory.createSqlType(toType(TSDataType.TEXT)));

    MManager mManager = MManager.getInstance();
    Set<String> devices = mManager.getAllDevices();

    // get one device in this storage group
    Iterator<String> iter = devices.iterator();
    String oneDevice = null;
    while(iter.hasNext()){
      if(iter.next().startsWith(storageGroup)){
        oneDevice = iter.next();
        break;
      }
    }
    // if no device in this storage group
    if(oneDevice == null){
      return null;
    }
    List<List<String>> timeseries = mManager.getShowTimeseriesPath(oneDevice);
    for (List<String> t : timeseries) {
      String sensorName = t.get(0);
      TSDataType sensorType = mManager.getSeriesType(sensorName);
      int index = sensorName.lastIndexOf('.');
      fieldInfo.add(sensorName.substring(index + 1), typeFactory.createSqlType(toType(sensorType)));
    }

    return RelDataTypeImpl.proto(fieldInfo.build());
  }


  public static SqlTypeName toType(TSDataType tsDataType){
    SqlTypeName typeName = SqlTypeName.ANY;
    if(tsDataType == TSDataType.INT32){
      typeName = SqlTypeName.INTEGER;
    }
    else if(tsDataType == TSDataType.INT64){
      typeName = SqlTypeName.BIGINT;
    }
    else if(tsDataType == TSDataType.FLOAT){
      typeName = SqlTypeName.FLOAT;
    }
    else if(tsDataType == TSDataType.DOUBLE){
      typeName = SqlTypeName.DOUBLE;
    }
    else if(tsDataType == TSDataType.BOOLEAN){
      typeName = SqlTypeName.BOOLEAN;
    }
    else if(tsDataType == TSDataType.TEXT){
      typeName = SqlTypeName.VARCHAR;
    }

    return typeName;
  }
  @Override
  protected Map<String, Table> getTableMap() {
    if(tableMap == null){
      tableMap = createTableMap();
    }
    return tableMap;
  }

  public Map<String, Table> createTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    MManager mManager = MManager.getInstance();
    List<String> storageGroups = mManager.getAllStorageGroupNames();
    for (String storageGroup : storageGroups) {
      storageGroup = storageGroup.toLowerCase();
      // storageGroup = storageGroup.replace("ROOT.", "").replace(".", "_");
      builder.put(storageGroup, new IoTDBTable(this, storageGroup));
    }

    return builder.build();
  }
}

// End IoTDBSchema.java