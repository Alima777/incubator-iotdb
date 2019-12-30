package org.apache.iotdb.calcite;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

enum IoTDBFieldType {
  STRING(String.class, "TEXT"),
  BOOLEAN(Boolean.class, "BOOLEAN"),
  INT32(Integer.class, "INT32"),
  INT64(Long.class, "INT64"),
  FLOAT(Float.class, "FLOAT"),
  DOUBLE(Double.class, "DOUBLE"),
  TIMESTAMP(Long.class, "timestamp");

  private final Class clazz;
  private final String simpleName;

  private static final Map<String, IoTDBFieldType> MAP = new HashMap<>();

  static {
    for (IoTDBFieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  IoTDBFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static IoTDBFieldType of(String typeString) {
    return MAP.get(typeString);
  }

}
