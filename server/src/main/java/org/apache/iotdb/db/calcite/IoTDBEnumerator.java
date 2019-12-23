package org.apache.iotdb.db.calcite;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import java.security.PrivateKey;
import java.sql.ResultSet;

public class IoTDBEnumerator implements Enumerator<Object> {
  /** Creates a IoTDBEnumerator.
   *
   * @param resp IoTDB result set
   */
  IoTDBEnumerator(TSExecuteStatementResp resp) {

  }
  @Override
  public Object current() {
    return null;
  }

  @Override
  public boolean moveNext() {
    return false;
  }

  @Override
  public void reset() {

  }

  @Override
  public void close() {

  }
}
