package org.apache.iotdb.db.calcite;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.util.Pair;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.thrift.TException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class IoTDBTable extends AbstractTable
    implements TranslatableTable {
  RelProtoDataType protoRowType;
  private final IoTDBSchema schema;
  private final String storageGroup;

  public IoTDBTable(IoTDBSchema schema, String storageGroup){
    super();
    this.schema = schema;
    this.storageGroup = storageGroup;
  }

  public String toString(){ return "IoTDBTable {" + storageGroup + "}"; };

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    try{
      if (protoRowType == null) {
        protoRowType = schema.getRelDataType(storageGroup);
      }
      return protoRowType.apply(typeFactory);
    } catch (Exception e){
      e.printStackTrace();
      return null;
    }
  }
/*
  public Enumerable<Object> query() {
    return query(ImmutableList.of());
  }
  *//** Executes a IoTDB physical plan.
   *
   * @param paths List of fields to project
   * @return Enumerator of results
   *//*
  public Enumerable<Object> query(List<Path> paths){
    QueryPlan physicalPlan = new QueryPlan();
    physicalPlan.setPaths(paths);

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        TSServiceImpl tsService = new TSServiceImpl();
        final TSExecuteStatementResp resp = tsService.executeCalciteQuery(physicalPlan);
        Enumerator<Object> enumerator = new IoTDBEnumerator(resp);
        return enumerator;
      }
    };
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new IoTDBQueryable<>(queryProvider, schema, this, storageGroup);
  }*/

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new IoTDBTableScan(cluster, cluster.traitSetOf(IoTDBRel.CONVENTION),
            relOptTable, this, null);
  }
/*
  *//** Implementation of {@link org.apache.calcite.linq4j.Queryable}
   *
   * @param <T> element type
   *//*
  public static class IoTDBQueryable<T> extends AbstractTableQueryable<T> {
    public IoTDBQueryable(QueryProvider queryProvider, SchemaPlus schema,
                              IoTDBTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable = (Enumerable<T>) getTable().query();
      return enumerable.enumerator();
    }

    private IoTDBTable getTable() {
      return (IoTDBTable) table;
    }

    *//** Called via code-generation.
     *
     *  org.apache.calcite.adapter.cassandra.CassandraMethod#CASSANDRA_QUERYABLE_QUERY
     *//*
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(List<Path> paths) {
      return getTable().query(paths);
    }
  }*/
}
