package org.apache.iotdb.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class IoTDBTable extends AbstractQueryableTable
    implements TranslatableTable {
  RelProtoDataType protoRowType;
  private final IoTDBSchema schema;
  private final String storageGroup;

  public IoTDBTable(IoTDBSchema schema, String storageGroup){
    super(Object[].class);
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
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return protoRowType.apply(typeFactory);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    return new IoTDBQueryable<>(queryProvider, schema, this, storageGroup);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new IoTDBTableScan(cluster, cluster.traitSetOf(IoTDBRel.CONVENTION),
            relOptTable, this, null);
  }

  public Enumerable<Object> query(final Connection connection) {
    return query(connection, ImmutableList.of(), ImmutableList.of(),
            ImmutableList.of(),  0, -1);
  }

  /** Executes a IoTDB SQL query.
   *
   * @param connection IoTDB connection
   * @param fields List of fields to project
   * @param predicates A list of predicates which should be used in the query
   * @return Enumerator of results
   */
  public Enumerable<Object> query(final Connection connection, List<Map.Entry<String, Class>> fields,
        final List<Map.Entry<String, String>> selectFields, List<String> predicates,
        final Integer offset, final Integer fetch){
    // Build the type of the resulting row based on the provided fields
    final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    final RelDataType rowType = getRowType(typeFactory);

    Function1<String, Void> addField = fieldName -> {
      SqlTypeName typeName =
              rowType.getField(fieldName, true, false).getType().getSqlTypeName();
      fieldInfo.add(fieldName, typeFactory.createSqlType(typeName))
              .nullable(true);
      return null;
    };

    addField.apply(IoTDBConstant.TimeColumn);
    addField.apply(IoTDBConstant.DeviceColumn);
    if (selectFields.isEmpty()) {
      for (Map.Entry<String, Class> field : fields) {
        if(field.getKey() != IoTDBConstant.TimeColumn && field.getKey() != IoTDBConstant.DeviceColumn){
          addField.apply(field.getKey());
        }
      }
    } else {
      for (Map.Entry<String, String> field : selectFields) {
        if(field.getKey() != IoTDBConstant.TimeColumn && field.getKey() != IoTDBConstant.DeviceColumn){
          addField.apply(field.getKey());
        }
      }
    }

    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    // Construct the list of fields to project
    final String selectString;
    if (selectFields.isEmpty()) {
      selectString = "*";
    } else {
      selectString = "*";
    }

    // Combine all predicates conjunctively
    String whereClause = "";
    if (!predicates.isEmpty()) {
      whereClause = " WHERE ";
      whereClause += Util.toString(predicates, "", " AND ", "");
    }

    // Build and issue the query and return an Enumerator over the results
    StringBuilder queryBuilder = new StringBuilder("SELECT ");
    queryBuilder.append(selectString);
    queryBuilder.append(" FROM " + storageGroup);
    queryBuilder.append(whereClause);
    queryBuilder.append(IoTDBConstant.GroupByDevice);
    final String query = queryBuilder.toString();

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        Enumerator<Object> enumerator = null;
        try {
          Statement statement = connection.createStatement();
          final ResultSet results = statement.executeQuery(query);
          enumerator = new IoTDBEnumerator(results, resultRowType);
          return enumerator;
        } catch (SQLException e) {
          e.printStackTrace();
        }
        return enumerator;
      }
    };
  }

  /** Implementation of {@link org.apache.calcite.linq4j.Queryable}
   *
   * @param <T> element type
   */
  public static class IoTDBQueryable<T> extends AbstractTableQueryable<T> {
    public IoTDBQueryable(QueryProvider queryProvider, SchemaPlus schema,
                          IoTDBTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
              (Enumerable<T>) getTable().query(getConnection());
      return enumerable.enumerator();
    }

    private IoTDBTable getTable() {
      return (IoTDBTable) table;
    }

    private Connection getConnection() {
      return schema.unwrap(IoTDBSchema.class).connection;
    }
    /** Called via code-generation.
     *
     */

    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(List<Map.Entry<String, Class>> fields,
        List<Map.Entry<String, String>> selectFields, List<String> predicates,
                                    Integer offset, Integer fetch) {
      return getTable().query(getConnection(), fields, selectFields, predicates, offset, fetch);
    }
  }
}
