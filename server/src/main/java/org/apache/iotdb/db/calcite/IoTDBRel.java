package org.apache.iotdb.db.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;

import java.util.*;

public interface IoTDBRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in IoTDB. */
  Convention CONVENTION = new Convention.Impl("IOTDB", IoTDBRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link IoTDBRel} nodes into a IoTDB Physical Plan. */
  class Implementor {
    final List<Path> paths = new ArrayList<>();
    final List<TSDataType> dataTypes = new ArrayList<>();
    IExpression iExpression = null;

    RelOptTable table;
    IoTDBTable ioTDBTable;

    public void addPaths(List<Path> paths) {
      Objects.requireNonNull(paths, "paths");
      paths.addAll(paths);
    }

    public void addDataTypes(List<TSDataType> dataTypes) {
      Objects.requireNonNull(dataTypes, "datatypes");
      dataTypes.addAll(dataTypes);
    }

    public void addIExpression(IExpression iExpression) {
      Objects.requireNonNull(dataTypes, "datatypes");
      iExpression = iExpression;
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((IoTDBRel) input).implement(this);
    }
  }
}

// End IoTDBRel.java