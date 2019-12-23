package org.apache.iotdb.db.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface IoTDBRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in IoTDB. */
  Convention CONVENTION = new Convention.Impl("IOTDB", IoTDBRel.class);

  /** Callback for the implementation process that converts a tree of
   * {@link IoTDBRel} nodes into a IoTDB Physical Plan. */
  class Implementor {
    final List<Path> paths = new ArrayList<>();

    RelOptTable table;
    IoTDBTable ioTDBTable;

    /** Adds newly paths.
     *
     * @param paths New fields to be projected from a query
     */
    public void add(List<Path> paths) {
      if (paths != null) {
        this.paths.addAll(paths);
      }
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((IoTDBRel) input).implement(this);
    }
  }
}

// End IoTDBRel.java