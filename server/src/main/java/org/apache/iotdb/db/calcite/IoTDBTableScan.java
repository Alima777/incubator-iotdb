package org.apache.iotdb.db.calcite;

import jdk.nashorn.internal.ir.PropertyKey;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iotdb.db.service.IoTDB;

import java.util.List;

/**
 * Relational expression representing a scan of a IoTDB storage group.
 */
public class IoTDBTableScan extends TableScan implements IoTDBRel {
  final IoTDBTable ioTDBTable;
  final RelDataType projectRowType;

  /**
   * Creates a IoTDBTableScan.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param ioTDBTable IoTDB table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  protected IoTDBTableScan(RelOptCluster cluster, RelTraitSet traitSet,
       RelOptTable table, IoTDBTable ioTDBTable, RelDataType projectRowType) {
    super(cluster, traitSet, table);
    this.ioTDBTable = ioTDBTable;
    this.projectRowType = projectRowType;

    assert ioTDBTable != null;
    assert getConvention() == IoTDBRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(IoTDBToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : IoTDBRules.RULES) {
      planner.addRule(rule);
    }
  }
  @Override
  public void implement(Implementor implementor) {
    implementor.ioTDBTable = ioTDBTable;
    implementor.table = table;
  }
}

// End IoTDBTableScan.java