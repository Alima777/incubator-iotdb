package org.apache.iotdb.db.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

/**
 * Relational expression representing a scan of a table in a IoTDB data source.
 */
public class IoTDBToEnumerableConverter extends ConverterImpl
        implements EnumerableRel {
  public IoTDBToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public Result implement(EnumerableRelImplementor enumerableRelImplementor, Prefer prefer) {
    return null;
  }
}
