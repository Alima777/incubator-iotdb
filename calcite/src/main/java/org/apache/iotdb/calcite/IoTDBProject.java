package org.apache.iotdb.calcite;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in IoTDB.
 */
public class IoTDBProject extends Project implements IoTDBRel {
  private final List<Pair<RexNode, String>> projectRows;
  private final String storageGroup;
  private List<Path> paths;
  private List<TSDataType> dataTypes;

  public IoTDBProject(RelOptCluster cluster, RelTraitSet traitSet,
                      RelNode input, List<? extends RexNode> projects, RelDataType rowType,
                      List<Pair<RexNode, String>> projectRows) {
    super(cluster, traitSet, input, projects, rowType);

    this.projectRows = projectRows;
    this.storageGroup = RelOptUtil.findAllTables(this).get(0).getQualifiedName().get(1);

/*    try {
      this.paths = translatePaths(projectRows);
      this.dataTypes = translateTypes(paths);
    } catch (LogicalOptimizeException e) {
      e.printStackTrace();
    }*/

    assert getConvention() == IoTDBRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

/*  public List<Path> getPaths(){
    return this.paths;
  }

  public List<TSDataType> getDataTypes(){
    return this.dataTypes;
  }

  private List<Path> translatePaths(List<Pair<RexNode, String>> projectRows) throws LogicalOptimizeException {
    List<Path> paths = new ArrayList<>();

    try{
      for (Pair<RexNode, String> projectRow : projectRows) {
        String measurement = projectRow.getValue().toLowerCase();
        if(!measurement.equals("deviceid") && !measurement.equals("itime")){
          String path = storageGroup.concat(".*." + measurement);
          List<String> all;
          all = mManager.getPaths(path);
          if (all.isEmpty()) {
            throw new LogicalOptimizeException(
                    "Path: \"" + path + "\" doesn't correspond to any known time series");
          }
          for (String subPath : all) {
            paths.add(new Path(subPath));
          }
        }
      }
      return paths;
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
  }

  private List<TSDataType> translateTypes(List<Path> paths) throws LogicalOptimizeException {
    // 根据 paths 从 mmanager 直接获取 TSDatatype
    List<TSDataType> tsDataTypes = new ArrayList<>();
    try {
      for (Path path : paths) {
        TSDataType dataType = null;
        dataType = mManager.getSeriesType(path.getFullPath());
        tsDataTypes.add(dataType);
      }
    } catch (PathException e) {
      throw new LogicalOptimizeException("error when translate types: " + e.getMessage());
    }
    return tsDataTypes;
  }*/

  @Override public Project copy(RelTraitSet traitSet, RelNode input,
                                List<RexNode> projects, RelDataType rowType) {
    return new IoTDBProject(getCluster(), traitSet, input, projects,
            rowType, projectRows);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                              RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
  }
}

// End IoTDBProject.java
