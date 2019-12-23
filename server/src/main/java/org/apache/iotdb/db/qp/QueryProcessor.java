/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.qp;

import java.lang.management.PlatformLoggingMXBean;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.time.ZoneId;
import java.util.*;

import ch.qos.logback.core.net.SyslogOutputStream;
import ch.qos.logback.core.rolling.TimeBasedFileNamingAndTriggeringPolicy;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.CalcRelSplitter;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Pair;
import org.apache.iotdb.db.calcite.*;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.exception.query.IllegalASTFormatException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.IQueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer;
import org.apache.iotdb.db.sql.ParseGenerator;
import org.apache.iotdb.db.sql.parse.AstNode;
import org.apache.iotdb.db.sql.parse.ParseException;
import org.apache.iotdb.db.sql.parse.ParseUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import sun.plugin.dom.exception.PluginNotSupportedException;

import javax.management.Query;

/**
 * provide a integration method for other user.
 */
public class QueryProcessor {

  private IQueryProcessExecutor executor;

  public QueryProcessor(IQueryProcessExecutor executor) {
    this.executor = executor;
  }

  public IQueryProcessExecutor getExecutor() {
    return executor;
  }

  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr)
      throws QueryProcessException, MetadataException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return parseSQLToPhysicalPlan(sqlStr, config.getZoneID());
  }

  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId)
      throws MetadataException, QueryProcessException {
    AstNode astNode = parseSQLToAST(sqlStr);
    Operator operator = parseASTToOperator(astNode, zoneId);
    operator = logicalOptimize(operator, executor);
    PhysicalGenerator physicalGenerator = new PhysicalGenerator(executor);
    return physicalGenerator.transformToPhysicalPlan(operator);
  }

  public QueryPlan parseSQLToPhysicalPlanThroughCalcite(String sqlStr)
          throws ValidationException, SqlParseException, RelConversionException {
    SchemaPlus parentSchema = Frameworks.createRootSchema(true);

    final List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder()
              .setCaseSensitive(false)
              .setQuotedCasing(Casing.UNCHANGED)
              .setUnquotedCasing(Casing.UNCHANGED)
              .build())
            .defaultSchema(parentSchema.add("IoTDBSchema", new IoTDBSchema("IoTDBSchema")))
            .programs(Programs.ofRules(Programs.RULE_SET))
            .traitDefs(traitDefs)
            .build();

    Planner planner = Frameworks.getPlanner(config);
    SqlNode parser = planner.parse(sqlStr);
    SqlNode validate = planner.validate(parser);
    RelRoot relRoot = planner.rel(validate);
    RelNode relNode = relRoot.project();

    // relNode = planner.transform(0, relNode.getTraitSet(), relNode);

    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleInstance(IoTDBRules.RULES[0]);
    hepProgramBuilder.addRuleInstance(IoTDBRules.RULES[1]);
    hepProgramBuilder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
    hepProgramBuilder.addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE);
    HepPlanner hepPlanner = new HepPlanner(hepProgramBuilder.build());
    hepPlanner.setRoot(relNode);
    relNode = hepPlanner.findBestExp();

    System.out.println("---------------------------After HepPlanner: ");
    System.out.println(RelOptUtil.toString(relNode));

/*    VolcanoPlanner volcanoPlanner = (VolcanoPlanner) relNode.getCluster().getPlanner();
    RelTraitSet desiredTraits = relNode.getCluster().traitSet().replace(IoTDBRel.CONVENTION);
    relNode = volcanoPlanner.changeTraits(relNode, desiredTraits);
    volcanoPlanner.setRoot(relNode);
    relNode = volcanoPlanner.findBestExp();

    System.out.println("---------------------------The best relational expression is: ");
    System.out.println(RelOptUtil.toString(relNode));*/
    planner.close();
    QueryPlan plan = new QueryPlan();
    plan = convertCalcitePlan(relNode, plan);
    return plan;
  }

  private QueryPlan convertCalcitePlan(RelNode relNode, QueryPlan plan) {
    if(relNode instanceof IoTDBProject){
      List<Path> paths = ((IoTDBProject) relNode).getPaths();
      List<TSDataType> dataTypes = ((IoTDBProject) relNode).getDataTypes();
      if(plan.getPaths() == null){
        plan.setPaths(paths);
      } else {
        plan.getPaths().addAll(paths);
      }
      if(plan.getDataTypes() == null){
        plan.setDataTypes(dataTypes);
      } else {
        plan.getDataTypes().addAll(dataTypes);
      }
    } else if(relNode instanceof IoTDBFilter){
      IExpression iExpression = ((IoTDBFilter) relNode).getIExpression();
      plan.setExpression(iExpression);
    }

    Iterator<RelNode> iter = relNode.getInputs().iterator();
    while(iter.hasNext()){
      RelNode input = iter.next();
      plan = convertCalcitePlan(input, plan);
    }

    return plan;
  }

  /**
   * Convert ast tree to Operator which type maybe {@code SFWOperator} or {@code AuthorOperator}
   *
   * @param astNode - input ast tree
   * @return - RootOperator has four subclass:Query/Insert/Delete/Update/Author
   * @throws IllegalASTFormatException exception in converting sql to operator
   */
  private RootOperator parseASTToOperator(AstNode astNode, ZoneId zoneId)
      throws QueryProcessException, MetadataException {
    LogicalGenerator generator = new LogicalGenerator(zoneId);
    return generator.getLogicalPlan(astNode);
  }

  /**
   * Given a SQL statement and generate an ast tree
   *
   * @param sqlStr input sql command
   * @return ast tree
   * @throws IllegalASTFormatException exception in sql parsing
   */
  private AstNode parseSQLToAST(String sqlStr) throws IllegalASTFormatException {
    AstNode astTree;
    // parse string to ASTTree
    try {
      astTree = ParseGenerator.generateAST(sqlStr);
    } catch (ParseException e) {
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    return ParseUtils.findRootNonNullToken(astTree);
  }

  /**
   * given an unoptimized logical operator tree and return a optimized result.
   *
   * @param operator unoptimized logical operator
   * @return optimized logical operator
   * @throws LogicalOptimizeException exception in logical optimizing
   */
  private Operator logicalOptimize(Operator operator, IQueryProcessExecutor executor)
      throws LogicalOperatorException {
    switch (operator.getType()) {
      case AUTHOR:
      case METADATA:
      case SET_STORAGE_GROUP:
      case DELETE_STORAGE_GROUP:
      case CREATE_TIMESERIES:
      case DELETE_TIMESERIES:
      case PROPERTY:
      case LOADDATA:
      case INSERT:
      case INDEX:
      case INDEXQUERY:
      case GRANT_WATERMARK_EMBEDDING:
      case REVOKE_WATERMARK_EMBEDDING:
      case TTL:
      case LOAD_CONFIGURATION:
      case SHOW:
        return operator;
      case QUERY:
      case UPDATE:
      case DELETE:
        SFWOperator root = (SFWOperator) operator;
        return optimizeSFWOperator(root, executor);
      default:
        throw new LogicalOperatorException(operator.getType().toString(), "");
    }
  }

  /**
   * given an unoptimized select-from-where operator and return an optimized result.
   *
   * @param root unoptimized select-from-where operator
   * @return optimized select-from-where operator
   * @throws LogicalOptimizeException exception in SFW optimizing
   */
  private SFWOperator optimizeSFWOperator(SFWOperator root, IQueryProcessExecutor executor)
      throws LogicalOperatorException {
    ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer(executor);
    root = (SFWOperator) concatPathOptimizer.transform(root);
    FilterOperator filter = root.getFilterOperator();
    if (filter == null) {
      return root;
    }
    RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
    filter = removeNot.optimize(filter);
    DnfFilterOptimizer dnf = new DnfFilterOptimizer();
    filter = dnf.optimize(filter);
    MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
    filter = merge.optimize(filter);
    root.setFilterOperator(filter);
    return root;
  }

}
