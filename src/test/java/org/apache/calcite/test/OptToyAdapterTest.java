/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.opttoy.OptToyRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.function.Function;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlannerTraitTest;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package.
 *
 * <p>Will start embedded cassandra cluster and populate it from local {@code twissandra.cql} file.
 * All configuration files are located in test classpath.
 *
 * <p>Note that tests will be skipped if running on JDK11+ (which is not yet supported by cassandra)
 * see <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>.
 */
// force tests to run sequentially (maven surefire and failsafe are running them in parallel)
// seems like some of our code is sharing static variables (like Hooks) which causes tests
// to fail non-deterministically (flaky tests).
public class OptToyAdapterTest extends RelOptTestBase {

  private static Planner planner;
  private static VolcanoPlanner optimizer;
  private static FrameworkConfig calciteConfig;
  private static SchemaPlus pdnSchema;
  private static CalciteConnection calciteConnection;

  @BeforeClass
  public static void setUp() throws ClassNotFoundException, SQLException {
    // run tests only if explicitly enabled
    String url = "jdbc:postgresql://localhost:5432/tpch_sf1";
    Properties props = new Properties();
    props.setProperty("caseSensitive", "false");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", props);
    calciteConnection = connection.unwrap(CalciteConnection.class);
    Class.forName("org.postgresql.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(url);
    dataSource.setUsername("madhav");
    dataSource.setPassword("password");
    JdbcSchema schema =
        JdbcSchema.create(calciteConnection.getRootSchema(), "name", dataSource, null, null);
    for (String tableName : schema.getTableNames()) {
      Table table = schema.getTable(tableName);
      calciteConnection.getRootSchema().add(tableName, table);
    }
    pdnSchema = calciteConnection.getRootSchema();
    SqlParser.Config parserConf =
        SqlParser.configBuilder().setCaseSensitive(false).setLex(Lex.MYSQL).build();
    calciteConfig =
        Frameworks.newConfigBuilder().defaultSchema(pdnSchema).parserConfig(parserConf).programs(Programs.ofRules(FilterProjectTransposeRule.INSTANCE)).build();


    planner = Frameworks.getPlanner(calciteConfig);


    //SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl((RelDataTypeSystem.DEFAULT);


    final RelDataTypeFactory typeFactory = planner.getTypeFactory();




  }
  public static SqlToRelConverter createSqlToRelConverter() {
    RelDataTypeFactory typeFactory = planner.getTypeFactory();

    final Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            CalciteSchema.from(pdnSchema),
            CalciteSchema.from(pdnSchema).path(null),
            (JavaTypeFactory) planner.getTypeFactory(), calciteConnection.config());

    final Context context = calciteConfig.getContext();
    SqlConformance conformance = SqlConformanceEnum.DEFAULT;
    if (context != null) {
      final CalciteConnectionConfig connectionConfig =
              context.unwrap(CalciteConnectionConfig.class);
      if (connectionConfig != null) {
        conformance= connectionConfig.conformance();
      }
    }

    final SqlValidator validator = new LocalValidatorImpl(calciteConfig.getOperatorTable(), catalogReader, typeFactory,
            conformance);
    validator.setIdentifierExpansion(true);
    final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    RelOptCluster cluster =
            RelOptCluster.create(optimizer, rexBuilder);

    final SqlToRelConverter.ConfigBuilder configBuilder =  SqlToRelConverter.configBuilder().withTrimUnusedFields(true).withDecorrelationEnabled(true);
    SqlToRelConverter.Config config = configBuilder.build();
    return new SqlToRelConverter(null, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);
  }

  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(
            CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL))
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }
  /**
   * Creates a config builder that will contain a view, "MYVIEW", and also the SCOTT JDBC schema,
   * whose tables implement {@link org.apache.calcite.schema.TranslatableTable}.
   */
  static Frameworks.ConfigBuilder expandingConfig(Connection connection) throws SQLException {
    final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    final SchemaPlus root = calciteConnection.getRootSchema();
    CalciteAssert.SchemaSpec spec = CalciteAssert.SchemaSpec.SCOTT;
    CalciteAssert.addSchema(root, spec);
    final String viewSql =
        String.format(Locale.ROOT, "select * from \"%s\".\"%s\" where 1=1", spec.schemaName, "EMP");

    // create view
    ViewTableMacro macro =
        ViewTable.viewMacro(
            root, viewSql, Collections.singletonList("test"), Arrays.asList("test", "view"), false);

    // register view (in root schema)
    root.add("MYVIEW", macro);

    return Frameworks.newConfigBuilder().defaultSchema(root);
  }

  @Test
  public void testScan() {
    // Equivalent SQL:
    //   SELECT *
    //   FROM emp
    final RelNode root = RelBuilder.create(config().build()).scan("EMP").build();
    assertThat(root, hasTree("LogicalTableScan(table=[[scott, EMP]])\n"));
  }

  static RelOptCluster newCluster(VolcanoPlanner planner) {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    return RelOptCluster.create(planner, new RexBuilder(typeFactory));
  }

  @Test
  public void testWhatIsGoinOn() throws SqlParseException, ValidationException, RelConversionException {
    String sql = "SELECT c_custkey from customer where c_custkey=100";
    // String sql = "select n.n_regionkeyzzz from (select * from "
    //       + "(select * from sales.customer) t) n where n.n_nationkey >1";

    optimizer = new VolcanoPlanner();
    optimizer.addRelTraitDef(ConventionTraitDef.INSTANCE);
    optimizer.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
    optimizer.addRule(new OptToyRules.OptToyTestFilter());
    /*
    // add rules
    optimizer.addRule(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
    optimizer.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
    optimizer.addRule(PruneEmptyRules.PROJECT_INSTANCE);
    // add ConverterRule
    optimizer.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    optimizer.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
     */
    SqlNode node = planner.parse(sql);
    node = planner.validate(node);
    SqlToRelConverter converter = createSqlToRelConverter();
    RelRoot n = converter.convertQuery(node, true, true);
    RelNode relNode = n.rel;

    //TODO(madhavsuresh): only works with needsValidation set to true.
    //System.out.println(n);
    //optimizer.addRelTraitDef();
    RelTraitSet desiredTraits =
            relNode.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
    relNode = optimizer.changeTraits(relNode, desiredTraits);
    optimizer.setRoot(relNode);
    optimizer.findBestExp();
    /*
    VolcanoPlanner planner = new VolcanoPlanner(null, null);


    Tester dynamicTester = createDynamicTester().withDecorrelation(true)
            .withClusterFactory(
                    relOptCluster -> RelOptCluster.create(planner, relOptCluster.getRexBuilder()));

    RelRoot root = dynamicTester.convertSqlToRel(sql);
     */

  }

  @Test
  public void testOptToyFilter() {

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    // Below two lines are important for the planner to use collation trait and generate merge join
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    planner.registerAbstractRelationalRules();

    planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);

    RelOptCluster cluster = newCluster(planner);

    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode logicalPlan =
        relBuilder
            .values(new String[] {"id", "name"}, "2", "a", "1", "b")
            .values(new String[] {"id", "name"}, "1", "x", "2", "y")
            .join(JoinRelType.INNER, "id")
            .build();

    RelTraitSet desiredTraits = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode newRoot = planner.changeTraits(logicalPlan, desiredTraits);
    OptToyRules.OptToyTestFilter oF = new OptToyRules.OptToyTestFilter();
    planner.addRule(oF);
    planner.setRoot(newRoot);

    /*
    RelNode bestExp = planner.findBestExp();
    VolcanoPlanner p = new VolcanoPlanner();
    p.addRelTraitDef(ConventionTraitDef.INSTANCE);
    p.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    p.registerAbstractRelationalRules();
    RelOptCluster cluster = newCluster(p);
    final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(cluster, null);
    RelNode logicalPlan = relBuilder
            .values(new String[]{"id", "name"}, "2", "a", "1", "b")
            .values(new String[]{"id", "name"}, "1", "x", "2", "y")
            .join(JoinRelType.INNER, "id")
            .build();
    RelTraitSet desiredTraits =
            cluster.traitSet().replace(EnumerableConvention.INSTANCE);
    final RelNode newRoot = p.changeTraits(logicalPlan, desiredTraits);
    p.setRoot(newRoot);

     */
    RelNode result = planner.findBestExp();
    System.out.println(RelOptUtil.toString(result));
  }
  private static class LocalValidatorImpl extends SqlValidatorImpl {
    protected LocalValidatorImpl(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlConformance conformance) {
      super(opTab, catalogReader, typeFactory, conformance);
    }

  }
}
