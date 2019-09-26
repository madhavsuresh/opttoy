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

import net.jcip.annotations.NotThreadSafe;
import org.apache.calcite.adapter.opttoy.OptToyRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;


import com.google.common.collect.ImmutableMap;


import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assume.assumeTrue;

/**
 * Tests for the {@code org.apache.calcite.adapter.cassandra} package.
 *
 * <p>Will start embedded cassandra cluster and populate it from local {@code twissandra.cql} file.
 * All configuration files are located in test classpath.
 *
 * <p>Note that tests will be skipped if running on JDK11+
 * (which is not yet supported by cassandra) see
 * <a href="https://issues.apache.org/jira/browse/CASSANDRA-9608">CASSANDRA-9608</a>.
 *
 */
// force tests to run sequentially (maven surefire and failsafe are running them in parallel)
// seems like some of our code is sharing static variables (like Hooks) which causes tests
// to fail non-deterministically (flaky tests).
@NotThreadSafe
public class OptToyAdapterTest {

  @BeforeClass
  public static void setUp() {
    // run tests only if explicitly enabled
  }
  public static Frameworks.ConfigBuilder config() {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    return Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(
                    CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT))
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test public void testOptToyFilter() {
    VolcanoPlanner p = new VolcanoPlanner();
    OptToyRules.OptToyTestFilter oF = new OptToyRules.OptToyTestFilter();
    p.addRule(oF);
    final RelBuilder builder = RelBuilder.create(config().build());
    final RelNode node = builder
            .scan("EMP")
            .build();
    System.out.println(RelOptUtil.toString(node));
  }

}

// End CassandraAdapterTest.java
