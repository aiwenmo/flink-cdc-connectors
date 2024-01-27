/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.parser;

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.parser.validate.FlinkCDCOperatorTable;
import com.ververica.cdc.runtime.parser.validate.FlinkCDCSchemaFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Unit tests for the {@link FlinkSqlParser}. */
public class FlinkSqlParserTest {

    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("order_id", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    public void testCalciteParser() {
        SqlSelect parse =
                FlinkSqlParser.parseSelect(
                        "select CONCAT(id, order_id) as uniq_id, * from tb where uniq_id > 10 and id is not null");
        Assert.assertEquals(
                "`CONCAT`(`id`, `order_id`) AS `uniq_id`, *", parse.getSelectList().toString());
        Assert.assertEquals("`uniq_id` > 10 AND `id` IS NOT NULL", parse.getWhere().toString());
    }

    @Test
    public void testFlinkCalciteValidate() {
        SqlSelect parse =
                FlinkSqlParser.parseSelect(
                        "select SUBSTR(id, 1) as uniq_id, * from tb where id is not null");

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", "tb");
        operand.put("columns", CUSTOMERS_SCHEMA.getColumns());
        org.apache.calcite.schema.Schema schema =
                FlinkCDCSchemaFactory.INSTANCE.create(rootSchema.plus(), "default_schema", operand);
        rootSchema.add("default_schema", schema);
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path("default_schema"),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        FlinkCDCOperatorTable flinkCDCOperatorTable = FlinkCDCOperatorTable.instance();
        FlinkSqlOperatorTable flinkSqlOperatorTable = FlinkSqlOperatorTable.instance(false);
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(flinkSqlOperatorTable, flinkCDCOperatorTable),
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validateSqlNode = validator.validate(parse);
        Assert.assertEquals(
                "SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`",
                parse.getSelectList().toString());
        Assert.assertEquals("`tb`.`id` IS NOT NULL", parse.getWhere().toString());
        Assert.assertEquals(
                "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                        + "FROM `default_schema`.`tb` AS `tb`\n"
                        + "WHERE `tb`.`id` IS NOT NULL",
                validateSqlNode.toString().replaceAll("\r\n", "\n"));
    }

    @Test
    public void testCalciteRelNode() {
        SqlSelect parse =
                FlinkSqlParser.parseSelect(
                        "select SUBSTR(id, 1) as uniq_id, * from tb where id is not null");

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", "tb");
        operand.put("columns", CUSTOMERS_SCHEMA.getColumns());
        org.apache.calcite.schema.Schema schema =
                FlinkCDCSchemaFactory.INSTANCE.create(rootSchema.plus(), "default_schema", operand);
        rootSchema.add("default_schema", schema);
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path("default_schema"),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        FlinkCDCOperatorTable flinkCDCOperatorTable = FlinkCDCOperatorTable.instance();
        FlinkSqlOperatorTable flinkSqlOperatorTable = FlinkSqlOperatorTable.instance(false);
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(flinkSqlOperatorTable, flinkCDCOperatorTable),
                        calciteCatalogReader,
                        factory,
                        SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validateSqlNode = validator.validate(parse);
        RexBuilder rexBuilder = new RexBuilder(factory);
        HepProgramBuilder builder = new HepProgramBuilder();
        HepPlanner planner = new HepPlanner(builder.build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter.Config config = SqlToRelConverter.config().withTrimUnusedFields(false);
        SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(
                        null,
                        validator,
                        calciteCatalogReader,
                        cluster,
                        StandardConvertletTable.INSTANCE,
                        config);
        RelRoot relRoot = sqlToRelConverter.convertQuery(validateSqlNode, false, true);
        relRoot = relRoot.withRel(sqlToRelConverter.flattenTypes(relRoot.rel, true));
        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        relRoot = relRoot.withRel(RelDecorrelator.decorrelateQuery(relRoot.rel, relBuilder));
        RelNode relNode = relRoot.rel;
        Assert.assertEquals(
                "SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`",
                parse.getSelectList().toString());
        Assert.assertEquals("`tb`.`id` IS NOT NULL", parse.getWhere().toString());
        Assert.assertEquals(
                "SELECT SUBSTR(`tb`.`id`, 1) AS `uniq_id`, `tb`.`id`, `tb`.`order_id`\n"
                        + "FROM `default_schema`.`tb` AS `tb`\n"
                        + "WHERE `tb`.`id` IS NOT NULL",
                validateSqlNode.toString().replaceAll("\r\n", "\n"));
        Assert.assertEquals(
                "rel#5:LogicalProject.(input=LogicalTableScan#4,exprs=[SUBSTR($0, 1), $0, $1])",
                relNode.toString());
    }

    @Test
    public void testParseComputedColumnNames() {
        List<String> computedColumnNames =
                FlinkSqlParser.parseComputedColumnNames("CONCAT(id, order_id) as uniq_id, *");
        Assert.assertEquals(new String[] {"uniq_id"}, computedColumnNames.toArray());
    }

    @Test
    public void testParseFilterColumnNameList() {
        List<String> computedColumnNames =
                FlinkSqlParser.parseFilterColumnNameList(" uniq_id > 10 and id is not null");
        Assert.assertEquals(new String[] {"uniq_id", "id"}, computedColumnNames.toArray());
    }

    @Test
    public void testTranslateFilterToJaninoExpression() {
        testFilterExpression("id is not null", "null != id");
        testFilterExpression("id is null", "null == id");
        testFilterExpression("id = 1 and uid = 2", "id == 1 && uid == 2");
        testFilterExpression("id = 1 or id = 2", "id == 1 || id == 2");
        testFilterExpression("id = '1'", "String.valueOf('1').equals(id)");
        testFilterExpression("id <> '1'", "!String.valueOf('1').equals(id)");
        testFilterExpression("upper(id)", "upper(id)");
        testFilterExpression("concat(a,b)", "concat(a, b)");
        testFilterExpression("id + 2", "id + 2");
        testFilterExpression("id - 2", "id - 2");
        testFilterExpression("id * 2", "id * 2");
        testFilterExpression("id / 2", "id / 2");
        testFilterExpression("id % 2", "id % 2");
        testFilterExpression("a < b", "a < b");
        testFilterExpression("a <= b", "a <= b");
        testFilterExpression("a > b", "a > b");
        testFilterExpression("a >= b", "a >= b");
        testFilterExpression("upper(lower(id))", "upper(lower(id))");
        testFilterExpression(
                "abs(uniq_id) > 10 and id is not null", "abs(uniq_id) > 10 && null != id");
    }

    private void testFilterExpression(String expression, String expressionExpect) {
        String janinoExpression =
                FlinkSqlParser.translateFilterExpressionToJaninoExpression(expression);
        Assert.assertEquals(expressionExpect, janinoExpression);
    }
}