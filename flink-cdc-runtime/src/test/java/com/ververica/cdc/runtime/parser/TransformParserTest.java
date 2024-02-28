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

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.Expressions;

import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.runtime.parser.metadata.TransformSchemaFactory;
import com.ververica.cdc.runtime.parser.metadata.TransformSqlOperatorTable;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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

/** Unit tests for the {@link TransformParser}. */
public class TransformParserTest {

    private static final Schema CUSTOMERS_SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("order_id", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    @Test
    public void testCalciteParser() {
        SqlSelect parse =
                TransformParser.parseSelect(
                        "select CONCAT(id, order_id) as uniq_id, * from tb where uniq_id > 10 and id is not null");
        Assert.assertEquals(
                "`CONCAT`(`id`, `order_id`) AS `uniq_id`, *", parse.getSelectList().toString());
        Assert.assertEquals("`uniq_id` > 10 AND `id` IS NOT NULL", parse.getWhere().toString());
    }

    @Test
    public void testTransformCalciteValidate() {
        SqlSelect parse =
                TransformParser.parseSelect(
                        "select SUBSTR(id, 1) as uniq_id, * from tb where id is not null");

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", "tb");
        operand.put("columns", CUSTOMERS_SCHEMA.getColumns());
        org.apache.calcite.schema.Schema schema =
                TransformSchemaFactory.INSTANCE.create(
                        rootSchema.plus(), "default_schema", operand);
        rootSchema.add("default_schema", schema);
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path("default_schema"),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        TransformSqlOperatorTable transformSqlOperatorTable = TransformSqlOperatorTable.instance();
        SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(sqlStdOperatorTable, transformSqlOperatorTable),
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
                TransformParser.parseSelect(
                        "select SUBSTR(id, 1) as uniq_id, * from tb where id is not null");

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        Map<String, Object> operand = new HashMap<>();
        operand.put("tableName", "tb");
        operand.put("columns", CUSTOMERS_SCHEMA.getColumns());
        org.apache.calcite.schema.Schema schema =
                TransformSchemaFactory.INSTANCE.create(
                        rootSchema.plus(), "default_schema", operand);
        rootSchema.add("default_schema", schema);
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CalciteCatalogReader calciteCatalogReader =
                new CalciteCatalogReader(
                        rootSchema,
                        rootSchema.path("default_schema"),
                        factory,
                        new CalciteConnectionConfigImpl(new Properties()));
        TransformSqlOperatorTable transformSqlOperatorTable = TransformSqlOperatorTable.instance();
        SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        SqlOperatorTables.chain(sqlStdOperatorTable, transformSqlOperatorTable),
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
    }

    @Test
    public void testParseComputedColumnNames() {
        List<String> computedColumnNames =
                TransformParser.parseComputedColumnNames("CONCAT(id, order_id) as uniq_id, *");
        Assert.assertEquals(new String[] {"uniq_id"}, computedColumnNames.toArray());
    }

    @Test
    public void testParseFilterColumnNameList() {
        List<String> computedColumnNames =
                TransformParser.parseFilterColumnNameList(" uniq_id > 10 and id is not null");
        Assert.assertEquals(new String[] {"uniq_id", "id"}, computedColumnNames.toArray());
    }

    @Test
    public void testTranslateFilterToJaninoExpression() {
        testFilterExpression("id is not null", "null != id");
        testFilterExpression("id is null", "null == id");
        testFilterExpression("id = 1 and uid = 2", "id == 1 && uid == 2");
        testFilterExpression("id = 1 or id = 2", "id == 1 || id == 2");
        testFilterExpression("not (id = 1)", "!id == 1");
        testFilterExpression("id = '1'", "String.valueOf('1').equals(id)");
        testFilterExpression("id <> '1'", "!String.valueOf('1').equals(id)");
        testFilterExpression("d between d1 and d2", "betweenAsymmetric(d, d1, d2)");
        testFilterExpression("d not between d1 and d2", "notBetweenAsymmetric(d, d1, d2)");
        testFilterExpression("d in (d1, d2)", "in(d, d1, d2)");
        testFilterExpression("d not in (d1, d2)", "notIn(d, d1, d2)");
        testFilterExpression("id is false", "false == id");
        testFilterExpression("id is not false", "true == id");
        testFilterExpression("id is true", "true == id");
        testFilterExpression("id is not true", "false == id");
        testFilterExpression("a || b", "concat(a, b)");
        testFilterExpression("CHAR_LENGTH(id)", "charLength(id)");
        testFilterExpression("trim(id)", "trim('BOTH', String.valueOf(' '), id)");
        testFilterExpression(
                "REGEXP_REPLACE(id, '[a-zA-Z]', '')",
                "regexpReplace(id, String.valueOf('[a-zA-Z]'), String.valueOf(''))");
        testFilterExpression("upper(id)", "upper(id)");
        testFilterExpression("lower(id)", "lower(id)");
        testFilterExpression("concat(a,b)", "concat(a, b)");
        testFilterExpression("SUBSTRING(a,1)", "substring(a, 1)");
        testFilterExpression("id like '^[a-zA-Z]'", "like(id, String.valueOf('^[a-zA-Z]'))");
        testFilterExpression("id not like '^[a-zA-Z]'", "notLike(id, String.valueOf('^[a-zA-Z]'))");
        testFilterExpression("power(2,2)", "power(2, 2)");
        testFilterExpression("asb(2)", "asb(2)");
        testFilterExpression("ceil(2)", "ceil(2)");
        testFilterExpression("floor(2)", "floor(2)");
        testFilterExpression("round(2,2)", "round(2, 2)");
        testFilterExpression("uuid()", "uuid()");
        testFilterExpression("id = LOCALTIME", "id == localtime()");
        testFilterExpression("id = LOCALTIMESTAMP", "id == localtimestamp()");
        testFilterExpression("id = CURRENT_TIME", "id == currentTime()");
        testFilterExpression("id = CURRENT_DATE", "id == currentDate()");
        testFilterExpression("id = CURRENT_TIMESTAMP", "id == currentTimestamp()");
        testFilterExpression("NOW()", "now()");
        testFilterExpression("YEAR(dt)", "year(dt)");
        testFilterExpression("QUARTER(dt)", "quarter(dt)");
        testFilterExpression("MONTH(dt)", "month(dt)");
        testFilterExpression("WEEK(dt)", "week(dt)");
        testFilterExpression(
                "DATE_FORMAT(dt, 'yyyy-MM-dd')", "dateFormat(dt, String.valueOf('yyyy-MM-dd'))");
        testFilterExpression(
                "TO_DATE(dt, 'yyyy-MM-dd')", "toDate(dt, String.valueOf('yyyy-MM-dd'))");
        testFilterExpression("TO_TIMESTAMP(dt)", "toTimestamp(dt)");
        testFilterExpression(
                "TIMESTAMP_DIFF('DAY', dt1, dt2)",
                "timestampDiff(String.valueOf('DAY'), dt1, dt2)");
        testFilterExpression("NULLIF(a,b)", "nullif(a, b)");
        testFilterExpression("COALESCE(a,b,c)", "coalesce(a, b, c)");
        testFilterExpression("id + 2", "id + 2");
        testFilterExpression("id - 2", "id - 2");
        testFilterExpression("id * 2", "id * 2");
        testFilterExpression("id / 2", "id / 2");
        testFilterExpression("id % 2", "id % 2");
        testFilterExpression("a < b", "a < b");
        testFilterExpression("a <= b", "a <= b");
        testFilterExpression("a > b", "a > b");
        testFilterExpression("a >= b", "a >= b");
        testFilterExpression(
                "__table_name__ = 'tb'", "String.valueOf('tb').equals(__table_name__)");
        testFilterExpression(
                "__database_name__ = 'tb'", "String.valueOf('tb').equals(__database_name__)");
        testFilterExpression("upper(lower(id))", "upper(lower(id))");
        testFilterExpression(
                "abs(uniq_id) > 10 and id is not null", "abs(uniq_id) > 10 && null != id");
    }

    @Test
    public void testSqlCall() {
        ApiExpression apiExpression = Expressions.concat("1", "2");
        ApiExpression substring = apiExpression.substring(1);
        System.out.println(substring);
    }

    private void testFilterExpression(String expression, String expressionExpect) {
        String janinoExpression =
                TransformParser.translateFilterExpressionToJaninoExpression(expression);
        Assert.assertEquals(expressionExpect, janinoExpression);
    }
}
