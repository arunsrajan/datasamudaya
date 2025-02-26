/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.common.utils.sql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * @author arun
 * Sql Optimizer class apache calcite with volcano planner
 */
public class Optimizer {

	private final CalciteConnectionConfig config;
	private final SqlValidator validator;
	private final SqlToRelConverter converter;
	private final VolcanoPlanner planner;

	public Optimizer(
			CalciteConnectionConfig config,
			SqlValidator validator,
			SqlToRelConverter converter,
			VolcanoPlanner planner
	) {
		this.config = config;
		this.validator = validator;
		this.converter = converter;
		this.planner = planner;
	}

	public static Optimizer create(SimpleSchema schema) {
		RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

		Properties configProperties = new Properties();
		configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
		configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
		configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
		CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

		CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
		rootSchema.add(schema.getSchemaName(), schema);
		Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
				rootSchema,
				List.of(schema.getSchemaName()),
				typeFactory,
				config
		);


		SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
		List<SqlFunction> sqlFunctions = Functions.getAllSqlFunctions();

		SqlOperatorTable customSqlOperatorTable = SqlOperatorTables.of(sqlFunctions);
		SqlOperatorTable operatorTable = new SqlFunctionsChainedOperatorTable(Arrays.asList(sqlStdOperatorTable,customSqlOperatorTable));


		SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
				.withLenientOperatorLookup(config.lenientOperatorLookup())
				.withDefaultNullCollation(config.defaultNullCollation())
				.withIdentifierExpansion(true);

		SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

		VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
		planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

		RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

		SqlToRelConverter.Config converterConfig = SqlToRelConverter.config();
		converterConfig = converterConfig.withTrimUnusedFields(true)
				.withExpand(true);

		SqlToRelConverter converter = new SqlToRelConverter(
				null,
				validator,
				catalogReader,
				cluster,
				StandardConvertletTable.INSTANCE,
				converterConfig
		);

		return new Optimizer(config, validator, converter, planner);
	}

	public SqlNode parse(String sql) throws Exception {
		SqlParser.Config parserConfig = SqlParser.config();
		parserConfig = parserConfig.withCaseSensitive(config.caseSensitive()).
		withUnquotedCasing(config.unquotedCasing()).
		withQuotedCasing(config.quotedCasing()).
		withConformance(config.conformance());

		SqlParser parser = SqlParser.create(sql, parserConfig);

		return parser.parseStmt();
	}

	public SqlNode validate(SqlNode node) {
		return validator.validate(node);
	}

	public RelNode convert(SqlNode node) {
		RelRoot root = converter.convertQuery(node, false, true);

		return root.rel;
	}

	public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
		Program program = Programs.of(RuleSets.ofList(rules));

		return program.run(
				planner,
				node,
				requiredTraitSet,
				Collections.emptyList(),
				Collections.emptyList()
		);
	}

}
