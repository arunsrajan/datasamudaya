package com.github.datasamudaya.stream.sql.dataframe.build;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import com.github.datasamudaya.stream.sql.build.StreamPipelineCalciteSqlBuilder;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.utils.Optimizer;
import com.github.datasamudaya.stream.utils.SimpleSchema;
import com.github.datasamudaya.stream.utils.SimpleTable;

/**
 * The DataFrame Object to manipulate the columns and build output using calcite
 * @author arun
 *
 */
public class DataFrame {
	DataFrameContext dfcontext;
	RelBuilder builder;
	List<String> columns;
	SimpleSchema simpleschema;
	protected DataFrame(DataFrameContext dfcontext) {
		this.dfcontext = dfcontext;
		SchemaPlus schemaplus = Frameworks.createRootSchema(true);
		SimpleSchema.Builder schemabuilder = SimpleSchema.newBuilder(dfcontext.db);
		schemabuilder.addTable(
				getSimpleTable(dfcontext.tablename, dfcontext.columns, dfcontext.types.toArray(new SqlTypeName[0])));
		simpleschema = schemabuilder.build();
		// Add your custom schema to the root schema
		schemaplus.add(dfcontext.db, simpleschema);
		builder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schemaplus).build());
		builder = builder.scan(dfcontext.db, dfcontext.tablename);
		columns = Arrays.asList(dfcontext.columns);
	}

	/**
	 * Get Table object from tablename fields, types
	 * @param tablename
	 * @param fields
	 * @param types
	 * @return SimpleTable object
	 */
	private static SimpleTable getSimpleTable(String tablename, String[] fields, SqlTypeName[] types) {
		SimpleTable.Builder builder = SimpleTable.newBuilder(tablename);
		int typecount = 0;
		for (String field : fields) {
			builder = builder.addField(field, types[typecount]);
			typecount++;
		}
		return builder.withRowCount(60000L).build();
	}

	/**
	 * The function builds only the required columns
	 * @param requiredcolumns
	 * @return dataframe object
	 */
	public DataFrame select(String... requiredcolumns) {
		List<RexNode> columnsbuilder = new ArrayList<>();
		for (String column: requiredcolumns) {
			columnsbuilder.add(builder.field(columns.indexOf(column)));
		}
		builder = builder.project(columnsbuilder);
		return this;
	}
	
	/**
	 * The function builds the filter condition
	 * @param filtercondition
	 * @return dataframe object
	 */
	public DataFrame filter(Predicate filtercondition) {
		PredicateToRexNodeConverter predtorex = new PredicateToRexNodeConverter(builder);
		builder = builder.filter(predtorex.convertPredicateToRexNode(filtercondition));
		return this;
	}
	
	/**
	 * The method executes the sql
	 * @return executed sql output
	 * @throws Exception 
	 */
	public Object execute() throws Exception {
		RelNode relnode = builder.build();
		String sql = convertRelNodeToSqlString(relnode, SqlDialect.DatabaseProduct.H2.getDialect());
		StreamPipelineSql sps = StreamPipelineCalciteSqlBuilder.newBuilder()
		.add(dfcontext.folder, dfcontext.tablename, 
				Arrays.asList(dfcontext.columns), dfcontext.types)
		.setSql(sql)
		.setFileformat(dfcontext.fileformat)
		.setDb(dfcontext.db)
		.setPipelineConfig(dfcontext.pipelineconfig)
		.build();
		
		return sps.collect(true, null);
	}

	/**
	 * Converts the RelationNode to SqlNode to SQL Node.
	 * @param relNode
	 * @param sqlDialect
	 * @return SqlNode object
	 */
	public static String convertRelNodeToSqlString(RelNode relNode, SqlDialect sqlDialect) {
		RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
		SqlNode sqlNode = converter.visitRoot(relNode).asStatement();
		return sqlNode.toSqlString(sqlDialect).getSql();
	}
}
