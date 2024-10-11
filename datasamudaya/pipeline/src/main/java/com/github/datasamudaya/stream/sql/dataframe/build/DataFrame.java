package com.github.datasamudaya.stream.sql.dataframe.build;

import static java.util.Objects.isNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.utils.sql.Functions;
import com.github.datasamudaya.common.utils.sql.SimpleSchema;
import com.github.datasamudaya.common.utils.sql.SimpleTable;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;

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
	private static final Logger log = LoggerFactory.getLogger(DataFrame.class);

	protected DataFrame(DataFrameContext dfcontext) {
		this.dfcontext = dfcontext;
		SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
		List<SqlFunction> sqlFunctions = Functions.getAllSqlFunctions();
		sqlFunctions.stream().forEach(sqlFunction -> sqlStdOperatorTable.register(sqlFunction));
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
		RelDataType updatedRowType = builder.peek().getRowType();
		List<RelDataTypeField> updatedFields = updatedRowType.getFieldList();
		List<RexNode> columnsbuilder = new ArrayList<>();
		List<String> addedColumns = new ArrayList<>();
		for (RelDataTypeField field : updatedFields) {
			addedColumns.add(field.getName());
		}
		for (String column : requiredcolumns) {
			columnsbuilder.add(builder.field(addedColumns.indexOf(column)));
		}
		builder = builder.project(columnsbuilder);
		return this;
	}

	/**
	 * The function sorts the output from previous RelNode
	 * @param requiredcolumns
	 * @return dataframe object
	 */
	public DataFrame sortBy(Tuple2<String, String>... requiredcolumns) {
		RelDataType updatedRowType = builder.peek().getRowType();
		List<RelDataTypeField> updatedFields = updatedRowType.getFieldList();
		List<RelFieldCollation> columnsbuilder = new ArrayList<>();
		List<String> addedColumns = new ArrayList<>();
		for (RelDataTypeField field : updatedFields) {
			addedColumns.add(field.getName());
		}
		for (Tuple2<String, String> column : requiredcolumns) {
			RelFieldCollation relfieldcollation = new RelFieldCollation(addedColumns.indexOf(column.v1), isNull(column.v2) || "ASC".equals(column.v2) ? Direction.ASCENDING : Direction.DESCENDING);
			columnsbuilder.add(relfieldcollation);
		}
		RelCollation relcollation = RelCollations.of(columnsbuilder);
		builder = builder.sort(relcollation);
		return this;
	}

	/**
	 * The function sorts the output from previous RelNode using ordinal
	 * @param ordinal
	 * @return dataframe object
	 */
	public DataFrame sortByOrdinal(Tuple2<Integer, String>... ordinal) {
		List<RelFieldCollation> columnsbuilder = new ArrayList<>();
		for (Tuple2<Integer, String> column : ordinal) {
			RelFieldCollation relfieldcollation = new RelFieldCollation(column.v1, isNull(column.v2) || "ASC".equals(column.v2) ? Direction.ASCENDING : Direction.DESCENDING);
			columnsbuilder.add(relfieldcollation);
		}
		RelCollation relcollation = RelCollations.of(columnsbuilder);
		builder = builder.sort(relcollation);
		return this;
	}

	/**
	 * Select With Ordinal
	 * @param ordinal
	 * @return dataframe object
	 */
	public DataFrame select(Integer... ordinal) {
		List<RexNode> columnsbuilder = new ArrayList<>();
		for (Integer column : ordinal) {
			columnsbuilder.add(builder.field(column));
		}
		builder = builder.project(columnsbuilder);
		return this;
	}

	/**
	 * The function add functions to the select expression
	 * @param funcbuilder
	 * @return dataframe object
	 */
	public DataFrame selectWithFunc(FunctionBuilder funcbuilder) {
		List<RexNode> functions = funcbuilder.build(builder);
		builder = builder.project(functions);
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
	 * This function builds the aggregate functions and adds to the builder
	 * @param aggfunctionbuilder
	 * @param groupby
	 * @return dataframe object
	 */
	public DataFrame aggregate(AggregateFunctionBuilder aggfunctionbuilder, String... groupby) {
		List<AggCall> aggfunctions = aggfunctionbuilder.build(builder);
		builder.aggregate(builder.groupKey(groupby), aggfunctions.toArray(new AggCall[0]));
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
		log.debug("SQL From DataFrame Builder {}", sql);
		StreamPipelineSql sps = StreamPipelineSqlBuilder.newBuilder()
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
	protected static String convertRelNodeToSqlString(RelNode relNode, SqlDialect sqlDialect) {
		RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
		SqlNode sqlNode = converter.visitRoot(relNode).asStatement();
		return sqlNode.toSqlString(sqlDialect).getSql();
	}
}
