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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	SimpleSchema simpleschema;
	private static final Logger log = LoggerFactory.getLogger(DataFrame.class);

	protected DataFrame(DataFrameContext dfcontext) {
		this.dfcontext = dfcontext;		
		SchemaPlus schemaplus = Frameworks.createRootSchema(true);
		SimpleSchema.Builder schemabuilder = SimpleSchema.newBuilder(dfcontext.db);
		int tablecount = dfcontext.folder.size();
		int index = 0;
		while(index<tablecount) {
			schemabuilder.addTable(
					getSimpleTable(dfcontext.tablename.get(index), dfcontext.columns.get(index), dfcontext.types.get(index).toArray(new SqlTypeName[0])));
			index++;
		}		
		simpleschema = schemabuilder.build();
		// Add your custom schema to the root schema
		schemaplus.add(dfcontext.db, simpleschema);
		builder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schemaplus).build());
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
	 * The function scans the table
	 * @param tablename
	 * @return dataframe object
	 */
	public DataFrame scantable(String tablename) {
		builder.scan(dfcontext.db, tablename);
		return this;
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
	 * Inner Join
	 * @param right
	 * @param joincondition
	 * @return dataframe object
	 */
	public DataFrame innerjoin(DataFrame right, Predicate<?> joincondition) {
		RelBuilder joinbuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
		joinbuilder.push(builder.build());
		joinbuilder.push(right.builder.build());
		int tablecount = right.dfcontext.folder.size();
		int index = 0;
		while(index<tablecount) {
			dfcontext.addTable(right.dfcontext.folder.get(index), right.dfcontext.columns.get(index), right.dfcontext.tablename.get(index),right.dfcontext.types.get(index));
			index++;
		}
		PredicateToRexNodeConverter predtorex = new PredicateToRexNodeConverter(joinbuilder, true);
		RexNode joinconditionnode = predtorex.convertPredicateToRexNode(joincondition);		
		builder = joinbuilder.join(JoinRelType.INNER, joinconditionnode);
		return this;
	}
	
	/**
	 * Left Join
	 * @param right
	 * @param joincondition
	 * @return dataframe object
	 */
	public DataFrame leftjoin(DataFrame right, Predicate<?> joincondition) {
		RelBuilder joinbuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
		joinbuilder.push(builder.build());
		joinbuilder.push(right.builder.build());
		int tablecount = right.dfcontext.folder.size();
		int index = 0;
		while(index<tablecount) {
			dfcontext.addTable(right.dfcontext.folder.get(index), right.dfcontext.columns.get(index), right.dfcontext.tablename.get(index),right.dfcontext.types.get(index));
			index++;
		}
		PredicateToRexNodeConverter predtorex = new PredicateToRexNodeConverter(joinbuilder, true);
		RexNode joinconditionnode = predtorex.convertPredicateToRexNode(joincondition);		
		builder = joinbuilder.join(JoinRelType.LEFT, joinconditionnode);
		return this;
	}
	
	/**
	 * Right Join
	 * @param right
	 * @param joincondition
	 * @return dataframe object
	 */
	public DataFrame rightjoin(DataFrame right, Predicate<?> joincondition) {
		RelBuilder joinbuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
		joinbuilder.push(builder.build());
		joinbuilder.push(right.builder.build());
		int tablecount = right.dfcontext.folder.size();
		int index = 0;
		while(index<tablecount) {
			dfcontext.addTable(right.dfcontext.folder.get(index), right.dfcontext.columns.get(index), right.dfcontext.tablename.get(index),right.dfcontext.types.get(index));
			index++;
		}
		PredicateToRexNodeConverter predtorex = new PredicateToRexNodeConverter(joinbuilder, true);
		RexNode joinconditionnode = predtorex.convertPredicateToRexNode(joincondition);		
		builder = joinbuilder.join(JoinRelType.RIGHT, joinconditionnode);
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
		PredicateToRexNodeConverter predtorex = new PredicateToRexNodeConverter(builder, false);
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
		StreamPipelineSqlBuilder spsb = StreamPipelineSqlBuilder.newBuilder().setSql(sql)
				.setFileformat(dfcontext.fileformat)
				.setDb(dfcontext.db)
				.setPipelineConfig(dfcontext.pipelineconfig);
		int index = 0;
		int tablecount = dfcontext.folder.size();
		while(index<tablecount) {
			spsb.add(dfcontext.folder.get(index), dfcontext.tablename.get(index),
					Arrays.asList(dfcontext.columns.get(index)), dfcontext.types.get(index));
			index++;
		}
		
				
		StreamPipelineSql sps = spsb.build();

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
