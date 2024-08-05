package com.github.datasamudaya.tasks.scheduler.sql;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableIntersect;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.adapter.enumerable.EnumerableSortedAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.stream.StreamPipeline;
import com.github.datasamudaya.stream.sql.RequiredColumnsExtractor;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;
import com.github.datasamudaya.tasks.executor.Reducer;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationBuilder;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.validation.Validation;
import net.sf.jsqlparser.util.validation.ValidationError;
import net.sf.jsqlparser.util.validation.feature.DatabaseType;

public class MapReduceApplicationSqlBuilder implements Serializable {
	private static final long serialVersionUID = 6360524229924008489L;
	private static final Logger log = LoggerFactory.getLogger(MapReduceApplicationSqlBuilder.class);
	String sql;
	ConcurrentMap<String, String> tablefoldermap = new ConcurrentHashMap<>();
	ConcurrentMap<String, List<String>> tablecolumnsmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap = new ConcurrentHashMap<>();
	String hdfs;
	transient JobConfiguration jc;
	String db;

	private MapReduceApplicationSqlBuilder() {

	}

	/**
	 * Creates a new sql builder object.
	 * 
	 * @return sql builder object.
	 */
	public static MapReduceApplicationSqlBuilder newBuilder() {
		return new MapReduceApplicationSqlBuilder();
	}

	/**
	 * This function adds the sql parameters like folder, tablename, columns and
	 * sqltypes.
	 * 
	 * @param folder
	 * @param tablename
	 * @param columns
	 * @param sqltypes
	 * @return sql builder object
	 */
	public MapReduceApplicationSqlBuilder add(String folder, String tablename, List<String> columns,
			List<SqlTypeName> sqltypes) {
		tablefoldermap.put(tablename, folder);
		tablecolumnsmap.put(tablename, columns);
		tablecolumntypesmap.put(tablename, sqltypes);
		return this;
	}

	/**
	 * Sets HDFS URI
	 * 
	 * @param hdfs
	 * @return sql builder object
	 */
	public MapReduceApplicationSqlBuilder setHdfs(String hdfs) {
		this.hdfs = hdfs;
		return this;
	}

	/**
	 * Sets the JobConfiguration object to run sql using the configuration.
	 * 
	 * @param jc
	 * @return sql builder object
	 */
	public MapReduceApplicationSqlBuilder setJobConfiguration(JobConfiguration jc) {
		this.jc = jc;
		return this;
	}

	/**
	 * Sets the sql query.
	 * 
	 * @param sql
	 * @return sql builder object
	 */
	public MapReduceApplicationSqlBuilder setSql(String sql) {
		this.sql = sql;
		return this;
	}
	
	
	/**
	 * database schema
	 * @param db
	 * @return database schema
	 */
	public MapReduceApplicationSqlBuilder setDb(String db) {
		this.db = db;
		return this;
	}

	/**
	 * The build method to create sql pipeline object.
	 * 
	 * @return SQL pipeline object
	 * @throws Exception
	 */
	public Object build() throws Exception {
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		Validation validation = new Validation(
				Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB, DatabaseType.POSTGRESQL, DatabaseType.H2),
				sql);
		List<ValidationError> errors = validation.validate();
		if (!CollectionUtils.isEmpty(errors)) {
			log.error("Syntax error in SQL {}", errors);
			throw new Exception("Syntax error in SQL");
		}
		RelNode relnode = SQLUtils.validateSql(tablecolumnsmap, tablecolumntypesmap, sql, db, isDistinct);
		descendants.put(relnode, false);
		requiredcolumnindex = new RequiredColumnsExtractor().getRequiredColumnsByTable(relnode);
		log.info("Required Columns: {}", requiredcolumnindex);
		mrab = MapReduceApplicationBuilder.newBuilder();
		mrab.setJobConf(jc).setOutputfolder("/aircararrivaldelay");		
		execute(relnode, 0);
		return mrab.build();
	}
	
	private final Map<RelNode, Boolean> descendants = new ConcurrentHashMap<>();
	AtomicBoolean isDistinct = new AtomicBoolean(false);
	private Map<String, Set<String>> requiredcolumnindex;
	MapReduceApplicationBuilder mrab;
	
	protected void execute(RelNode relNode, int depth) throws Exception {

		if (relNode instanceof EnumerableTableScan ets) {
			String table = ets.getTable().getQualifiedName().get(1);
			List<String> reqcolindex = nonNull(requiredcolumnindex.get(table)) ? 	new ArrayList<>(requiredcolumnindex.get(table)) : new ArrayList<>();
			List<String> columns = tablecolumnsmap.get(table);
			Set<String> reqcolumns = reqcolindex.stream().map(index->columns.get(Integer.parseInt(index))).collect(Collectors.toCollection(LinkedHashSet::new));
			Map<String, Long> tablecolindex = new ConcurrentHashMap<>();
			for(int colindex=0;colindex<columns.size();colindex++) {
				tablecolindex.put(columns.get(colindex), Long.valueOf(colindex));
			}
			mrab.addMapper(new MapperReducerSqlMapper(table, reqcolumns,					
					tablecolumntypesmap.get(table),tablecolindex),
					tablefoldermap.get(table));

		} else if (relNode instanceof EnumerableFilter ef) {
			RexNode condit = ef.getCondition();
			
		} else if (relNode instanceof EnumerableSort es) {
			
		} else if (relNode instanceof EnumerableHashJoin ehj) {
			
		} else if(relNode instanceof EnumerableNestedLoopJoin enlj) {
			
		} else if (relNode instanceof EnumerableIntersect) {
			
		} else if (relNode instanceof EnumerableProject ep) {
			mrab.addReducer(new MapperReducerSqlReducer());
		} else if (relNode instanceof EnumerableAggregate || relNode instanceof EnumerableSortedAggregate) {
			
			if (isDistinct.get()) {
			}
		}
		
		List<RelNode> inputs = relNode.getInputs();
		if (CollectionUtils.isNotEmpty(inputs)) {
			StreamPipeline<?> sp = null;
			List<StreamPipeline<Object[]>> childs = new ArrayList<>();
			for (RelNode child : inputs) {
				descendants.put(child, true);
				execute(child, depth + 1);
			}
		}
	}
	
	class MapperReducerSqlMapper implements Mapper<Long, String, Context> {
		private static final long serialVersionUID = 3328584603251312114L;
		ConcurrentMap<String, List<String>> tablecolumns = new ConcurrentHashMap<>(tablecolumnsmap);
		String maintablename;
		Set<String> selectcols;
		List<SqlTypeName> tablecolumntypes;
		Map<String, Long> tablecolindexmap;

		public MapperReducerSqlMapper(String maintablename, Set<String> selectcols,
				List<SqlTypeName> tablecolumntypes, Map<String, Long> tablecolindexmap) {
			this.maintablename = maintablename;
			this.selectcols = selectcols;
			this.tablecolindexmap = tablecolindexmap;
			this.tablecolumntypes = tablecolumntypes;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void map(Long index, String line, Context context) {
			try {
				var csvformat = CSVFormat.DEFAULT.withQuote('"').withEscape('\\');
				List<String> columns = tablecolumns.get(maintablename);
				csvformat = csvformat.withDelimiter(',').withHeader(columns.toArray(new String[columns.size()]))
						.withIgnoreHeaderCase().withIgnoreEmptyLines(true).withTrim();
				CSVParser parser = csvformat.parse(new StringReader(line));
				parser.forEach(csvrecord -> {					
						// Extract relevant columns
						Map<String, Object> valuemap = null;
						if (CollectionUtils.isNotEmpty(selectcols)) {
							valuemap = new HashMap<String, Object>();
							for (String column : selectcols) {
								// Output as key-value pair
								valuemap.put(column, SQLUtils.getValueMR(csvrecord.get(column),
										tablecolumntypes.get(tablecolindexmap.get(column).intValue())));
								context.put(maintablename, Tuple.tuple(valuemap));
							}
						}
				});
			}
			catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		}

	}
	
	
	class MapperReducerSqlReducer implements Reducer<String, Tuple, Context> {
		@Override
		public void reduce(String key, List<Tuple> values, Context context) {
			for(Tuple tuple: values) {
				if(tuple instanceof Tuple1 maptup)
				context.put(key, maptup.v1);
			}
			log.info("In Reduce Output Values {}", context.keys().size());
		}
	}
	
	class MapperReducerSqlCombiner implements Combiner<String, Tuple, Context>{
		private static final long serialVersionUID = 3328584603251312114L;
		List<FunctionWithCols> functionwithcols = new ArrayList<>();
		boolean isaggfunction;

		public MapperReducerSqlCombiner(List<FunctionWithCols> functionwithcols, boolean isaggfunction) {
			this.functionwithcols = functionwithcols;
			this.isaggfunction = isaggfunction;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void combine(String key, List<Tuple> tuples, Context context) {
			boolean isnotempty = CollectionUtils.isNotEmpty(functionwithcols);
			boolean istuple2 = tuples.get(0) instanceof Tuple2;
			if (!istuple2) {
				Map<String, Double> valuesaggregate = (Map<String, Double>) ((Tuple1) tuples.remove(0)).v1;
				for (Tuple tuple : tuples) {
					if (isnotempty && isaggfunction) {
						if (tuple instanceof Tuple1 tuple1) {
							Map<String, Double> fnvalues = (Map<String, Double>) tuple1.v1();
							fnvalues.keySet().forEach(fnkey -> {
								if (fnkey.startsWith("min")) {
									valuesaggregate.put(fnkey,
											Math.min(valuesaggregate.get(fnkey), fnvalues.get(fnkey)));
								}
								if (fnkey.startsWith("max")) {
									valuesaggregate.put(fnkey,
											Math.max(valuesaggregate.get(fnkey), fnvalues.get(fnkey)));
								} else {
									valuesaggregate.put(fnkey, valuesaggregate.get(fnkey) + fnvalues.get(fnkey));
								}
							});
						}
					} else {
						context.put("Reducer", Tuple.tuple(key, tuple));
					}
				}
				if (isnotempty && isaggfunction) {
					context.put("Reducer", Tuple.tuple(key, Tuple.tuple(valuesaggregate)));
				}
			} else {
				List<Tuple2<Map<String, Object>, Map<String, Double>>> tuplesgrpby = (List) tuples;
				Map<Map<String, Object>, Map<String, Double>> valuesagg = tuplesgrpby.stream().collect(
						Collectors.toMap(tuplekey -> tuplekey.v1, tuplevalue -> tuplevalue.v2, (values1, values2) -> {
							values1.keySet().forEach(fnkey -> {
								if (fnkey.startsWith("min")) {
									values1.put(fnkey, Math.min(values1.get(fnkey), values2.get(fnkey)));
								} else if (fnkey.startsWith("max")) {
									values1.put(fnkey, Math.max(values1.get(fnkey), values2.get(fnkey)));
								} else {
									values1.put(fnkey, values1.get(fnkey) + values2.get(fnkey));
								}
							});
							return values1;
						}));
				valuesagg.entrySet().stream().map(entry -> Tuple.tuple(entry.getKey(), entry.getValue()))
						.forEach(value -> context.put("Reducer", Tuple.tuple(key, value)));
			}
		}

		PlainSelect ps = null;
		Set<String> finalselectcolumns = null;		

		/**
		 * Compare two objects to sort in order.
		 * 
		 * @param obj1
		 * @param obj2
		 * @return value in Long for sorting.
		 */
		public int compareTo(Object obj1, Object obj2) {
			if (obj1 instanceof Integer val1 && obj2 instanceof Integer val2) {
				return val1.compareTo(val2);
			} else if (obj1 instanceof Long val1 && obj2 instanceof Long val2) {
				return val1.compareTo(val2);
			} else if (obj1 instanceof Double val1 && obj2 instanceof Double val2) {
				return val1.compareTo(val2);
			} else if (obj1 instanceof Float val1 && obj2 instanceof Float val2) {
				return val1.compareTo(val2);
			} else if (obj1 instanceof String val1 && obj2 instanceof String val2) {
				return val1.compareTo(val2);
			}
			return 0;
		}
	}
	
	protected Object getMapperCombinerReducer(Object statement) {
		Map<String, Long> columnindexmap = new ConcurrentHashMap<>();
		return mrab.setJobConf(jc)
				.setOutputfolder("/aircararrivaldelay").build();
	}

	/**
	 * Evaluates the expression of object array. 
	 * @param expression
	 * @param row
	 * @param columnsforeachjoin
	 * @return evaluates to true if expression succeeds and false if fails.
	 */
	public static boolean evaluateExpression(Expression expression, Map<String, Object> row) {
		if (expression instanceof BinaryExpression binaryExpression) {
			String operator = binaryExpression.getStringExpression();
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();

			switch (operator.toUpperCase()) {
				case "AND":
					return evaluateExpression(leftExpression, row)
							&& evaluateExpression(rightExpression, row);
				case "OR":
					return evaluateExpression(leftExpression, row)
							|| evaluateExpression(rightExpression, row);
				case ">":
					String leftValue = getValueString(leftExpression, row);
					String rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) > Double.valueOf(rightValue);
				case ">=":
					leftValue = getValueString(leftExpression, row);
					rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) >= Double.valueOf(rightValue);
				case "<":
					leftValue = getValueString(leftExpression, row);
					rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) < Double.valueOf(rightValue);
				case "<=":
					leftValue = getValueString(leftExpression, row);
					rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) <= Double.valueOf(rightValue);
				case "=":
					Object leftValueO = getValueStringWhere(leftExpression, row);
					Object rightValueO = getValueStringWhere(rightExpression, row);
					return compareToWhere(leftValueO, rightValueO) == 0;
				case "<>":
					Object leftValue0 = getValueStringWhere(leftExpression, row);
					Object rightValue0 = getValueStringWhere(rightExpression, row);
					return compareToWhere(leftValue0, rightValue0) != 0;
				default:
					throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			return evaluateExpression(subExpression, row);
		} else {
			String value = getValueString(expression, row);
			return Boolean.parseBoolean(value);
		}
	}

	/**
	 * Evaluates the join condition or expression and returns true or false.
	 * @param expression
	 * @param jointable
	 * @param row1
	 * @param row2
	 * @param leftablecolumns
	 * @param righttablecolumns
	 * @return true or false
	 */
	public static boolean evaluateExpressionJoin(Expression expression, String jointable, Map<String, Object> row1, Map<String, Object> row2) {
		if (expression instanceof BinaryExpression binaryExpression) {
			String operator = binaryExpression.getStringExpression();
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();
			Map<String, Object> rowleft = null;
			List<String> columnsrowleft = null;
			if (leftExpression instanceof Column column && column.getTable().getName().equals(jointable)) {
				rowleft = row2;
			} else {
				rowleft = row1;
			}
			Map<String, Object> rowright = null;
			List<String> columnsrowright = null;
			if (rightExpression instanceof Column column && column.getTable().getName().equals(jointable)) {
				rowright = row2;
			} else {
				rowright = row1;
			}
			switch (operator.toUpperCase()) {
				case "AND":
					return evaluateExpressionJoin(leftExpression, jointable, rowleft, rowright)
							&& evaluateExpressionJoin(rightExpression, jointable, rowleft, rowright);
				case "OR":
					return evaluateExpressionJoin(leftExpression, jointable, rowleft, rowright)
							|| evaluateExpressionJoin(rightExpression, jointable, rowleft, rowright);
				case ">":
					String leftValue = getValueString(leftExpression, rowleft);
					String rightValue = getValueString(rightExpression, rowright);
					return Double.valueOf(leftValue) > Double.valueOf(rightValue);
				case ">=":
					leftValue = getValueString(leftExpression, rowleft);
					rightValue = getValueString(rightExpression, rowright);
					return Double.valueOf(leftValue) >= Double.valueOf(rightValue);
				case "<":
					leftValue = getValueString(leftExpression, rowleft);
					rightValue = getValueString(rightExpression, rowright);
					return Double.valueOf(leftValue) < Double.valueOf(rightValue);
				case "<=":
					leftValue = getValueString(leftExpression, rowleft);
					rightValue = getValueString(rightExpression, rowright);
					return Double.valueOf(leftValue) <= Double.valueOf(rightValue);
				case "=":
					Object leftValueO = getValueString(leftExpression, rowleft);
					Object rightValueO = getValueString(rightExpression, rowright);
					return compareToWhere(leftValueO, rightValueO) == 0;
				case "<>":
					leftValue = getValueString(leftExpression, rowleft);
					rightValue = getValueString(rightExpression, rowright);
					return compareToWhere(leftValue, rightValue) != 0;
				default:
					throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			return evaluateExpressionJoin(subExpression, jointable, row1, row2);
		} else {
			Map<String, Object> row = null;
			List<String> columnsrow = null;
			if (expression instanceof Column column && column.getTable().getName().equals(jointable)) {
				row = row2;
			} else {
				row = row1;
			}
			String value = getValueString(expression, row);
			return Boolean.parseBoolean(value);
		}
	}

	/**
	 * Compare two objects to sort in order.
	 * @param obj1
	 * @param obj2
	 * @return value in Long for sorting.
	 */
	public static int compareToWhere(Object obj1, Object obj2) {
		if (obj1 instanceof Integer val1 && obj2 instanceof Integer val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Long val1 && obj2 instanceof Long val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Double val1 && obj2 instanceof Double val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Float val1 && obj2 instanceof Float val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof String val1 && obj2 instanceof String val2) {
			return val1.compareTo(val2);
		}
		return -1;
	}

	/**
	 * Gets the value of string given expression in column and records in map.
	 * @param expression
	 * @param row
	 * @return string value
	 */
	private static Object getValueStringWhere(Expression expression, Map<String, Object> row) {
		if (expression instanceof LongValue lv) {
			return Double.valueOf(lv.getValue());
		} else if (expression instanceof StringValue sv) {
			return sv.getValue();
		} else if (expression instanceof DoubleValue dv) {
			return Double.toString(dv.getValue());
		} else if (expression instanceof Column column) {
			String columnName = column.getColumnName();
			if (row.get(columnName) instanceof Double val) {
				return Double.valueOf(val);
			} else {
				return Double.valueOf((String) row.get(columnName));
			}
		} else {
			return Double.valueOf(0.0d);
		}
	}

	/**
	 * Gets the value of string given expression in column and records in map.
	 * @param expression
	 * @param row
	 * @return string value
	 */
	private static String getValueString(Expression expression, Map<String, Object> row) {
		if (expression instanceof LongValue lv) {
			return String.valueOf(lv.getValue());
		} else if (expression instanceof StringValue sv) {
			return sv.getValue();
		} else if (expression instanceof DoubleValue dv) {
			return Double.toString(dv.getValue());
		} else if (expression instanceof Column column) {
			String columnName = column.getColumnName();
			return String.valueOf(row.get(columnName));
		} else {
			return String.valueOf(0.0d);
		}
	}

	/**
	 * This function gets the column from single function.
	 * @param functions
	 * @return column referred in function like sum, min,max.
	 */
	public Column getColumn(Function function) {
		List<Expression> parameters = function.getParameters().getExpressions();
		return (Column) parameters.get(0);
	}

	/**
	 * This function evaluates the expression for a given row record.
	 * @param expression
	 * @param row
	 * @return evaluates to true if expression satisfies the condition else false
	 */
	public static boolean evaluateExpression(Expression expression, CSVRecord row) {
		if (expression instanceof BinaryExpression binaryExpression) {
			String operator = binaryExpression.getStringExpression();
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();

			switch (operator.toUpperCase()) {
				case "AND":
					return evaluateExpression(leftExpression, row) && evaluateExpression(rightExpression, row);
				case "OR":
					return evaluateExpression(leftExpression, row) || evaluateExpression(rightExpression, row);
				case ">":
					String leftValue = getValueString(leftExpression, row);
					String rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) > Double.valueOf(rightValue);
				case ">=":
					leftValue = getValueString(leftExpression, row);
					rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) >= Double.valueOf(rightValue);
				case "<":
					leftValue = getValueString(leftExpression, row);
					rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) < Double.valueOf(rightValue);
				case "<=":
					leftValue = getValueString(leftExpression, row);
					rightValue = getValueString(rightExpression, row);
					return Double.valueOf(leftValue) <= Double.valueOf(rightValue);
				case "=":
					Object leftValueO = getValueString(leftExpression, row);
					Object rightValueO = getValueString(rightExpression, row);
					return leftValueO.equals(rightValueO);
				case "<>":
					leftValue = getValueString(leftExpression, row);
					rightValue = getValueString(rightExpression, row);
					return !leftValue.equals(rightValue);
				default:
					throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			return evaluateExpression(subExpression, row);
		} else {
			String value = getValueString(expression, row);
			return Boolean.parseBoolean(value);
		}
	}

	/**
	 * Evaluates Expression and returns values
	 * @param expression
	 * @param row
	 * @return evaluated value
	 */
	public static Object evaluateBinaryExpression(Expression expression, CSVRecord row, List<SqlTypeName> tablecolumntypes, Map<String, Long> tablecolindexmap) {
		if (expression instanceof Function fn) {
			String name = fn.getName().toLowerCase();
			List<Expression> expfunc = fn.getParameters().getExpressions();
			switch (name) {
				case "abs":

					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "abs");
				case "length":
					// Get the length of string value	                
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "length");
				case "round":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "round");
				case "ceil":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "ceil");
				case "floor":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "floor");
				case "pow":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), evaluateBinaryExpression(expfunc.get(1), row, tablecolumntypes, tablecolindexmap), "pow");
				case "sqrt":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "sqrt");
				case "exp":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "exp");
				case "loge":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "loge");
				case "lowercase":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "lowercase");
				case "uppercase":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "uppercase");
				case "base64encode":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "base64encode");
				case "base64decode":
					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "base64decode");
				case "normalizespaces":

					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "normalizespaces");
				case "trim":

					return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap), null, "trim");
				case "substring":
					LongValue pos = (LongValue) expfunc.get(1);
					LongValue length = (LongValue) expfunc.get(2);
					String val = (String) evaluateBinaryExpression(expfunc.get(0), row, tablecolumntypes, tablecolindexmap);
					return val.substring((int) pos.getValue(), Math.min(((String) val).length(), (int) pos.getValue() + (int) length.getValue()));
			}
		} else if (expression instanceof BinaryExpression bex) {
			Expression leftExpression = bex.getLeftExpression();
			Expression rightExpression = bex.getRightExpression();
			String operator = bex.getStringExpression();
			Object leftValue = null;
			Object rightValue = null;
			if (leftExpression instanceof Function fn) {
				leftValue = evaluateBinaryExpression(leftExpression, row, tablecolumntypes, tablecolindexmap);
			} else if (leftExpression instanceof LongValue lv) {
				leftValue = lv.getValue();
			} else if (leftExpression instanceof DoubleValue dv) {
				leftValue = dv.getValue();
			} else if (leftExpression instanceof StringValue sv) {
				leftValue = sv.getValue();
			} else if (leftExpression instanceof Column column) {
				leftValue = SQLUtils.getValueMR(row.get(column.getColumnName()),
						tablecolumntypes.get(tablecolindexmap.get(column.getColumnName()).intValue()));
			} else if (leftExpression instanceof BinaryExpression) {
				leftValue = evaluateBinaryExpression(leftExpression, row, tablecolumntypes, tablecolindexmap);
			} else if (leftExpression instanceof Parenthesis parenthesis) {
				Expression subExpression = parenthesis.getExpression();
				leftValue = evaluateBinaryExpression(subExpression, row, tablecolumntypes, tablecolindexmap);
			}
			if (rightExpression instanceof Function fn) {
				rightValue = evaluateBinaryExpression(rightExpression, row, tablecolumntypes, tablecolindexmap);
			} else if (rightExpression instanceof LongValue lv) {
				rightValue = lv.getValue();
			} else if (rightExpression instanceof DoubleValue dv) {
				rightValue = dv.getValue();
			} else if (rightExpression instanceof StringValue sv) {
				rightValue = sv.getValue();
			} else if (rightExpression instanceof Column column) {
				rightValue = SQLUtils.getValueMR(row.get(column.getColumnName()),
						tablecolumntypes.get(tablecolindexmap.get(column.getColumnName()).intValue()));
			} else if (rightExpression instanceof BinaryExpression binaryExpression) {
				rightValue = evaluateBinaryExpression(binaryExpression, row, tablecolumntypes, tablecolindexmap);
			} else if (rightExpression instanceof Parenthesis parenthesis) {
				Expression subExpression = parenthesis.getExpression();
				rightValue = evaluateBinaryExpression(subExpression, row, tablecolumntypes, tablecolindexmap);
			}
			switch (operator) {
				case "+":
					return evaluateValuesByOperator(leftValue, rightValue, operator);
				case "-":
					return evaluateValuesByOperator(leftValue, rightValue, operator);
				case "*":
					return evaluateValuesByOperator(leftValue, rightValue, operator);
				case "/":
					return evaluateValuesByOperator(leftValue, rightValue, operator);
				default:
					throw new IllegalArgumentException("Invalid operator: " + operator);
			}
		} else if (expression instanceof LongValue lv) {
			return Double.valueOf(lv.getValue());
		} else if (expression instanceof DoubleValue dv) {
			return dv.getValue();
		} else if (expression instanceof StringValue sv) {
			return sv.getValue();
		} else if (expression instanceof Parenthesis parenthesis) {
			return evaluateBinaryExpression(parenthesis.getExpression(), row, tablecolumntypes, tablecolindexmap);
		} else if (expression instanceof Column column) {
			return SQLUtils.getValueMR(row.get(column.getColumnName()),
					tablecolumntypes.get(tablecolindexmap.get(column.getColumnName()).intValue()));
		}
		return Double.valueOf(0.0d);
	}

	/**
	 * Evaluates tuple using operator
	 * @param leftValue
	 * @param rightValue
	 * @param operator
	 * @return evaluated value
	 */
	public static Object evaluateValuesByOperator(Object leftValue, Object rightValue, String operator) {
		switch (operator) {
			case "+":
				if (leftValue instanceof String lv && rightValue instanceof Double rv) {
					return lv + rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv + rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv + rv;
				} else if (leftValue instanceof String lv && rightValue instanceof Long rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof String rv) {
					return lv + rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof String rv) {
					return lv + rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv + rv;
				} else if (leftValue instanceof String lv && rightValue instanceof String rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv + rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv + rv;
				}
			case "-":
				if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv - rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv - rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv - rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv - rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv - rv;
				}
			case "*":
				if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv * rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv * rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Integer rv) {
					return lv * rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Double rv) {
					return lv * rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv * rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv * rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv * rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv * rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv * rv;
				}
			case "/":
				if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv / rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv / rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Integer rv) {
					return lv / rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Double rv) {
					return lv / rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv / rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv / (double) rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv / (double) rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv / (double) rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv / (double) rv;
				}
			default:
				return 0;
		}

	}

	/**
	 * Evaluated functions by name and value
	 * @param value
	 * @param powerval
	 * @param name
	 * @return evaluated value
	 */
	public static Object evaluateFunctionsWithType(Object value, Object powerval, String name) {
		switch (name) {
			case "abs":

				if (value instanceof Double dv) {
					return Math.abs(dv);
				} else if (value instanceof Long lv) {
					return Math.abs(lv);
				} else if (value instanceof Float fv) {
					return Math.abs(fv);
				} else if (value instanceof Integer iv) {
					return Math.abs(iv);
				}
			case "length":
				// Get the length of string value
				String val = (String) value;
				// return the result to the stack
				return Long.valueOf(val.length());
			case "trim":
				// Get the length of string value
				val = (String) value;
				// return the result to the stack
				return val.trim();
			case "round":

				if (value instanceof Double dv) {
					return Math.round(dv);
				} else if (value instanceof Long lv) {
					return Math.round(lv);
				} else if (value instanceof Float fv) {
					return Math.round(fv);
				} else if (value instanceof Integer iv) {
					return Math.round(iv);
				}
			case "ceil":

				if (value instanceof Double dv) {
					return Math.ceil(dv);
				} else if (value instanceof Long lv) {
					return Math.ceil(lv);
				} else if (value instanceof Float fv) {
					return Math.ceil(fv);
				} else if (value instanceof Integer iv) {
					return Math.ceil(iv);
				}
			case "floor":

				if (value instanceof Double dv) {
					return Math.floor(dv);
				} else if (value instanceof Long lv) {
					return Math.floor(lv);
				} else if (value instanceof Float fv) {
					return Math.floor(fv);
				} else if (value instanceof Integer iv) {
					return Math.floor(iv);
				}
			case "pow":

				if (value instanceof Double dv && powerval instanceof Integer powval) {
					return Math.pow(dv, powval);
				} else if (value instanceof Long lv && powerval instanceof Integer powval) {
					return Math.pow(lv, powval);
				} else if (value instanceof Float fv && powerval instanceof Integer powval) {
					return Math.pow(fv, powval);
				} else if (value instanceof Integer iv && powerval instanceof Integer powval) {
					return Math.pow(iv, powval);
				}
			case "sqrt":

				if (value instanceof Double dv) {
					return Math.sqrt(dv);
				} else if (value instanceof Long lv) {
					return Math.sqrt(lv);
				} else if (value instanceof Float fv) {
					return Math.sqrt(fv);
				} else if (value instanceof Integer iv) {
					return Math.sqrt(iv);
				}
			case "exp":

				if (value instanceof Double dv) {
					return Math.exp(dv);
				} else if (value instanceof Long lv) {
					return Math.exp(lv);
				} else if (value instanceof Float fv) {
					return Math.exp(fv);
				} else if (value instanceof Integer iv) {
					return Math.exp(iv);
				}
			case "loge":

				if (value instanceof Double dv) {
					return Math.log(dv);
				} else if (value instanceof Long lv) {
					return Math.log(lv);
				} else if (value instanceof Float fv) {
					return Math.log(fv);
				} else if (value instanceof Integer iv) {
					return Math.log(iv);
				}
			case "lowercase":

				return ((String) value).toLowerCase();
			case "uppercase":

				return ((String) value).toUpperCase();
			case "base64encode":

				return Base64.getEncoder().encodeToString(((String) value).getBytes());
			case "base64decode":

				return new String(Base64.getDecoder().decode(((String) value).getBytes()));
			case "normalizespaces":

				return StringUtils.normalizeSpace((String) value);
		}
		return name;
	}

	/**
	 * Evaluates the expression with row to get value.
	 * @param expression
	 * @param row
	 * @return get the record column data from expression.
	 */
	private static String getValueString(Expression expression, CSVRecord row) {
		if (expression instanceof LongValue value) {
			return String.valueOf(value.getValue());
		} else if (expression instanceof StringValue value) {
			return value.getValue();
		} else if (expression instanceof DoubleValue value) {
			return Double.toString(value.getValue());
		} else {
			Column column = (Column) expression;
			String columnName = column.getColumnName();
			return String.valueOf(row.get(columnName));
		}
	}

	/**
	 * Gets the required columns from all the join tables 
	 * @param plainSelect
	 * @param tablerequiredcolumns
	 * @param expressionsTable
	 * @param joinTableExpressions
	 */
	public void getRequiredColumnsForAllTables(PlainSelect plainSelect, Map<String, Set<String>> tablerequiredcolumns,
			Map<String, List<Expression>> expressionsTable, Map<String, List<Expression>> joinTableExpressions) {

		List<Expression> expressions = new Vector<>();

		if (nonNull(plainSelect.getJoins())) {
			plainSelect.getJoins().parallelStream()
					.map((Serializable & java.util.function.Function<? super Join, ? extends Expression>) join -> join
							.getOnExpression())
					.forEach((Serializable & Consumer<Expression>) expression -> expressions.add(expression));
		}
		if (nonNull(plainSelect.getWhere())) {
			getColumnsFromBinaryExpression(plainSelect.getWhere(), tablerequiredcolumns, expressionsTable,
					expressionsTable);
		}
		for (Expression onExpression : expressions) {
			getColumnsFromBinaryExpression(onExpression, tablerequiredcolumns, joinTableExpressions,
					joinTableExpressions);
		}
	}

	/**
	 * Get All tables from expression 
	 * @param expression
	 * @param tablerequiredcolumns
	 */
	public void getColumnsFromBinaryExpression(Expression expression, Set<String> tablenames) {
		if (expression instanceof BinaryExpression binaryExpression) {
			Expression leftExpression = binaryExpression.getLeftExpression();
			if (leftExpression instanceof BinaryExpression) {
				getColumnsFromBinaryExpression(leftExpression, tablenames);
			} else if (leftExpression instanceof Column column) {
				tablenames.add(column.getTable().getName());
			} else if (leftExpression instanceof Function fn) {
				getColumnsFromBinaryExpression(fn, tablenames);
			}
			Expression rightExpression = binaryExpression.getRightExpression();
			if (rightExpression instanceof BinaryExpression) {
				getColumnsFromBinaryExpression(rightExpression, tablenames);
			} else if (rightExpression instanceof Column column) {
				tablenames.add(column.getTable().getName());
			} else if (rightExpression instanceof Function fn) {
				getColumnsFromBinaryExpression(fn, tablenames);
			}
		} else if (expression instanceof Column column) {
			tablenames.add(column.getTable().getName());
		} else if (expression instanceof Function function) {
			getColumnsFromBinaryExpression(function.getParameters().getExpressions().get(0), tablenames);
		}
	}

	/**
	 * Gets all the columns from the expressions.
	 * @param expression
	 * @param tablerequiredcolumns
	 * @param expressions
	 * @param joinTableExpressions
	 */
	public void getColumnsFromBinaryExpression(Expression expression, Map<String, Set<String>> tablerequiredcolumns,
			Map<String, List<Expression>> expressions, Map<String, List<Expression>> joinTableExpressions) {
		if (expression instanceof BinaryExpression binaryExpression) {
			Expression leftExpression = binaryExpression.getLeftExpression();
			if (leftExpression instanceof BinaryExpression) {
				getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions, joinTableExpressions);
			}
			Expression rightExpression = binaryExpression.getRightExpression();
			if (rightExpression instanceof BinaryExpression) {
				getColumnsFromBinaryExpression(rightExpression, tablerequiredcolumns, expressions,
						joinTableExpressions);
			}
			if (leftExpression instanceof Column column1 && !(rightExpression instanceof Column)) {
				if (nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if (!(leftExpression instanceof Column) && rightExpression instanceof Column column1) {
				if (nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if (leftExpression instanceof Column col1 && rightExpression instanceof Column col2) {
				List<Expression> expressionsTable = joinTableExpressions
						.get(col1.getTable().getName() + "-" + col2.getTable().getName());
				if (isNull(expressionsTable)) {
					expressionsTable = new Vector<>();
					joinTableExpressions.put(col1.getTable().getName() + "-" + col2.getTable().getName(),
							expressionsTable);
				}
				expressionsTable.add(binaryExpression);
			}
			// Check if either left or right expression is a column
			if (leftExpression instanceof Column column) {
				if (nonNull(column.getTable())) {
					Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
					if (isNull(columns)) {
						columns = new LinkedHashSet<>();
						tablerequiredcolumns.put(column.getTable().getName(), columns);
					}
					columns.add(column.getColumnName());
				}
			}

			if (rightExpression instanceof Column column) {
				if (nonNull(column.getTable())) {
					Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
					if (isNull(columns)) {
						columns = new LinkedHashSet<>();
						tablerequiredcolumns.put(column.getTable().getName(), columns);
					}
					columns.add(column.getColumnName());
				}
			}
		} else if (expression instanceof Column column) {
			Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
			if (isNull(columns)) {
				columns = new LinkedHashSet<>();
				tablerequiredcolumns.put(column.getTable().getName(), columns);
			}
			columns.add(column.getColumnName());
		}
	}

	/**
	 * Adds all the required columns from select items.
	 * @param allRequiredColumns
	 * @param allColumnsSelectItems
	 */
	public void addAllRequiredColumnsFromSelectItems(Map<String, Set<String>> allRequiredColumns,
			Map<String, Set<String>> allColumnsSelectItems) {
		allColumnsSelectItems.keySet().parallelStream().forEach((Serializable & Consumer<String>) (key -> {
			Set<String> allReqColumns = allRequiredColumns.get(key);
			Set<String> allSelectItem = allColumnsSelectItems.get(key);
			if (isNull(allReqColumns)) {
				allRequiredColumns.put(key, allSelectItem);
			} else {
				allReqColumns.addAll(allSelectItem);
			}
		}));
	}

	/**
	 * This functions returns initial filter or expressions.
	 * @param leftTableExpressions
	 * @return expression
	 */
	public Expression getFilterExpression(List<Expression> leftTableExpressions) {
		Expression expression = null;
		if (CollectionUtils.isNotEmpty(leftTableExpressions)) {
			expression = leftTableExpressions.get(0);
			for (int expressioncount = 1;expressioncount < leftTableExpressions.size();expressioncount++) {
				expression = new OrExpression(expression, leftTableExpressions.get(expressioncount));
			}
			return expression;
		}
		return null;
	}
}
