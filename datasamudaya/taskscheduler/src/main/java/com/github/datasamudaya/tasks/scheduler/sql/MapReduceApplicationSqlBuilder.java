package com.github.datasamudaya.tasks.scheduler.sql;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;

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
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;
import com.github.datasamudaya.tasks.executor.Reducer;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationBuilder;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
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
		Statement statement = parserManager.parse(new StringReader(sql));

		return getMapperCombinerReducer(statement);
	}

	protected Object getMapperCombinerReducer(Object statement) {
		if (!(statement instanceof Select)) {
			throw new IllegalArgumentException("Only SELECT statements are supported");
		}
		Select select = (Select) statement;
		String tablename = ((Table) ((PlainSelect) select.getSelectBody()).getFromItem()).getName();
		List<SelectItem> selectItems = ((PlainSelect) select.getSelectBody()).getSelectItems();
		Expression whereexp = ((PlainSelect) select.getSelectBody()).getWhere();
		List<String> functioncols = new ArrayList<>();
		List<FunctionWithCols> functionwithcolumns = new ArrayList<>();
		boolean isonlycolumns = false;
		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

		List<Function> functions = new ArrayList<>();
		Map<String, Set<String>> tablerequiredcolumns = new ConcurrentHashMap<>();
		Set<String> columnsselect = new LinkedHashSet<>();
		Set<String> selectcolumnsresult = new LinkedHashSet<>();
		boolean isdistinct = nonNull(plainSelect.getDistinct());
		boolean isaggfunc = false;
		Table table = (Table) plainSelect.getFromItem();
		if ("*".equals(plainSelect.getSelectItems().get(0).toString())) {
			tablerequiredcolumns.put(table.getName(), new LinkedHashSet<>(tablecolumnsmap.get(table.getName())));
			columnsselect.addAll(tablecolumnsmap.get(table.getName()));
			selectcolumnsresult.addAll(tablecolumnsmap.get(table.getName()));
			List<Join> joins = plainSelect.getJoins();
			if (CollectionUtils.isNotEmpty(joins)) {
				joins.parallelStream().forEach((Serializable & Consumer<Join>) (join -> {
					String jointablename = ((Table) join.getRightItem()).getName();
					tablerequiredcolumns.put(tablename, new LinkedHashSet<>(tablecolumnsmap.get(jointablename)));
					columnsselect.addAll(tablecolumnsmap.get(jointablename));
					selectcolumnsresult.addAll(tablecolumnsmap.get(jointablename));
				}));
			}
		} else {
			for (SelectItem selectItem : selectItems) {
				if (selectItem instanceof SelectExpressionItem selectExpressionItem) {
					if (selectExpressionItem.getExpression() instanceof Column column) {
						if (nonNull(column.getTable())) {
							Set<String> requiredcolumns = tablerequiredcolumns.get(column.getTable().getName());
							if (isNull(requiredcolumns)) {
								requiredcolumns = new LinkedHashSet<>();
								tablerequiredcolumns.put(column.getTable().getName(), requiredcolumns);
							}
							requiredcolumns.add(column.getColumnName());
							columnsselect.add(column.getColumnName());
							selectcolumnsresult.add(column.getColumnName());
						}
					} else if (selectExpressionItem.getExpression() instanceof Function function) {
						functions.add(function);
						String functionname = function.getName().toLowerCase();
						if (functionname.startsWith("count") || functionname.startsWith("sum")
								|| functionname.startsWith("min")
								|| functionname.startsWith("max")) {
							isaggfunc = true;
						}
						if (nonNull(function.getParameters())) {
							Expression exp = function.getParameters().getExpressions().get(0);
							if (exp instanceof Column column) {
								Set<String> requiredcolumns = tablerequiredcolumns.get(column.getTable().getName());
								if (isNull(requiredcolumns)) {
									requiredcolumns = new LinkedHashSet<>();
									tablerequiredcolumns.put(column.getTable().getName(), requiredcolumns);
								}
								requiredcolumns.add(column.getColumnName());
								columnsselect.add(column.getColumnName());
								var functionwithcolsobj = new FunctionWithCols();
								functionwithcolsobj.setName(function.getName().toLowerCase());
								var params = function.getParameters().getExpressions().stream()
										.map(col -> ((Column) col).getColumnName()).collect(Collectors.toList());
								functionwithcolsobj.setAlias(
										nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
												: function.toString());
								functionwithcolsobj.setTablename(column.getTable().getName());
								functionwithcolumns.add(functionwithcolsobj);
								functioncols.add(column.getColumnName());
								functionwithcolsobj.setFunction(function);
							} else {
								var functionwithcolsobj = new FunctionWithCols();
								functionwithcolsobj.setName(function.getName().toLowerCase());
								Set<String> tablenames = new LinkedHashSet<>();
								getColumnsFromBinaryExpression(function, tablenames);
								functionwithcolsobj.setTablename(tablenames.iterator().next());
								functionwithcolsobj.setAlias(
										nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
												: function.toString());
								functionwithcolsobj.setFunction(function);
								functionwithcolumns.add(functionwithcolsobj);
							}
							selectcolumnsresult.add(nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
									: function.toString());
						} else {
							var functionwithcolsobj = new FunctionWithCols();
							functionwithcolsobj.setName(function.getName().toLowerCase());
							functionwithcolumns.add(functionwithcolsobj);
							functionwithcolsobj.setAlias(
									nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
											: null);
							functionwithcolsobj.setFunction(function);
							selectcolumnsresult.add(nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
									: function.getName() + "()");
						}
					}
				}
			}
		}
		Map<String, List<Expression>> expressionsTable = new ConcurrentHashMap<>();
		Map<String, Set<String>> tablerequiredAllcolumns = new ConcurrentHashMap<>();
		Map<String, List<Expression>> joinTableExpressions = new ConcurrentHashMap<>();
		getRequiredColumnsForAllTables(plainSelect, tablerequiredAllcolumns, expressionsTable,
				joinTableExpressions);
		addAllRequiredColumnsFromSelectItems(tablerequiredAllcolumns, tablerequiredcolumns);
		Set<String> columnsRootTable = isNull(tablerequiredAllcolumns.get(table.getName())) ? new LinkedHashSet<>()
				: tablerequiredAllcolumns.get(table.getName());
		Expression expressionRootTable = getFilterExpression(expressionsTable.get(table.getName()));

		Set<String> selectcolumns = new LinkedHashSet<>(columnsRootTable);
		boolean isfunctions = !functionwithcolumns.isEmpty();
		for (String col :functioncols) {
			selectcolumns.remove(col);
			selectcolumnsresult.remove(col);
		}
		class MapperReducerSqlMapper implements Mapper<Long, String, Context> {
			private static final long serialVersionUID = 3328584603251312114L;
			ConcurrentMap<String, List<String>> tablecolumns = new ConcurrentHashMap<>(tablecolumnsmap);
			String maintablename;
			Set<String> selectcols;
			List<FunctionWithCols> functionwithcols;
			Expression where;
			List<SqlTypeName> tablecolumntypes;
			Map<String, Long> tablecolindexmap;
			boolean isaggfunc;

			public MapperReducerSqlMapper(String maintablename, Set<String> selectcols,
					List<FunctionWithCols> functionwithcols, Expression where,
					List<SqlTypeName> tablecolumntypes, Map<String, Long> tablecolindexmap,
					boolean isaggfunc) {
				this.maintablename = maintablename;
				this.selectcols = selectcols;
				this.functionwithcols = functionwithcols;
				this.where = where;
				this.tablecolumntypes = tablecolumntypes;
				this.tablecolindexmap = tablecolindexmap;
				this.isaggfunc = isaggfunc;
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
						if (isNull(where) || nonNull(where) && evaluateExpression(where, csvrecord)) {
							// Extract relevant columns
							Map<String, Object> valuemap = null;
							if (CollectionUtils.isNotEmpty(selectcols)) {
								valuemap = new HashMap<String, Object>();
								for (String column : selectcols) {
									// Output as key-value pair
									valuemap.put(column, SQLUtils.getValueMR(csvrecord.get(column),
											tablecolumntypes.get(tablecolindexmap.get(column).intValue())));
								}
							}
							if (!functionwithcols.isEmpty()) {
								var functionvaluesmap = new HashMap<>();
								for (FunctionWithCols functionwithcols :functionwithcols) {
									if (isaggfunc) {
										String funcname = functionwithcols.getName();
										String matchingtablename = functionwithcols.getTablename();
										boolean istablenamematches = nonNull(matchingtablename) ? matchingtablename.equalsIgnoreCase(maintablename) : false;
										if (istablenamematches) {
											Object evaluatevalue = evaluateBinaryExpression(functionwithcols.getFunction().getParameters().getExpressions().get(0), csvrecord, tablecolumntypes, tablecolindexmap);
											if (funcname.startsWith("sum")) {
												functionvaluesmap.put(nonNull(functionwithcols.getAlias()) ? functionwithcols.getAlias() : functionwithcols.getFunction().toString(), evaluatevalue);
												continue;
											}
											if (funcname.startsWith("max")) {
												functionvaluesmap.put(nonNull(functionwithcols.getAlias()) ? functionwithcols.getAlias() : functionwithcols.getFunction().toString(), evaluatevalue);
												continue;
											}
											if (funcname.startsWith("min")) {
												functionvaluesmap.put(nonNull(functionwithcols.getAlias()) ? functionwithcols.getAlias() : functionwithcols.getFunction().toString(), evaluatevalue);
												continue;
											}
										} else if (funcname.startsWith("count")) {
											functionvaluesmap.put(nonNull(functionwithcols.getAlias()) ? functionwithcols.getAlias() : "count()", 1.0d);
											continue;
										}
									} else {
										Object evaluatevalue = evaluateBinaryExpression(functionwithcols.getFunction(), csvrecord, tablecolumntypes, tablecolindexmap);
										functionvaluesmap.put(nonNull(functionwithcols.getAlias()) ? functionwithcols.getAlias() : functionwithcols.getFunction().toString(), evaluatevalue);
									}
								}
								if (nonNull(valuemap)) {
									if (isaggfunc) {
										context.put(maintablename, Tuple.tuple(valuemap, functionvaluesmap));
									} else {
										functionvaluesmap.putAll(valuemap);
										context.put(maintablename, Tuple.tuple(functionvaluesmap));
									}
								} else {
									context.put(maintablename, Tuple.tuple(functionvaluesmap));
								}
							} else {
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
		class MapperReducerSqlCombinerReducer implements Combiner<String, Tuple, Context>,
				Reducer<String, Tuple, Context> {
			private static final long serialVersionUID = 3328584603251312114L;
			List<FunctionWithCols> functionwithcols = functionwithcolumns;
			boolean isaggfunction;

			public MapperReducerSqlCombinerReducer(List<FunctionWithCols> functionwithcols, boolean isaggfunction) {
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
										valuesaggregate.put(fnkey, Math.min(valuesaggregate.get(fnkey), fnvalues.get(fnkey)));
									}
									if (fnkey.startsWith("max")) {
										valuesaggregate.put(fnkey, Math.max(valuesaggregate.get(fnkey), fnvalues.get(fnkey)));
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
					Map<Map<String, Object>, Map<String, Double>> valuesagg = tuplesgrpby.stream()
							.collect(Collectors.toMap(tuplekey -> tuplekey.v1,
									tuplevalue -> tuplevalue.v2, (values1, values2) -> {
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
					valuesagg.entrySet().stream()
							.map(entry -> Tuple.tuple(entry.getKey(), entry.getValue()))
							.forEach(value -> context.put("Reducer", Tuple.tuple(key, value)));
				}
			}
			PlainSelect ps = plainSelect;
			Set<String> finalselectcolumns = selectcolumnsresult;

			@Override
			public void reduce(String key, List<Tuple> values, Context context) {
				Map<String, List<Tuple>> mapListMap = new HashMap<>();

				for (Tuple tup : values) {
					String groupByValue = "";
					if (tup instanceof Tuple2 tup2) {
						groupByValue = (String) tup2.v1;
						List<Tuple> groupList = mapListMap.getOrDefault(groupByValue, new ArrayList<>());
						groupList.add((Tuple) tup2.v2);
						mapListMap.put(groupByValue, groupList);
					}
				}
				Map<String, List<Map<String, Object>>> processedmap = new HashMap<>();
				mapListMap.keySet().forEach(keytoproc -> {
					Context ctx = new DataCruncherContext<>();
					combine("Reducer", mapListMap.get(keytoproc), ctx);
					List<Tuple> tupletomerge = (List<Tuple>) ctx.get("Reducer");
					List<Map<String, Object>> objects = (List) tupletomerge.stream().map(t -> {
						if (t instanceof Tuple2 tuple2) {
							Object tup2val = tuple2.v2;
							if (tup2val instanceof Tuple2 value) {
								Map<String, Object> val1 = (Map<String, Object>) value.v1;
								Map<String, Object> val2 = (Map<String, Object>) value.v2;
								val1.putAll(val2);
								return val1;
							} else if (tup2val instanceof Tuple1 value1) {
								return (Map<String, Object>) value1.v1;
							}
						}
						return (Map<String, Object>) null;
					}).collect(Collectors.toList());
					processedmap.put(keytoproc, objects);
				});
				String tablename = ((Table) ps.getFromItem()).getName();
				List<Map<String, Object>> maintableoutput = new ArrayList<>();
				maintableoutput.addAll(processedmap.get(tablename));
				List<Map<String, Object>> mergedoutput = new ArrayList<>();
				if (nonNull(ps.getJoins())) {
					List<Join> joins = ps.getJoins();
					for (Join join : joins) {
						String jointable = ((Table) join.getRightItem()).getName();
						List<Map<String, Object>> joinoutput = processedmap.get(jointable);
						if (join.isInner()) {
							for (Map<String, Object> mainmap :maintableoutput) {
								for (Map<String, Object> joinmap :joinoutput) {
									if (evaluateExpressionJoin(join.getOnExpression(), jointable, mainmap, joinmap)) {
										Map<String, Object> mergedOut = new HashMap<>();
										mergedOut.putAll(mainmap);
										mergedOut.putAll(joinmap);
										mergedoutput.add(mergedOut);
									}
								}
							}
							maintableoutput = mergedoutput;
						} else if (join.isLeft()) {
							for (Map<String, Object> mainmap :maintableoutput) {
								for (Map<String, Object> joinmap :joinoutput) {
									if (evaluateExpressionJoin(join.getOnExpression(), jointable, mainmap, joinmap)) {
										Map<String, Object> mergedOut = new HashMap<>();
										mergedOut.putAll(mainmap);
										mergedOut.putAll(joinmap);
										mergedoutput.add(mergedOut);
									} else {
										Map<String, Object> mergedOut = new HashMap<>();
										mergedOut.putAll(mainmap);
										mergedOut.keySet().addAll(joinmap.keySet());
										mergedoutput.add(mergedOut);
									}
								}
							}
							maintableoutput = mergedoutput;
						} else if (join.isRight()) {
							for (Map<String, Object> mainmap :maintableoutput) {
								for (Map<String, Object> joinmap :joinoutput) {
									if (evaluateExpressionJoin(join.getOnExpression(), jointable, mainmap, joinmap)) {
										Map<String, Object> mergedOut = new HashMap<>();
										mergedOut.putAll(mainmap);
										mergedOut.putAll(joinmap);
										mergedoutput.add(mergedOut);
									} else {
										Map<String, Object> mergedOut = new HashMap<>();
										mergedOut.putAll(joinmap);
										mergedOut.keySet().addAll(mainmap.keySet());
										mergedoutput.add(mergedOut);
									}
								}
							}
							maintableoutput = mergedoutput;
						}
					}
				}
				if (nonNull(ps.getWhere())) {
					Expression where = ps.getWhere();
					maintableoutput = maintableoutput.stream().filter(val ->
							evaluateExpression(where, val)).collect(Collectors.toList());
				}
				maintableoutput = maintableoutput.stream().map(val -> {
					val.keySet().retainAll(finalselectcolumns);
					return val;
				}).collect(Collectors.toList());
				if (CollectionUtils.isNotEmpty(ps.getOrderByElements())) {
					var orderbyelements = ps.getOrderByElements();
					List<Column> columns = new ArrayList<>();
					List<String> directions = new ArrayList<>();
					if (orderbyelements != null) {
						for (OrderByElement orderByElement : orderbyelements) {
							orderByElement.getExpression().accept(new ExpressionVisitorAdapter() {
								@Override
								public void visit(Column column) {
									columns.add(column);
								}
							});

							String direction = orderByElement.isAsc() ? "ASC" : "DESC";
							directions.add(direction);
						}
					}
					log.info("Order by {} {}", columns, directions);
					Collections.sort(maintableoutput, (map1, map2) -> {

						for (int i = 0;i < columns.size();i++) {
							String columnName = columns.get(i).getColumnName();
							String sortOrder = directions.get(i);
							Object value1 = map1.get(columnName);
							Object value2 = map2.get(columnName);
							int result = compareTo(value1, value2);
							if ("DESC".equals(sortOrder)) {
								result = -result;
							}
							if (result != 0) {
								return result;
							}
						}
						return 0;
					});
				}
				context.addAll(key, maintableoutput);
				log.info("In Reduce Output Values {}", maintableoutput.size());
			}

			/**
			 * Compare two objects to sort in order.
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
		Map<String, Long> columnindexmap = new ConcurrentHashMap<>();
		List<String> columnsfortable = tablecolumnsmap.get(tablename);
		for (int originalcolumnindex = 0;originalcolumnindex < columnsfortable
				.size();originalcolumnindex++) {
			columnindexmap.put(columnsfortable.get(originalcolumnindex), Long.valueOf(originalcolumnindex));
		}
		MapReduceApplicationBuilder mrab = MapReduceApplicationBuilder.newBuilder()
				.addMapper(new MapperReducerSqlMapper(tablename, selectcolumns, functionwithcolumns, expressionRootTable,
						tablecolumntypesmap.get(tablename), columnindexmap, isaggfunc), tablefoldermap.get(tablename));
		if (CollectionUtils.isNotEmpty(plainSelect.getJoins())) {
			List<Join> joins = plainSelect.getJoins();
			for (Join join : joins) {
				Table rightItem = (Table) join.getRightItem();
				Expression expressionJoinTable = getFilterExpression(expressionsTable.get(rightItem.getName()));
				Set<String> columnsJoinTable = isNull(tablerequiredAllcolumns.get(table.getName())) ? new LinkedHashSet<>()
						: tablerequiredAllcolumns.get(rightItem.getName());
				selectcolumns = new LinkedHashSet<>(columnsJoinTable);
				for (String col :functioncols) {
					selectcolumns.remove(col);
				}
				columnindexmap = new ConcurrentHashMap<>();
				columnsfortable = tablecolumnsmap.get(rightItem.getName());
				for (int originalcolumnindex = 0;originalcolumnindex < columnsfortable
					.size();originalcolumnindex++) {
					columnindexmap.put(columnsfortable.get(originalcolumnindex), Long.valueOf(originalcolumnindex));
				}
				mrab = mrab.addMapper(new MapperReducerSqlMapper(rightItem.getName(), selectcolumns, functionwithcolumns, expressionJoinTable,
						tablecolumntypesmap.get(rightItem.getName()), columnindexmap,
						isaggfunc), tablefoldermap.get(rightItem.getName()));
			}
		}
		mrab = mrab.addCombiner(new MapperReducerSqlCombinerReducer(functionwithcolumns, isaggfunc))
				.addReducer(new MapperReducerSqlCombinerReducer(functionwithcolumns, isaggfunc));
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
