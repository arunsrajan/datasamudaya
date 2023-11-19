package com.github.datasamudaya.stream.sql.build;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.functions.CoalesceFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.StreamPipeline;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.google.common.collect.MapMaker;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.validation.Validation;
import net.sf.jsqlparser.util.validation.ValidationError;
import net.sf.jsqlparser.util.validation.feature.DatabaseType;

/**
 * The SQL builder class.
 * 
 * @author arun
 *
 */
public class StreamPipelineSqlBuilder implements Serializable {
	private static final long serialVersionUID = -8585345445522511086L;
	private static Logger log = LoggerFactory.getLogger(StreamPipelineSqlBuilder.class);
	String sql;
	String db;
	ConcurrentMap<String, String> tablefoldermap = new ConcurrentHashMap<>();
	ConcurrentMap<String, List<String>> tablecolumnsmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap = new ConcurrentHashMap<>();
	String hdfs;
	transient PipelineConfig pc;
	private Stack<List<String>> columnstack = new Stack<>();

	private StreamPipelineSqlBuilder() {

	}

	/**
	 * Creates a new sql builder object.
	 * 
	 * @return sql builder object.
	 */
	public static StreamPipelineSqlBuilder newBuilder() {
		return new StreamPipelineSqlBuilder();
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
	public StreamPipelineSqlBuilder add(String folder, String tablename, List<String> columns,
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
	public StreamPipelineSqlBuilder setHdfs(String hdfs) {
		this.hdfs = hdfs;
		return this;
	}

	/**
	 * Sets the pipelineconfig object to run sql using the configuration.
	 * 
	 * @param pc
	 * @return sql builder object
	 */
	public StreamPipelineSqlBuilder setPipelineConfig(PipelineConfig pc) {
		this.pc = pc;
		return this;
	}

	/**
	 * Sets the sql query.
	 * 
	 * @param sql
	 * @return sql builder object
	 */
	public StreamPipelineSqlBuilder setSql(String sql) {
		this.sql = sql;
		return this;
	}

	/**
	 * Sets the sql db
	 * 
	 * @param db
	 * @return sql builder object
	 */
	public StreamPipelineSqlBuilder setDb(String db) {
		this.db = db;
		return this;
	}

	/**
	 * The build method to create sql pipeline object.
	 * 
	 * @return SQL pipeline object
	 * @throws Exception
	 */
	public StreamPipelineSql build() throws Exception {
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		Validation validation = new Validation(
				Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB, DatabaseType.POSTGRESQL, DatabaseType.H2),
				sql);
		List<ValidationError> errors = validation.validate();
		if (!CollectionUtils.isEmpty(errors)) {
			log.error("Syntax error in SQL {}", errors);
			throw new Exception("Syntax error in SQL");
		}
		SQLUtils.validateSql(tablecolumnsmap, tablecolumntypesmap, sql, db);
		Statement statement = parserManager.parse(new StringReader(sql));
		return new StreamPipelineSql(execute(statement));
	}

	/**
	 * Execute the sql statement.
	 * 
	 * @param statement
	 * @return output of the sql execution
	 * @throws Exception
	 */
	protected Object execute(Object statement) throws Exception {
		if (!(statement instanceof Select || statement instanceof PlainSelect)) {
			throw new IllegalArgumentException("Only SELECT statements are supported");
		}
		StreamPipeline<Map<String, Object>> subselectpipeline = null;
		if ((statement instanceof Select select && select.getSelectBody() instanceof PlainSelect)
				|| statement instanceof PlainSelect) {
			var plainSelect = (PlainSelect) (statement instanceof PlainSelect ? statement
					: ((Select) statement).getSelectBody());

			FromItem fromItem = plainSelect.getFromItem();
			Table table = null;
			List<SelectItem> selectItems = plainSelect.getSelectItems();
			List<Function> aggfunctions = new ArrayList<>();
			List<Expression> nonaggfunctions = new ArrayList<>();
			Map<Function, String> functionalias = new HashMap<>();
			Map<Parenthesis, String> parenthesisalias = new HashMap<>();
			Map<String, Set<String>> tablerequiredcolumns = new ConcurrentHashMap<>();
			final List<String> columnsselect = new ArrayList<>();
			List<String> functioncols = new ArrayList<>();
			boolean isdistinct = nonNull(plainSelect.getDistinct());
			List<String> orderedselectcolumns = new ArrayList<>();
			if (fromItem instanceof SubSelect) {
				SelectBody subSelectBody = ((SubSelect) fromItem).getSelectBody();
				if (subSelectBody instanceof PlainSelect) {
					subselectpipeline = (StreamPipeline<Map<String, Object>>) execute((PlainSelect) subSelectBody);
				}
			} else if (fromItem instanceof Table) {
				table = (Table) plainSelect.getFromItem();
			}
			if (plainSelect.getSelectItems().get(0).toString().equals("*")) {
				if (nonNull(table)) {
					tablerequiredcolumns.put(table.getName(),
							new LinkedHashSet<>(tablecolumnsmap.get(table.getName())));
					columnsselect.addAll(tablecolumnsmap.get(table.getName()));
					orderedselectcolumns.addAll(tablecolumnsmap.get(table.getName()));
				} else {
					List<String> subselectcolumns = columnstack.pop();
					columnsselect.addAll(subselectcolumns);
					orderedselectcolumns.addAll(subselectcolumns);
				}
				List<Join> joins = plainSelect.getJoins();
				if (CollectionUtils.isNotEmpty(joins)) {
					joins.parallelStream().forEach((Serializable & Consumer<Join>) (join -> {
						String tablename = ((Table) join.getRightItem()).getName();
						tablerequiredcolumns.put(tablename, new LinkedHashSet<>(tablecolumnsmap.get(tablename)));
						columnsselect.addAll(tablecolumnsmap.get(tablename));
						orderedselectcolumns.addAll(tablecolumnsmap.get(tablename));
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
								orderedselectcolumns.add(nonNull(selectExpressionItem.getAlias())
										? selectExpressionItem.getAlias().getName()
										: column.getColumnName());

							} else {
								columnsselect.add(column.getColumnName());
								orderedselectcolumns.add(nonNull(selectExpressionItem.getAlias())
										? selectExpressionItem.getAlias().getName()
										: column.getColumnName());
							}
						} else if (selectExpressionItem.getExpression() instanceof Function function) {
							orderedselectcolumns.add(nonNull(selectExpressionItem.getAlias())?
									selectExpressionItem.getAlias().getName():selectExpressionItem.toString());
							
							functionalias.put(function,
									nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
											: null);
							String functionname = function.getName().toLowerCase();
							if (functionname.startsWith("avg") || functionname.startsWith("min")
									|| functionname.startsWith("max") || functionname.startsWith("sum")
									|| functionname.startsWith("count") || functionname.startsWith("grpconcat")
									|| functionname.startsWith("first_value")
									|| functionname.startsWith("last_value")) {
								aggfunctions.add(function);
							} else {
								nonaggfunctions.add(function);
							}
							SQLUtils.getColumnsFromSelectItemsFunctions(function, tablerequiredcolumns, columnsselect,
									functioncols);
						} else if (selectExpressionItem.getExpression() instanceof Parenthesis parenthesis) {
							orderedselectcolumns.add(
									nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
											: selectExpressionItem.toString());
							parenthesisalias.put(parenthesis,
									nonNull(selectExpressionItem.getAlias()) ? selectExpressionItem.getAlias().getName()
											: null);
							nonaggfunctions.add(parenthesis);
							SQLUtils.getColumnsFromSelectItemsFunctions(parenthesis, tablerequiredcolumns,
									columnsselect, functioncols);
						}
					}
				}
			}
			if(nonNull(table)) {
				if(!columnstack.isEmpty()) {
					columnstack.pop();
				}
				columnstack.push(orderedselectcolumns);
			}
			List<String> roottablecolumn = nonNull(table) ? tablecolumnsmap.get(table.getName()) : null;
			Map<String, List<Expression>> expressionsTable = new ConcurrentHashMap<>();
			Map<String, Set<String>> tablerequiredAllcolumns = new ConcurrentHashMap<>();
			Map<String, List<Expression>> joinTableExpressions = new ConcurrentHashMap<>();
			SQLUtils.getRequiredColumnsForAllTables(plainSelect, tablerequiredAllcolumns, expressionsTable,
					joinTableExpressions);
			SQLUtils.addAllRequiredColumnsFromSelectItems(tablerequiredAllcolumns, tablerequiredcolumns);
			Set<String> columnsRootTable = nonNull(table)
					? isNull(tablerequiredAllcolumns.get(table.getName())) ? new LinkedHashSet<>()
							: tablerequiredAllcolumns.get(table.getName())
					: new LinkedHashSet<>(columnsselect);
			Expression expressionRootTable = nonNull(table)?SQLUtils.getFilterExpression(expressionsTable.get(table.getName())):null;
			StreamPipeline<Map<String, Object>> pipeline = nonNull(table)
					? StreamPipeline.newCsvStreamHDFSSQL(hdfs, tablefoldermap.get(table.getName()), this.pc,
							roottablecolumn.toArray(new String[roottablecolumn.size()]),
							tablecolumntypesmap.get(table.getName()), new ArrayList<>(columnsRootTable))
					: subselectpipeline;
			if (nonNull(expressionRootTable)) {
				pipeline = pipeline.filter(new PredicateSerializable<Map<String, Object>>() {
					private static final long serialVersionUID = -9040664505941669357L;
					Expression exp = expressionRootTable;

					@Override
					public boolean test(Map<String, Object> record) {
						return SQLUtils.evaluateExpression(exp, record);
					}
				});
			}
			Map<String, Long> roottablecolumnindexmap = new ConcurrentHashMap<>();
			if(nonNull(roottablecolumn)) {
				for (int originalcolumnindex = 0; originalcolumnindex < roottablecolumn.size(); originalcolumnindex++) {
					roottablecolumnindexmap.put(roottablecolumn.get(originalcolumnindex),
							Long.valueOf(originalcolumnindex));
				}
			}
			Map<String, Map<String, Double>> jointablescolumnindexmap = new ConcurrentHashMap<>();
			var columnsforeachjoin = new ArrayList<String>();
			columnsforeachjoin.addAll(columnsRootTable);
			if (nonNull(plainSelect.getJoins())) {
				for (Join join : plainSelect.getJoins()) {
					String tablename = ((Table) join.getRightItem()).getName();
					List<String> jointablecolumn = tablecolumnsmap.get(tablename);
					Map<String, Double> jointablecolumnindexmap = new ConcurrentHashMap<>();
					for (int originalcolumnindex = 0; originalcolumnindex < jointablecolumn
							.size(); originalcolumnindex++) {
						jointablecolumnindexmap.put(jointablecolumn.get(originalcolumnindex),
								Double.valueOf(originalcolumnindex));
					}
					jointablescolumnindexmap.put(tablename, jointablecolumnindexmap);
					Set<String> columnsjointable = isNull(tablerequiredAllcolumns.get(tablename))
							? new LinkedHashSet<>()
							: tablerequiredAllcolumns.get(tablename);
					columnsforeachjoin.addAll(columnsjointable);
					StreamPipeline<Map<String, Object>> pipelineJoin = StreamPipeline.newCsvStreamHDFSSQL(hdfs,
							tablefoldermap.get(tablename), this.pc,
							jointablecolumn.toArray(new String[jointablecolumn.size()]),
							tablecolumntypesmap.get(tablename), new ArrayList<>(columnsjointable));
					Expression expressionJoinTable = SQLUtils.getFilterExpression(expressionsTable.get(tablename));
					if (nonNull(expressionJoinTable)) {
						pipelineJoin = pipelineJoin.filter(new PredicateSerializable<Map<String, Object>>() {
							private static final long serialVersionUID = -9040664505941669357L;
							Expression exp = expressionJoinTable;

							@Override
							public boolean test(Map<String, Object> valuemap) {
								return SQLUtils.evaluateExpression(exp, valuemap);
							}
						});
					}
					pipeline = buildJoinPredicate(pipeline, pipelineJoin, tablename,
							join.getOnExpressions().iterator().next(), join.isInner(), join.isLeft(), join.isRight(),
							columnsforeachjoin, new ArrayList<>(columnsjointable))
							.map(new MapFunction<Tuple2<Map<String, Object>, Map<String, Object>>, Map<String, Object>>() {
								private static final long serialVersionUID = -132962119666155193L;

								@Override
								public Map<String, Object> apply(
										Tuple2<Map<String, Object>, Map<String, Object>> tuple2) {
									Map<String, Object> columnvaluemap = new HashMap<>(tuple2.v1);
									columnvaluemap.putAll(tuple2.v2);
									return columnvaluemap;
								}
							});
				}
			}
			if (nonNull(plainSelect.getWhere())) {
				Expression expression = plainSelect.getWhere();
				pipeline = pipeline.filter(new PredicateSerializable<Map<String, Object>>() {
					private static final long serialVersionUID = -9040664505941669357L;
					Expression exp = expression;
					List<String> columnseachjoin = columnsforeachjoin;

					@Override
					public boolean test(Map<String, Object> recordmap) {
						return SQLUtils.evaluateExpression(exp, recordmap, columnseachjoin);
					}

				});
			}
			Set<String> colsel = new LinkedHashSet<>(columnsforeachjoin);
			log.info("Selected Columns With Function Columns Removed {}", colsel);
			StreamPipeline<Map<String, Object>> pipelinemap = pipeline
					.map(new MapFunction<Map<String, Object>, Map<String, Object>>() {
						private static final long serialVersionUID = 8102198486566760753L;
						Set<String> selcolumnsfunccolsremoved = colsel;

						@Override
						public Map<String, Object> apply(Map<String, Object> mapvalues) {							
							Map<String, Object> objectValuesMap = new HashMap<>();							
							selcolumnsfunccolsremoved.stream()
							.forEach(key -> objectValuesMap.put(key, mapvalues.get(key)));
							return objectValuesMap;

						}
					});

			for (String col : functioncols) {
				columnsselect.remove(col);
			}
			Boolean isaggfunc = CollectionUtils.isNotEmpty(aggfunctions);
			Set<String> groupby = new LinkedHashSet<>();
			if (CollectionUtils.isNotEmpty(nonaggfunctions)) {
				for (Expression expr : nonaggfunctions) {
					if (expr instanceof Function fn) {
						List<Expression> exps = SQLUtils.getExpressions(fn);
						String aliasfunction = functionalias.get(fn);
						if (nonNull(exps) && exps.size() >= 1) {
							String alias = SQLUtils.getAliasForFunction(fn, functionalias);
							columnsselect.add(alias);
							groupby.add(alias);
						} else if (fn.getName().toLowerCase().startsWith("currentisodate")) {
							String alias = nonNull(aliasfunction) ? aliasfunction : "currentisodate()";
							columnsselect.add(alias);
							groupby.add(alias);
						}
					} else if (expr instanceof Parenthesis parenthesis) {
						columnsselect.add(parenthesisalias.get(parenthesis));
					}
				}
				pipelinemap = pipelinemap.map(new MapFunction<Map<String, Object>, Map<String, Object>>() {
					private static final long serialVersionUID = 6329566708048046421L;
					List<Expression> nonagg = new ArrayList<>(nonaggfunctions);
					List<String> columns = new ArrayList<>(columnsselect);
					Map<Function, String> aliasfn = functionalias;
					Map<Parenthesis, String> parenthealias = parenthesisalias;
					boolean isaggfuncqueried = isaggfunc;
					SimpleDateFormat dateFormat = new SimpleDateFormat();

					@Override
					public Map<String, Object> apply(Map<String, Object> mapvalues) {
						Map<String, Object> nonaggfnvalues = new HashMap<>();
						for (Expression exp : nonagg) {
							if (exp instanceof Function fn) {
								List<Expression> exps = SQLUtils.getExpressions(fn);
								String aliasfunction = aliasfn.get(fn);
								if (nonNull(exps) && exps.size() >= 1) {
									String alias = SQLUtils.getAliasForFunction(fn, aliasfn);
									Object value = SQLUtils.evaluateBinaryExpression(fn, mapvalues);
									nonaggfnvalues.put(alias, value);
								} else if (fn.getName().toLowerCase().startsWith("currentisodate")) {
									String alias = nonNull(aliasfunction) ? aliasfunction : "currentisodate()";
									dateFormat.applyPattern("yyyy-MM-dd'T'HH:mm:ssZ");
									nonaggfnvalues.put(alias, dateFormat.format(new Date(System.currentTimeMillis())));
								}
							} else if (exp instanceof Parenthesis parenthesis) {
								String aliasparenthesis = parenthealias.get(parenthesis);
								String alias = nonNull(aliasparenthesis) ? aliasparenthesis : parenthesis.toString();
								nonaggfnvalues.put(alias, SQLUtils.evaluateBinaryExpression(parenthesis, mapvalues));
							}
						}
						if (!isaggfuncqueried) {
							mapvalues.keySet().retainAll(columns);
						}
						mapvalues.putAll(nonaggfnvalues);
						return mapvalues;

					}
				});
			}
			if (CollectionUtils.isNotEmpty(aggfunctions)) {
				if (nonNull(plainSelect.getGroupBy())) {
					GroupByElement gbe = plainSelect.getGroupBy();
					ExpressionList el = gbe.getGroupByExpressionList();
					List<Expression> expressions = el.getExpressions();
					for (Expression exp : expressions) {
						if (exp instanceof Column column) {
							groupby.add(column.getColumnName());
						} else {
							groupby.add(exp.toString());
						}
					}
				} else if (!groupby.isEmpty() && isNull(plainSelect.getGroupBy())) {
					throw new IllegalArgumentException("Provide proper GroupBy expressions or columns");
				}
				List<List<Expression>> exp = new ArrayList<>();
				Boolean isaverage = false, iscount = false;
				List<String> functionaverages = new ArrayList<>();
				List<Function> funcrealign = new ArrayList<>();
				Function countfn = null;
				for (Function fn : aggfunctions) {
					String functionname = fn.getName().toLowerCase();
					if (functionname.startsWith("count")) {
						iscount = true;
						countfn = fn;
					} else {
						columnsselect.add(SQLUtils.getAliasForFunction(fn, functionalias));
						funcrealign.add(fn);
						exp.add(SQLUtils.getExpressions(fn));
						if (functionname.startsWith("avg")) {
							functionaverages.add(SQLUtils.getAliasForFunction(fn, functionalias));
							isaverage = true;
						}
					}
				}
				if (iscount || isaverage && !iscount) {
					exp.add(Arrays.asList());
					if (iscount) {
						columnsselect.add(SQLUtils.getAliasForFunction(countfn, functionalias));
						funcrealign.add(countfn);
					} else {
						Function function = new Function();
						function.setName("count");
						function.setAllColumns(true);
						funcrealign.add(function);
					}
				}
				pipelinemap = pipelinemap.mapToPair(new MapToPairFunction<Map<String, Object>, Tuple2<Tuple, Tuple>>() {
					private static final long serialVersionUID = 8102198486566760753L;
					List<List<Expression>> expre = exp;
					Set<String> grpby = groupby;

					@Override
					public Tuple2<Tuple, Tuple> apply(Map<String, Object> mapvalues) {
						Object[] fnobj = new Object[expre.size()];
						Object[] grpbyobj = new Object[grpby.size()];

						int index = 0;
						for (Object grpobj : grpby) {
							grpbyobj[index] = mapvalues.get(grpobj);
							index++;
						}
						index = 0;
						for (List<Expression> expr : expre) {
							if (expr.isEmpty()) {
								fnobj[index] = 1l;
							} else {
								fnobj[index] = SQLUtils.evaluateBinaryExpression(expr.get(0), mapvalues);
							}
							index++;
						}

						return Tuple.tuple(SQLUtils.convertObjectToTuple(grpbyobj),
								SQLUtils.convertObjectToTuple(fnobj));

					}
				}).reduceByKey(new ReduceByKeyFunction<Tuple>() {
					private static final long serialVersionUID = -8773950223630733894L;
					List<Function> aggregatefunc = funcrealign;

					@Override
					public Tuple apply(Tuple tuple1, Tuple tuple2) {
						return SQLUtils.evaluateTuple(tuple1, tuple2, aggregatefunc);
					}

				}).coalesce(1, new CoalesceFunction<Tuple>() {
					private static final long serialVersionUID = -6496272568103409255L;
					List<Function> aggregatefunc = funcrealign;

					@Override
					public Tuple apply(Tuple tuple1, Tuple tuple2) {
						return SQLUtils.evaluateTuple(tuple1, tuple2, aggregatefunc);
					}

				}).map(new MapFunction<Tuple2<Tuple, Tuple>, Map<String, Object>>() {							
					private static final long serialVersionUID = 9098846821052824347L;
					Set<String> grpby = groupby;
					List<Function> aggregatefunc = funcrealign;
					Map<Function, String> funcalias = functionalias;
					List<String> fnaverages = functionaverages;
					List<String> orderedcolumns = orderedselectcolumns;
					@Override
					public Map<String, Object> apply(Tuple2<Tuple,Tuple> tuple2) {
						Map<String, Object> mapwithfinalvalues = new HashMap<>();
						SQLUtils.populateMapFromTuple(mapwithfinalvalues, tuple2.v1, new ArrayList<>(grpby));
						SQLUtils.populateMapFromFunctions(mapwithfinalvalues, tuple2.v2, aggregatefunc, funcalias);								
						if(!fnaverages.isEmpty()) {
							long count = SQLUtils.getCountFromTuple(tuple2.v2);
							for(String fnaverage:fnaverages) {
								mapwithfinalvalues.put(fnaverage, SQLUtils.evaluateValuesByOperator(mapwithfinalvalues.get(fnaverage),count,"/"));
							}
						}
						Map<String, Object> orderedvalues = new LinkedHashMap<>();
						for(String column:orderedcolumns) {
							orderedvalues.put(column, mapwithfinalvalues.get(column));
						}
						mapwithfinalvalues.clear();
						return orderedvalues;
					}
				});
			} else {
				pipelinemap = pipelinemap.map(new MapFunction<Map<String, Object>, Map<String, Object>>() {
					List<String> orderedcolumns = orderedselectcolumns;
					private static final long serialVersionUID = 9098846821052824347L;

					@Override
					public Map<String, Object> apply(Map<String, Object> mapwithfinalvalues) {
						Map<String, Object> orderedvalues = new LinkedHashMap<>();
						for (String column : orderedcolumns) {
							orderedvalues.put(column, mapwithfinalvalues.get(column));
						}
						return orderedvalues;
					}
				});
			}

			if (isdistinct) {
				pipelinemap = pipelinemap
						.mapToPair(new MapToPairFunction<Map<String, Object>, Tuple2<Map<String, Object>, Double>>() {
							private static final long serialVersionUID = -6412672309048067129L;

							@Override
							public Tuple2<Map<String, Object>, Double> apply(Map<String, Object> record) {

								return new Tuple2<Map<String, Object>, Double>(record, 0.0d);
							}

						}).reduceByKey(new ReduceByKeyFunction<Double>() {
							private static final long serialVersionUID = -2395505885613892042L;

							@Override
							public Double apply(Double t, Double u) {
								return t + u;
							}

						}).coalesce(1, new CoalesceFunction<Double>() {
							private static final long serialVersionUID = -2395505885613892042L;

							@Override
							public Double apply(Double t, Double u) {
								return t + u;
							}

						}).map(val -> val.v1).distinct();
			}

			pipelinemap = orderBy(pipelinemap, plainSelect);
			return pipelinemap;
		}
		return statement.toString();
	}

	/**
	 * Filter with Join builder.
	 * 
	 * @param pipelinefunction
	 * @param pipelinefunctionright
	 * @param colsel
	 * @return stream of maps.
	 * @throws Exception
	 */
	protected StreamPipeline<Map<String, Object>> buildJoin(StreamPipeline<Map<String, Object>> pipelinefunction,
			StreamPipeline<Map<String, Object>> pipelinefunctionright, Set<String> colsel) throws Exception {
		return pipelinefunction
				.join(pipelinefunctionright, new JoinPredicate<Map<String, Object>, Map<String, Object>>() {
					private static final long serialVersionUID = -1432723151946554217L;
					Set<String> columnsselect = colsel;

					public boolean test(Map<String, Object> rowleft, Map<String, Object> rowright) {
						for (String column : columnsselect) {
							if (!rowleft.get(column).equals(rowright.get(column))) {
								return false;
							}
						}
						return true;
					}
				}).map(new MapFunction<Tuple2<Map<String, Object>, Map<String, Object>>, Map<String, Object>>() {
					private static final long serialVersionUID = -132962119666155193L;

					@Override
					public Map<String, Object> apply(Tuple2<Map<String, Object>, Map<String, Object>> tuple2) {
						tuple2.v1.putAll(tuple2.v2);
						return tuple2.v1;
					}
				});
	}

	/**
	 * Join for left, right and inner.
	 * 
	 * @param pipeline1
	 * @param pipeline2
	 * @param jointable
	 * @param expression
	 * @param inner
	 * @param left
	 * @param right
	 * @param leftablecolumns
	 * @param righttablecolumns
	 * @return pipeline of Tuples of object array.
	 * @throws PipelineException
	 */
	public static StreamPipeline<Tuple2<Map<String, Object>, Map<String, Object>>> buildJoinPredicate(
			StreamPipeline<Map<String, Object>> pipeline1, StreamPipeline<Map<String, Object>> pipeline2,
			String jointable, Expression expression, Boolean inner, Boolean left, Boolean right,
			List<String> leftablecolumns, List<String> righttablecolumns) throws PipelineException {
		if (inner) {
			return pipeline1.join(pipeline2, new JoinPredicate<Map<String, Object>, Map<String, Object>>() {
				private static final long serialVersionUID = -1432723151946554217L;
				Expression exp = expression;
				String jointab = jointable;
				List<String> leftablecol = leftablecolumns;
				List<String> righttablecol = righttablecolumns;

				public boolean test(Map<String, Object> rowleft, Map<String, Object> rowright) {
					return SQLUtils.evaluateExpressionJoin(exp, jointab, rowleft, rowright, leftablecol, righttablecol);
				}
			});
		} else if (left) {
			return pipeline1.leftOuterjoin(pipeline2,
					new LeftOuterJoinPredicate<Map<String, Object>, Map<String, Object>>() {
						private static final long serialVersionUID = -9071237179844212655L;
						Expression exp = expression;
						String jointab = jointable;
						List<String> leftablecol = leftablecolumns;
						List<String> righttablecol = righttablecolumns;

						public boolean test(Map<String, Object> rowleft, Map<String, Object> rowright) {
							return SQLUtils.evaluateExpressionJoin(exp, jointab, rowleft, rowright, leftablecol,
									righttablecol);
						}
					});
		} else {
			return pipeline1.rightOuterjoin(pipeline2,
					new RightOuterJoinPredicate<Map<String, Object>, Map<String, Object>>() {
						private static final long serialVersionUID = 7097332223096552391L;
						Expression exp = expression;
						String jointab = jointable;
						List<String> leftablecol = leftablecolumns;
						List<String> righttablecol = righttablecolumns;

						public boolean test(Map<String, Object> rowleft, Map<String, Object> rowright) {
							return SQLUtils.evaluateExpressionJoin(exp, jointab, rowleft, rowright, leftablecol,
									righttablecol);
						}
					});
		}
	}

	/**
	 * Exceutes the order by in sql query
	 * 
	 * @param pipelinefunction
	 * @param plainSelect
	 * @return stream of maps
	 * @throws Exception
	 */
	public StreamPipeline<Map<String, Object>> orderBy(StreamPipeline<Map<String, Object>> pipelinefunction,
			PlainSelect plainSelect) throws Exception {
		if (CollectionUtils.isNotEmpty(plainSelect.getOrderByElements())) {
			var orderbyelements = plainSelect.getOrderByElements();
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
			pipelinefunction = pipelinefunction.sorted((map1, map2) -> {
				var columnslocal = columns;
				var directionslocal = directions;

				for (int i = 0; i < columnslocal.size(); i++) {
					String columnName = columnslocal.get(i).getColumnName();
					String sortOrder = directionslocal.get(i);
					Object value1 = map1.get(columnName);
					Object value2 = map2.get(columnName);
					int result = SQLUtils.compareTo(value1, value2);
					if (sortOrder.equals("DESC")) {
						result = -result;
					}
					if (result != 0) {
						return result;
					}
				}
				return 0;
			});
		}
		return pipelinefunction;
	}

}
