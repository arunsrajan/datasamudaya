package com.github.datasamudaya.tasks.scheduler.sql;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableAggregateBase;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableIntersect;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.adapter.enumerable.EnumerableSortedAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.stream.sql.RequiredColumnsExtractor;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;
import com.github.datasamudaya.tasks.executor.Reducer;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationBuilder;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

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
	 * 
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
		Validation validation = new Validation(
				Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB, DatabaseType.POSTGRESQL, DatabaseType.H2),
				sql);
		List<ValidationError> errors = validation.validate();
		if (!CollectionUtils.isEmpty(errors)) {
			log.error("Syntax error in SQL {}", errors);
			throw new Exception("Syntax error in SQL");
		}
		RelNode relnode = Utils.validateSql(tablecolumnsmap, tablecolumntypesmap, sql, db, isDistinct);
		log.info("Final Plan: {}",
				RelOptUtil.dumpPlan(sql, relnode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
		descendants.put(relnode, false);
		requiredcolumnindex = new RequiredColumnsExtractor().getRequiredColumnsByTable(relnode);
		log.info("Required Columns: {}", requiredcolumnindex);
		mrab = MapReduceApplicationBuilder.newBuilder();
		mrab.setJobConf(jc).setOutputfolder("/aircararrivaldelay");
		traverse(relnode, 0);
		formMetadataRelNodeMapReduce(relnode);
		return mrab.build();
	}

	private final Map<RelNode, Boolean> descendants = new ConcurrentHashMap<>();
	AtomicBoolean isDistinct = new AtomicBoolean(false);
	private Map<String, Set<String>> requiredcolumnindex;
	MapReduceApplicationBuilder mrab;
	List<String> functions = null;
	List<Integer> colindexes = null;
	int[] grpcolindexes = null;
	List<SqlTypeName> togeneratezerobytype = null;
	List<RexNode> columnsp = null;
	Map<Integer, Integer> origcolreqcolmap = null;
	boolean isprojectiondescendtablescan;
	boolean isagg = false;
	List<RelFieldCollation> rfc = null;
	Map<RelNode, RelNode> childnoderelnodeparentsmap = new ConcurrentHashMap<>();
	Map<RelNode, RelNode> parentchildmap = new ConcurrentHashMap<>();
	Map<RelNode, Map<String, Map<Integer, Integer>>> jointableorigcolreqcolmap = new ConcurrentHashMap<>();
	Map<RelNode, Integer> relnodetotalinputsizemap = new ConcurrentHashMap<>();

	protected void traverse(RelNode relNode, int depth) throws Exception {
		List<RelNode> inputs = relNode.getInputs();
		if (relNode instanceof EnumerableTableScan ets) {
			relnodetotalinputsizemap.put(relNode, ets.getTable().getRowType().getFieldCount());
		}
		if (CollectionUtils.isNotEmpty(inputs)) {
			for (RelNode child : inputs) {
				descendants.put(child, true);
				childnoderelnodeparentsmap.put(child, relNode);
				parentchildmap.put(relNode, child);
				traverse(child, depth + 1);
				relnodetotalinputsizemap.put(relNode,
						relnodetotalinputsizemap.getOrDefault(relNode, 0) + relnodetotalinputsizemap.get(child));
			}
		}
	}

	protected void formMetadataRelNodeMapReduce(RelNode relNode) {

		if (relNode instanceof EnumerableTableScan ets) {
			String table = ets.getTable().getQualifiedName().get(1);
			List<String> reqcolindex = nonNull(requiredcolumnindex.get(table))
					? new ArrayList<>(requiredcolumnindex.get(table))
					: new ArrayList<>();
			List<String> columns = tablecolumnsmap.get(table);
			Map<String, Long> tablecolindex = new ConcurrentHashMap<>();
			for (int colindex = 0; colindex < columns.size(); colindex++) {
				tablecolindex.put(columns.get(colindex), Long.valueOf(colindex));
			}
			if (CollectionUtils.isNotEmpty(reqcolindex)) {
				origcolreqcolmap = new ConcurrentHashMap<>();
				for (int colindex = 0; colindex < reqcolindex.size(); colindex++) {
					origcolreqcolmap.put(Integer.valueOf(reqcolindex.get(colindex)), colindex);
				}
			}
			RexNode filtercondition = null;
			RelNode relnodeparent = childnoderelnodeparentsmap.get(ets);
			if (nonNull(relnodeparent) && relnodeparent instanceof EnumerableFilter ef) {
				filtercondition = ef.getCondition();
			}
			List<RexNode> selectedcolumns = null;
			if (nonNull(relnodeparent) && relnodeparent instanceof EnumerableProject ep) {
				relnodeparent = childnoderelnodeparentsmap.get(ep);
				if (nonNull(relnodeparent) && relnodeparent instanceof EnumerableFilter ef) {
					filtercondition = ef.getCondition();
				}
				selectedcolumns = ep.getProjects().stream().collect(Collectors.toList());
			}
			mrab.addMapper(
					new MapperReducerSqlMapper(table, selectedcolumns, tablecolumntypesmap.get(table),
							functions, colindexes, grpcolindexes, filtercondition),
					tablefoldermap.get(table));
			mrab.addCombiner(new MapperReducerSqlCombiner(CollectionUtils.isNotEmpty(functions), functions));
			mrab.addReducer(new MapperReducerSqlReducer(togeneratezerobytype, columnsp,
					isprojectiondescendtablescan && !isagg || nonNull(rfc) ? origcolreqcolmap : null,
					CollectionUtils.isNotEmpty(functions), functions, rfc));

		} else if (relNode instanceof EnumerableFilter ef) {
			RelNode parentnode = childnoderelnodeparentsmap.get(ef);
			RelNode childnode = ef.getInput(0);
			if (parentnode instanceof EnumerableHashJoin ehj) {
				if (ehj.getLeft() == relNode && childnode instanceof EnumerableTableScan ets) {
					String table = ets.getTable().getQualifiedName().get(1);
					Map<String, Map<Integer, Integer>> tableorigcolreqcolmap = jointableorigcolreqcolmap.get(ehj);
					if (isNull(tableorigcolreqcolmap)) {
						tableorigcolreqcolmap = new ConcurrentHashMap<>();
						jointableorigcolreqcolmap.put(ehj, tableorigcolreqcolmap);
					}
					Set<String> reqcols = requiredcolumnindex.get(table);
					int index = 0;
					tableorigcolreqcolmap.put(table, formOrigColumnsReqColumnsMap(reqcols, index));
				} else if (ehj.getRight() == relNode && childnode instanceof EnumerableTableScan ets) {
					String table = ets.getTable().getQualifiedName().get(1);
					Map<String, Map<Integer, Integer>> tableorigcolreqcolmap = jointableorigcolreqcolmap.get(ehj);
					if (isNull(tableorigcolreqcolmap)) {
						tableorigcolreqcolmap = new ConcurrentHashMap<>();
						jointableorigcolreqcolmap.put(ehj, tableorigcolreqcolmap);
					}
					Set<String> reqcols = requiredcolumnindex.get(table);
					int index = relnodetotalinputsizemap.get(ehj.getLeft());
					tableorigcolreqcolmap.put(table, formOrigColumnsReqColumnsMap(reqcols, index));
				}
			}
		} else if (relNode instanceof EnumerableSort es) {
			rfc = es.getCollation().getFieldCollations();
		} else if (relNode instanceof EnumerableHashJoin ehj) {
			List<RelNode> relnodes = new ArrayList<>();
			relnodes.add(ehj.getLeft());
			relnodes.add(ehj.getRight());
			for (RelNode childnode : relnodes) {
				if (childnode == ehj.getLeft() && childnode instanceof EnumerableTableScan ets) {
					String table = ets.getTable().getQualifiedName().get(1);
					Map<String, Map<Integer, Integer>> tableorigcolreqcolmap = jointableorigcolreqcolmap.get(ehj);
					if (isNull(tableorigcolreqcolmap)) {
						tableorigcolreqcolmap = new ConcurrentHashMap<>();
						jointableorigcolreqcolmap.put(ehj, tableorigcolreqcolmap);
					}
					Set<String> reqcols = requiredcolumnindex.get(table);
					int index = 0;
					tableorigcolreqcolmap.put(table, formOrigColumnsReqColumnsMap(reqcols, index));
				} else if (childnode == ehj.getRight() && childnode instanceof EnumerableTableScan ets) {
					String table = ets.getTable().getQualifiedName().get(1);
					Map<String, Map<Integer, Integer>> tableorigcolreqcolmap = jointableorigcolreqcolmap.get(ehj);
					if (isNull(tableorigcolreqcolmap)) {
						tableorigcolreqcolmap = new ConcurrentHashMap<>();
						jointableorigcolreqcolmap.put(ehj, tableorigcolreqcolmap);
					}
					Set<String> reqcols = requiredcolumnindex.get(table);
					int index = relnodetotalinputsizemap.get(ehj.getLeft());
					tableorigcolreqcolmap.put(table, formOrigColumnsReqColumnsMap(reqcols, index));
				}
			}

		} else if (relNode instanceof EnumerableNestedLoopJoin enlj) {

		} else if (relNode instanceof EnumerableIntersect) {

		} else if (relNode instanceof EnumerableProject ep) {
			RelNode childnode = parentchildmap.get(ep);
			if(nonNull(childnode) && !(childnode instanceof EnumerableTableScan)) {
				togeneratezerobytype = ep.getProjects().stream().map(rexnode -> SQLUtils.findGreatestType(rexnode))
						.toList();
				columnsp = new ArrayList<>(ep.getProjects());
				if (CollectionUtils.isNotEmpty(ep.getInputs())) {
					RelNode node = ep.getInputs().get(0);
					if (node instanceof EnumerableTableScan etsnode || node instanceof EnumerableFilter ef) {
						isprojectiondescendtablescan = true;
					}
				}
			}
		} else if (relNode instanceof EnumerableAggregate || relNode instanceof EnumerableSortedAggregate) {
			isagg = true;
			if (isDistinct.get()) {
			}
			EnumerableAggregateBase grpby = (EnumerableAggregateBase) relNode;
			List<Pair<AggregateCall, String>> aggfunctions = grpby.getNamedAggCalls();
			functions = new ArrayList<>();
			colindexes = new ArrayList<>();
			grpcolindexes = SQLUtils.getGroupByColumnIndexes(grpby);
			for (Pair<AggregateCall, String> pair : aggfunctions) {
				functions.add(pair.getKey().getAggregation().getName().toLowerCase());
				if ("count".equalsIgnoreCase(pair.getKey().getAggregation().getName())) {
					colindexes.add(null);
				} else {
					colindexes.add(pair.getKey().getArgList().get(0));
				}
			}
		}
		List<RelNode> inputs = relNode.getInputs();
		if (CollectionUtils.isNotEmpty(inputs)) {
			for (RelNode child : inputs) {
				formMetadataRelNodeMapReduce(child);
			}
		}
	}

	/**
	 * The method returns orig col and req col indexes map
	 * 
	 * @param reqcols
	 * @param index
	 * @return map
	 */
	public Map<Integer, Integer> formOrigColumnsReqColumnsMap(Set<String> reqcols, int index) {
		Map<Integer, Integer> origcolreqcolmap = new ConcurrentHashMap<>();
		for (String reqcol : reqcols) {
			Integer colindex = Integer.parseInt(reqcol);
			origcolreqcolmap.put(index + colindex, colindex);
		}
		return origcolreqcolmap;
	}

	class MapperReducerSqlMapper implements Mapper<Long, String, Context> {
		private static final long serialVersionUID = 3328584603251312114L;
		ConcurrentMap<String, List<String>> tablecolumns = new ConcurrentHashMap<>(tablecolumnsmap);
		String maintablename;
		List<SqlTypeName> tablecolumntypes;
		List<String> functions;
		List<Integer> colindex;
		int[] grpcolindex;
		RexNode filtercondition;
		CsvParserSettings settings;
		CsvParser parser;
		Set<Integer> selcols = null;
		List<RexNode> selectedcolumns;
		Map<Integer, Integer> origcolprojcolumnmap = new ConcurrentHashMap<>();
		public MapperReducerSqlMapper(String maintablename, List<RexNode> selectedcolumns,
				List<SqlTypeName> tablecolumntypes, List<String> functions,
				List<Integer> colindex, int[] grpcolindex,
				RexNode filtercondition) {
			this.selectedcolumns = selectedcolumns;
			this.maintablename = maintablename;
			this.functions = functions;
			this.colindex = colindex;
			this.grpcolindex = grpcolindex;
			this.filtercondition = filtercondition;
			settings = new CsvParserSettings();
			settings.getFormat().setLineSeparator("\n");
			if (nonNull(selectedcolumns)) {
				selcols = (Set<Integer>) selectedcolumns.stream().flatMap(rexnode -> {
					List<Integer> colindexes = new ArrayList<>();
					evaluateRexNodeAsRexInputRef(rexnode, colindexes);
					return colindexes.stream();
				}).collect(Collectors.toCollection(LinkedHashSet::new));
				int projectcolindex = 0;
				for(Integer origcolumn:selcols) {
					origcolprojcolumnmap.put(origcolumn, projectcolindex++);
				}
				this.tablecolumntypes = selcols.stream().map(selcolindex -> tablecolumntypes.get(selcolindex))
						.collect(Collectors.toList());
			} else {
				selcols = IntStream.range(0,tablecolumns.get(maintablename).size()).boxed().collect(Collectors.toCollection(LinkedHashSet::new));
				int projectcolindex = 0;
				for(Integer origcolumn:selcols) {
					origcolprojcolumnmap.put(origcolumn, projectcolindex++);
				}
				this.tablecolumntypes = selcols.stream().map(selcolindex -> tablecolumntypes.get(selcolindex))
						.collect(Collectors.toList());
			}
			settings.selectIndexes(selcols.toArray(new Integer[0]));
			settings.setNullValue(DataSamudayaConstants.EMPTY);
			parser = new CsvParser(settings);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void map(Long indexrows, String line, Context context) {
			try {
				if (nonNull(line)) {
					String[] csvrecord = parser.parseLine(line);
					Object[] typedvalues = new Object[csvrecord.length];
					int recindex = 0;
					for(String colval: csvrecord) {
						typedvalues[recindex] = SQLUtils.getValueMR(colval, tablecolumntypes.get(recindex));
						recindex++;
					}
					Object[] valueobject = new Object[nonNull(selectedcolumns)?selectedcolumns.size():0];
					recindex = 0;
					if(nonNull(selectedcolumns)) {
						for (RexNode projcols : selectedcolumns) {								
							valueobject[recindex] = evaluateRexNode(projcols, typedvalues, origcolprojcolumnmap);
							recindex++;
						}
					} else {
						valueobject = typedvalues;
					}
					if (isNull(filtercondition)
							|| nonNull(filtercondition) && SQLUtils.evaluateExpression(filtercondition, valueobject)) {						
						if (CollectionUtils.isNotEmpty(functions)) {
							List<Object> fnobj = new ArrayList<>();
							Object[] grpbyobj = { DataSamudayaConstants.EMPTY };
							int index = 0;
							if (nonNull(grpcolindex) && grpcolindex.length > 0) {
								grpbyobj = new Object[grpcolindex.length];
								for (; index < grpcolindex.length; index++) {
									grpbyobj[index] = valueobject[grpcolindex[index]];
								}
							}
							index = 0;
							for (; index < functions.size(); index++) {
								String functionname = functions.get(index);
								if ("count".equalsIgnoreCase(functionname)) {
									fnobj.add(1l);
								} else {
									fnobj.add(valueobject[colindex.get(index)]);
									long cval = 0l;
									boolean toconsider = true;
									if (toconsider) {
										cval = 1l;
									}
									if (functionname.startsWith("avg")) {
										fnobj.add(cval);
									}
								}
							}
							context.put(maintablename, Tuple.tuple(SQLUtils.convertObjectToTuple(grpbyobj),
									SQLUtils.convertObjectToTuple(fnobj.toArray(new Object[0]))));
						} else {
							// Output as key-value pair
							context.put(maintablename, Tuple.tuple(valueobject));
						}
					}
				}
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		}

	}

	class MapperReducerSqlCombiner implements Combiner<String, Tuple, Context> {
		private static final long serialVersionUID = 3328584603251312114L;
		List<FunctionWithCols> functionwithcols = new ArrayList<>();
		boolean isaggfunction;
		final Boolean isnotempty;
		List<String> functions;

		public MapperReducerSqlCombiner(final Boolean isnotempty, List<String> functions) {
			this.isnotempty = isnotempty;
			this.functions = functions;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void combine(String key, List<Tuple> tuples, Context context) {
			boolean istuple2 = CollectionUtils.isNotEmpty(tuples)?tuples.get(0) instanceof Tuple2:false;
			if (istuple2) {
				if (isnotempty) {
					List<Tuple2> tuples2 = (List) tuples;
					Map<Object, Optional<Tuple>> tuplekeyvaluemap = tuples2.stream()
							.collect(Collectors.groupingBy(tuple2 -> tuple2.v1, Collectors.reducing((t1, t2) -> {
								return Tuple.tuple((Tuple) ((Tuple2) t1).v1, SQLUtils.evaluateTuple(functions,
										(Tuple) ((Tuple2) t1).v2, (Tuple) ((Tuple2) t2).v2));
							})));
					tuplekeyvaluemap.entrySet().stream()
							.forEach(entry -> context.put(entry.getKey(), entry.getValue().get()));
					log.info("Combiner Result = {}", tuplekeyvaluemap);
				}
			} else {
				for (Tuple tuple1 : tuples) {
					context.put(Tuple.tuple(key), tuple1);
				}
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

	class MapperReducerSqlReducer implements Reducer<Tuple, Tuple, Context> {
		List<SqlTypeName> sqltypes;
		List<RexNode> columns;
		Map<Integer, Integer> origcolreqcolmap;
		Boolean isnotempty;
		List<String> functions;
		List<RelFieldCollation> rfc;

		public MapperReducerSqlReducer(final List<SqlTypeName> sqltypes, final List<RexNode> columns,
				final Map<Integer, Integer> origcolreqcolmap, final Boolean isnotempty, List<String> functions,
				List<RelFieldCollation> rfc) {
			this.sqltypes = sqltypes;
			this.columns = columns;
			this.origcolreqcolmap = origcolreqcolmap;
			this.isnotempty = isnotempty;
			this.functions = functions;
			this.rfc = rfc;
		}

		@Override
		public void reduce(Tuple key, List<Tuple> tuples, Context context) {
			boolean istuple2 = tuples.get(0) instanceof Tuple2;
			if (istuple2) {
				if (isnotempty) {
					List<Tuple2> tuples2 = (List) tuples;
					log.info("In Reduce Tuples = {}", tuples2);
					Map<Object, Optional<Tuple>> tuplekeyvaluemap = tuples2.stream()
							.collect(Collectors.groupingBy(tuple2 -> tuple2.v1,
									Collectors.reducing(
											(t1, t2) -> Tuple.tuple(((Tuple2) t1).v1, SQLUtils.evaluateTuple(functions,
													(Tuple) ((Tuple2) t1).v2, (Tuple) ((Tuple2) t2).v2)))));
					List<Object[]> finaloutputs = null;
					if (nonNull(rfc)) {
						finaloutputs = new ArrayList<>();
					}
					final List<Object[]> finaloutputsobjects = finaloutputs;
					tuplekeyvaluemap.entrySet().stream().forEach(entry -> {
						Object[] valuesgrpby = SQLUtils.populateObjectFromTuple((Tuple) entry.getKey());
						Tuple2<Tuple, Tuple> grpbyfunctions = (Tuple2<Tuple, Tuple>) entry.getValue().get();
						log.info("In Reduce Group By Key = {}", Arrays.toString(valuesgrpby));
						log.info("In Reduce {} v2={}", grpbyfunctions, grpbyfunctions.v2);
						Object[] valuesfromfunctions = SQLUtils.populateObjectFromFunctions(grpbyfunctions.v2,
								functions);
						Object[] mergeobject = null;
						if (nonNull(valuesgrpby)) {
							mergeobject = new Object[valuesgrpby.length + valuesfromfunctions.length];
						} else {
							mergeobject = new Object[valuesfromfunctions.length];
						}
						int valuecount = 0;
						if (nonNull(valuesgrpby)) {
							for (Object value : valuesgrpby) {
								mergeobject[valuecount] = value;
								valuecount++;
							}
						}
						for (Object value : valuesfromfunctions) {
							mergeobject[valuecount] = value;
							valuecount++;
						}
						if (nonNull(origcolreqcolmap) && CollectionUtils.isNotEmpty(columns)) {
							Object[] finaloutput = new Object[columns.size()];
							for (int valueindex = 0; valueindex < columns.size(); valueindex++) {
								RexNode cols = columns.get(valueindex);
								finaloutput[valueindex] = evaluateRexNode(cols, (Object[]) mergeobject,
										origcolreqcolmap);
							}
							if (isNull(rfc)) {
								context.put("reducer", finaloutput);
							} else {
								finaloutputsobjects.add(finaloutput);
							}
						} else {
							if (isNull(rfc)) {
								context.put("reducer", mergeobject);
							} else {
								finaloutputsobjects.add(mergeobject);
							}
						}

					});
					if (nonNull(rfc)) {
						Collections.sort(finaloutputs, new ObjectArrayComparator(rfc));
						finaloutputs.stream().forEach(valuearray -> context.put("reducer", valuearray));
					}
				}
			} else {
				List<Object[]> finaloutputs = null;
				if (nonNull(rfc)) {
					finaloutputs = new ArrayList<>();
				}
				for (Tuple tuple : tuples) {
					if (tuple instanceof Tuple1 maptup) {
						Object[] finaloutput = null;
						if (nonNull(columns)) {
							finaloutput = new Object[columns.size()];
							for (int valueindex = 0; valueindex < columns.size(); valueindex++) {
								RexNode cols = columns.get(valueindex);
								finaloutput[valueindex] = evaluateRexNode(cols, (Object[]) maptup.v1, null);
							}
						} else {
							finaloutput = (Object[]) maptup.v1;
						}
						if (isNull(rfc)) {
							context.put("reducer", finaloutput);
						} else {
							finaloutputs.add(finaloutput);
						}
					}
				}
				if (nonNull(rfc)) {
					Collections.sort(finaloutputs, new ObjectArrayComparator(rfc));
					finaloutputs.stream().forEach(valuearray -> context.put("reducer", valuearray));
				}
			}

			log.info("In Reduce Output Values {}", context.keys().size());
		}
	}

	protected Object getMapperCombinerReducer(Object statement) {
		Map<String, Long> columnindexmap = new ConcurrentHashMap<>();
		return mrab.setJobConf(jc).setOutputfolder("/aircararrivaldelay").build();
	}

	static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	static SimpleDateFormat dateExtract = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat time = new SimpleDateFormat("HH:mm:ss");

	/**
	 * Evaluates function RexNode with given values
	 * 
	 * @param node
	 * @param values
	 * @return value processed
	 */
	public static Object evaluateRexNode(RexNode node, Object[] values, Map<Integer, Integer> origcolreqcolmap) {
		if (node.isA(SqlKind.FUNCTION) || node.isA(SqlKind.CASE)) {
			RexCall call = (RexCall) node;
			RexNode expfunc = call.getOperands().size() > 0 ? call.getOperands().get(0) : null;
			String name = call.getOperator().getName().toLowerCase();
			switch (name) {
			case "abs":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"abs");
			case "length", "char_length", "character_length":
				// Get the length of string value
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"length");
			case "round":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"round");
			case "ceil":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"ceil");
			case "floor":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"floor");
			case "power":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap),
						evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap), "pow");
			case "pow":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap),
						evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap), "pow");
			case "sqrt":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"sqrt");
			case "exp":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"exp");
			case "loge":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"loge");
			case "lowercase", "lower", "lcase":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"lowercase");
			case "uppercase", "upper", "ucase":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"uppercase");
			case "base64encode":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"base64encode");
			case "base64decode":
				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"base64decode");
			case "normalizespaces":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"normalizespaces");
			case "currentisodate":

				return SQLUtils.evaluateFunctionsWithType(null, null, "currentisodate");
			case "current_timemillis":

				return SQLUtils.evaluateFunctionsWithType(null, null, "currenttimemillis");
			case "rand":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"rand");
			case "rand_integer":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap),
						evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap), "randinteger");
			case "acos":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"acos");
			case "asin":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"asin");
			case "atan":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"atan");
			case "cos":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"cos");
			case "sin":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"sin");
			case "tan":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"tan");
			case "cosec":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"cosec");
			case "sec":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"sec");
			case "cot":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"cot");
			case "cbrt":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"cbrt");
			case "pii":

				return SQLUtils.evaluateFunctionsWithType(null, null, "pii");
			case "degrees":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"degrees");
			case "radians":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"radians");
			case "trimstr":

				return SQLUtils.evaluateFunctionsWithType(evaluateRexNode(expfunc, values, origcolreqcolmap), null,
						"trim");
			case "substring":
				RexLiteral pos = (RexLiteral) call.getOperands().get(1);
				RexLiteral length = (RexLiteral) (call.getOperands().size() > 2 ? call.getOperands().get(2) : null);
				String val = (String) evaluateRexNode(expfunc, values, origcolreqcolmap);
				if (nonNull(length)) {
					return val.substring((Integer) SQLUtils.getValue(pos, SqlTypeName.INTEGER),
							Math.min(((String) val).length(), (Integer) SQLUtils.getValue(pos, SqlTypeName.INTEGER)
									+ (Integer) SQLUtils.getValue(length, SqlTypeName.INTEGER)));
				}
				return val.substring((Integer) SQLUtils.getValue(pos, SqlTypeName.INTEGER));
			case "overlay":
				pos = (RexLiteral) call.getOperands().get(2);
				length = (RexLiteral) (call.getOperands().size() > 3 ? call.getOperands().get(3) : null);
				String val1 = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				String val2 = (String) evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap);
				if (nonNull(length)) {
					return val1.replaceAll(val1.substring((Integer) SQLUtils.getValue(pos, SqlTypeName.INTEGER),
							Math.min(((String) val2).length(), (Integer) SQLUtils.getValue(pos, SqlTypeName.INTEGER)
									+ (Integer) SQLUtils.getValue(length, SqlTypeName.INTEGER))),
							val2);
				}
				return val1.replaceAll(val1.substring((Integer) SQLUtils.getValue(pos, SqlTypeName.INTEGER)), val2);
			case "locate":
				val1 = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				val2 = (String) evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap);
				pos = (RexLiteral) (call.getOperands().size() > 2 ? call.getOperands().get(2) : null);
				if (nonNull(pos)) {
					int positiontosearch = Math.min((Integer) SQLUtils.getValue(pos, SqlTypeName.INTEGER),
							val2.length());
					return positiontosearch + val2.substring(positiontosearch).indexOf(val1);
				}
				return val2.indexOf(val1);
			case "cast":
				return evaluateRexNode(expfunc, values, origcolreqcolmap);
			case "group_concat":
				RexNode rexnode1 = call.getOperands().get(0);
				RexNode rexnode2 = call.getOperands().get(1);
				return (String) evaluateRexNode(rexnode1, values, origcolreqcolmap)
						+ evaluateRexNode(rexnode2, values, origcolreqcolmap);
			case "concat":
				rexnode1 = call.getOperands().get(0);
				rexnode2 = call.getOperands().get(1);
				return (String) evaluateRexNode(rexnode1, values, origcolreqcolmap)
						+ evaluateRexNode(rexnode2, values, origcolreqcolmap);
			case "position":
				rexnode1 = call.getOperands().get(0);
				rexnode2 = call.getOperands().get(1);
				RexNode rexnode3 = call.getOperands().size() > 2 ? call.getOperands().get(2) : null;
				if (nonNull(rexnode3)) {
					return ((String) evaluateRexNode(rexnode2, values, origcolreqcolmap)).indexOf(
							(String) evaluateRexNode(rexnode1, values, origcolreqcolmap),
							(Integer) evaluateRexNode(rexnode3, values, origcolreqcolmap));
				}
				return ((String) evaluateRexNode(rexnode2, values, origcolreqcolmap))
						.indexOf((String) evaluateRexNode(rexnode1, values, origcolreqcolmap));
			case "initcap":
				val = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				return val.length() > 1 ? StringUtils.upperCase("" + val.charAt(0)) + val.substring(1)
						: val.length() == 1 ? StringUtils.upperCase("" + val.charAt(0)) : val;
			case "ascii":
				val = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				return (int) val.charAt(0);
			case "charac":
				Integer asciicode = Integer
						.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap)));
				return (char) (asciicode % 256);
			case "insertstr":
				String value1 = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				Integer postoinsert = Integer
						.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(2), values, origcolreqcolmap)));
				Integer lengthtoinsert = Integer
						.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(3), values, origcolreqcolmap)));
				String value2 = (String) evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap);
				value2 = value2.substring(0, Math.min(lengthtoinsert, value2.length()));
				return value1.substring(0, Math.min(value1.length(), postoinsert)) + value2
						+ value1.substring(Math.min(value1.length(), postoinsert), value1.length());
			case "leftchars":
				value1 = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				Integer lengthtoextract = Integer
						.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap)));
				return value1.substring(0, Math.min(lengthtoextract, value1.length()));
			case "rightchars":
				value1 = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				lengthtoextract = Integer
						.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(1), values, origcolreqcolmap)));
				return value1.substring(value1.length() - Math.min(lengthtoextract, value1.length()));
			case "reverse":
				value1 = (String) evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap);
				return StringUtils.reverse(value1);
			case "trim":
				rexnode1 = call.getOperands().get(0);
				rexnode2 = call.getOperands().get(1);
				rexnode3 = call.getOperands().get(2);
				String leadtrailboth = (String) evaluateRexNode(rexnode1, values, origcolreqcolmap);
				String str1 = (String) evaluateRexNode(rexnode2, values, origcolreqcolmap);
				String str2 = (String) evaluateRexNode(rexnode3, values, origcolreqcolmap);
				if (leadtrailboth.equalsIgnoreCase("leading")) {
					while (str2.startsWith(str1)) {
						str2 = str2.substring(str1.length());
					}
					return str2;
				}
				if (leadtrailboth.equalsIgnoreCase("trailing")) {
					while (str2.endsWith(str1)) {
						str2 = str2.substring(0, str2.length() - str1.length());
					}
					return str2;
				}
				while (str2.startsWith(str1)) {
					str2 = str2.substring(str1.length());
				}
				while (str2.endsWith(str1)) {
					str2 = str2.substring(0, str2.length() - str1.length());
				}
				return str2;

			case "ltrim":
				rexnode1 = call.getOperands().get(0);
				str1 = (String) evaluateRexNode(rexnode1, values, origcolreqcolmap);
				return StringUtils.stripStart(str1, null);
			case "rtrim":
				rexnode1 = call.getOperands().get(0);
				str1 = (String) evaluateRexNode(rexnode1, values, origcolreqcolmap);
				return StringUtils.stripEnd(str1, null);
			case "curdate":
				return date.format(new Date(System.currentTimeMillis()));
			case "curtime":
				return time.format(new Date(System.currentTimeMillis()));
			case "now":
				return dateExtract.format(new Date(System.currentTimeMillis()));
			case "year":
				rexnode1 = call.getOperands().get(0);
				str1 = (String) evaluateRexNode(rexnode1, values, origcolreqcolmap);
				java.util.Calendar calendar = new java.util.GregorianCalendar();
				try {
					calendar.setTime(dateExtract.parse(str1));
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
				return calendar.get(Calendar.YEAR);
			case "month":
				rexnode1 = call.getOperands().get(0);
				str1 = (String) evaluateRexNode(rexnode1, values, origcolreqcolmap);
				calendar = new java.util.GregorianCalendar();
				try {
					calendar.setTime(dateExtract.parse(str1));
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
				return calendar.get(Calendar.MONTH);
			case "day":
				rexnode1 = call.getOperands().get(0);
				str1 = (String) evaluateRexNode(rexnode1, values, origcolreqcolmap);
				calendar = new java.util.GregorianCalendar();
				try {
					calendar.setTime(dateExtract.parse(str1));
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
				return calendar.get(Calendar.MONTH);
			case "case":
				List<RexNode> rexnodes = call.getOperands();
				for (int numnodes = 0; numnodes < rexnodes.size(); numnodes += 2) {
					if (SQLUtils.evaluateExpression(rexnodes.get(numnodes), values)) {
						return evaluateRexNode(rexnodes.get(numnodes + 1), values, origcolreqcolmap);
					} else if (numnodes + 2 == rexnodes.size() - 1) {
						return evaluateRexNode(rexnodes.get(numnodes + 2), values, origcolreqcolmap);
					}
				}
				return "";
			}
		} else if (node instanceof RexCall call && call.getOperator() instanceof SqlFloorFunction) {
			return SQLUtils.evaluateFunctionsWithType(
					evaluateRexNode(call.getOperands().get(0), values, origcolreqcolmap), null,
					call.getOperator().getName().toLowerCase());
		} else if (node instanceof RexLiteral) {
			return SQLUtils.getValue((RexLiteral) node, ((RexLiteral) node).getType().getSqlTypeName());
		} else {
			return getValueObject(node, values, origcolreqcolmap);
		}
		return null;
	}

	/**
	 * Gets value Object from RexNode and values
	 * 
	 * @param node
	 * @param values
	 * @return returns the values of RexNode
	 */
	public static Object getValueObject(RexNode node, Object[] values, Map<Integer, Integer> origcolreqcolmap) {
		if (node.isA(SqlKind.PLUS)) {
			// For addition nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return SQLUtils.evaluateValuesByOperator(getValueObject(call.operands.get(0), values, origcolreqcolmap),
					getValueObject(call.operands.get(1), values, origcolreqcolmap), "+");
		} else if (node.isA(SqlKind.MINUS)) {
			// For subtraction nodes, recursively evaluate left and right
			// children
			RexCall call = (RexCall) node;
			return SQLUtils.evaluateValuesByOperator(getValueObject(call.operands.get(0), values, origcolreqcolmap),
					getValueObject(call.operands.get(1), values, origcolreqcolmap), "-");
		} else if (node.isA(SqlKind.TIMES)) {
			// For multiplication nodes, recursively evaluate left and right
			// children
			RexCall call = (RexCall) node;
			return SQLUtils.evaluateValuesByOperator(getValueObject(call.operands.get(0), values, origcolreqcolmap),
					getValueObject(call.operands.get(1), values, origcolreqcolmap), "*");
		} else if (node.isA(SqlKind.DIVIDE)) {
			// For division nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return SQLUtils.evaluateValuesByOperator(getValueObject(call.operands.get(0), values, origcolreqcolmap),
					getValueObject(call.operands.get(1), values, origcolreqcolmap), "/");
		} else if (node.isA(SqlKind.LITERAL)) {
			// For literals, return their value
			RexLiteral literal = (RexLiteral) node;
			return SQLUtils.getValue(literal, literal.getType().getSqlTypeName());
		} else if (node.isA(SqlKind.FUNCTION)) {
			// For functions, return their value
			return evaluateRexNode(node, values, origcolreqcolmap);
		} else if (node instanceof RexInputRef inputRef) {
			// Handle column references
			return nonNull(values) ? values[nonNull(origcolreqcolmap) ? origcolreqcolmap.get(inputRef.getIndex())
					: inputRef.getIndex()] : inputRef.getIndex();
		} else {
			return null;
		}
	}

	public static void evaluateRexNodeAsRexInputRef(RexNode node, List<Integer> indexes) {
		if (node.isA(SqlKind.PLUS) || node.isA(SqlKind.MINUS) || node.isA(SqlKind.TIMES) || node.isA(SqlKind.DIVIDE)) {
			// For addition nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			evaluateRexNodeAsRexInputRef(call.operands.get(0), indexes);
			evaluateRexNodeAsRexInputRef(call.operands.get(1), indexes);
		}
		else if (node.isA(SqlKind.FUNCTION) || node.isA(SqlKind.CASE)) {
			RexCall call = (RexCall) node;
			RexNode expfunc = call.getOperands().size() > 0 ? call.getOperands().get(0) : null;
			String name = call.getOperator().getName().toLowerCase();
			switch (name) {
			case "abs":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "length", "char_length", "character_length":
				// Get the length of string value
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "round":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "ceil":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "floor":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "power":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				break;
			case "pow":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				break;
			case "sqrt":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "exp":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "loge":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "lowercase", "lower", "lcase":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "uppercase", "upper", "ucase":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "base64encode":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "base64decode":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "normalizespaces":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "currentisodate":
				break;
			case "current_timemillis":
				break;
			case "rand":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "rand_integer":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				break;
			case "acos":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "asin":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "atan":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "cos":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "sin":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "tan":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "cosec":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "sec":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "cot":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "cbrt":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "degrees":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "radians":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "trimstr":

				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "substring":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "overlay":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				break;

			case "locate":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				break;
			case "cast":
				evaluateRexNodeAsRexInputRef(expfunc, indexes);
				break;
			case "group_concat":
				RexNode rexnode1 = call.getOperands().get(0);
				RexNode rexnode2 = call.getOperands().get(1);
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				evaluateRexNodeAsRexInputRef(rexnode2, indexes);
				break;
			case "concat":
				rexnode1 = call.getOperands().get(0);
				rexnode2 = call.getOperands().get(1);
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				evaluateRexNodeAsRexInputRef(rexnode2, indexes);
				break;
			case "position":
				rexnode1 = call.getOperands().get(0);
				rexnode2 = call.getOperands().get(1);
				RexNode rexnode3 = call.getOperands().size() > 2 ? call.getOperands().get(2) : null;
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				evaluateRexNodeAsRexInputRef(rexnode2, indexes);
				evaluateRexNodeAsRexInputRef(rexnode3, indexes);
				break;
			case "initcap":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				break;
			case "ascii":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				break;
			case "charac":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				break;
			case "insertstr":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(2), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(3), indexes);
				break;
			case "leftchars":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				break;
			case "rightchars":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				break;
			case "reverse":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				break;
			case "trim":
				evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(1), indexes);
				evaluateRexNodeAsRexInputRef(call.getOperands().get(2), indexes);
				break;
			case "ltrim":
				rexnode1 = call.getOperands().get(0);
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				break;
			case "rtrim":
				rexnode1 = call.getOperands().get(0);
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				break;
			case "curdate":
				date.format(new Date(System.currentTimeMillis()));
				break;
			case "curtime":
				time.format(new Date(System.currentTimeMillis()));
				break;
			case "now":
				dateExtract.format(new Date(System.currentTimeMillis()));
				break;
			case "year":
				rexnode1 = call.getOperands().get(0);
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				break;
			case "month":
				rexnode1 = call.getOperands().get(0);
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				break;

			case "day":
				rexnode1 = call.getOperands().get(0);
				evaluateRexNodeAsRexInputRef(rexnode1, indexes);
				break;
			case "case":
				List<RexNode> rexnodes = call.getOperands();
				for (int numnodes = 0; numnodes < rexnodes.size(); numnodes += 1) {
					evaluateRexNodeAsRexInputRef(rexnodes.get(numnodes), indexes);
				}
				break;
			}
		} else if (node instanceof RexCall call && call.getOperator() instanceof SqlFloorFunction) {
			evaluateRexNodeAsRexInputRef(call.getOperands().get(0), indexes);
		} else if (node instanceof RexInputRef inputRef) {
			indexes.add(inputRef.getIndex());
		}
	}
}
