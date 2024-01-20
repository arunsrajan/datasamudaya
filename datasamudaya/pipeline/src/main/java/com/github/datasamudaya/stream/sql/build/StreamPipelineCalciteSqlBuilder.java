package com.github.datasamudaya.stream.sql.build;

import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableAggregateBase;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.adapter.enumerable.EnumerableSortedAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.functions.CoalesceFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.StreamPipeline;
import com.github.datasamudaya.stream.utils.SQLUtils;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.util.validation.Validation;
import net.sf.jsqlparser.util.validation.ValidationError;
import net.sf.jsqlparser.util.validation.feature.DatabaseType;

/**
 * The SQL builder class.
 * 
 * @author arun
 *
 */
public class StreamPipelineCalciteSqlBuilder implements Serializable {
	private static final long serialVersionUID = -8585345445522511086L;
	private static final Logger log = LoggerFactory.getLogger(StreamPipelineCalciteSqlBuilder.class);
	String sql;
	String db;
	String fileformat;
	ConcurrentMap<String, String> tablefoldermap = new ConcurrentHashMap<>();
	ConcurrentMap<String, List<String>> tablecolumnsmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap = new ConcurrentHashMap<>();
	String hdfs;
	transient PipelineConfig pc;
	private final Stack<List<String>> columnstack = new Stack<>();

	private StreamPipelineCalciteSqlBuilder() {

	}

	/**
	 * Creates a new sql builder object.
	 * 
	 * @return sql builder object.
	 */
	public static StreamPipelineCalciteSqlBuilder newBuilder() {
		return new StreamPipelineCalciteSqlBuilder();
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
	public StreamPipelineCalciteSqlBuilder add(String folder, String tablename, List<String> columns,
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
	public StreamPipelineCalciteSqlBuilder setHdfs(String hdfs) {
		this.hdfs = hdfs;
		return this;
	}

	/**
	 * Sets the pipelineconfig object to run sql using the configuration.
	 * 
	 * @param pc
	 * @return sql builder object
	 */
	public StreamPipelineCalciteSqlBuilder setPipelineConfig(PipelineConfig pc) {
		this.pc = pc;
		return this;
	}

	/**
	 * Sets the sql query.
	 * 
	 * @param sql
	 * @return sql builder object
	 */
	public StreamPipelineCalciteSqlBuilder setSql(String sql) {
		this.sql = sql;
		return this;
	}

	/**
	 * Sets the sql db
	 * 
	 * @param db
	 * @return sql builder object
	 */
	public StreamPipelineCalciteSqlBuilder setDb(String db) {
		this.db = db;
		return this;
	}
	
	public StreamPipelineCalciteSqlBuilder setFileformat(String fileformat) {
		this.fileformat = fileformat;
		return this;
	}

	/**
	 * The build method to create sql pipeline object.
	 * 
	 * @return SQL pipeline object
	 * @throws Exception
	 */
	public StreamPipelineSql build() throws Exception {
		if(nonNull(pc)) {
			pc.setSqlpigquery(sql);
		}
		CCJSqlParserManager parserManager = new CCJSqlParserManager();
		Validation validation = new Validation(
				Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB, DatabaseType.POSTGRESQL, DatabaseType.H2),
				sql);
		List<ValidationError> errors = validation.validate();
		if (!CollectionUtils.isEmpty(errors)) {
			log.error("Syntax error in SQL {}", errors);
			throw new Exception("Syntax error in SQL");
		}
		RelNode relnode = SQLUtils.validateSql(tablecolumnsmap, tablecolumntypesmap, sql, db);
		descendants.put(relnode, false);
		return new StreamPipelineSql(execute(relnode, 0));
	}

	private Map<RelNode, Boolean> descendants = new ConcurrentHashMap<>();
	
	/**
	 * Execute the sql statement.
	 * 
	 * @param statement
	 * @return output of the sql execution
	 * @throws Exception
	 */
	protected StreamPipeline<?> execute(RelNode relNode, int depth) throws Exception {
		
		List<RelNode> inputs = relNode.getInputs();
		if(CollectionUtils.isNotEmpty(inputs)) {
			StreamPipeline<?> sp = null;
	        for (RelNode child : inputs) {
	        	descendants.put(child, true);
	        	sp = execute(child, depth + 1);	        	
	        }
	        return buildStreamPipeline((StreamPipeline<Object[]>) sp, relNode);
		}
		descendants.put(relNode, true);
		return buildStreamPipeline(null, relNode);
	}
	
	/**
	 * Build the streamed pipeline object
	 * @param sp
	 * @param relNode
	 * @return streamed pipeline object
	 * @throws Exception 
	 */
	protected StreamPipeline<?> buildStreamPipeline(StreamPipeline<Object[]> sp, RelNode relNode) throws Exception{
		if(relNode instanceof EnumerableTableScan ets) {
			String table = ets.getTable().getQualifiedName().get(1);
			return StreamPipeline.newCsvStreamHDFSSQL(hdfs, tablefoldermap.get(table), this.pc,
					tablecolumnsmap.get(table).toArray(new String[0]),
					tablecolumntypesmap.get(table), tablecolumnsmap.get(table));
		} else if(relNode instanceof EnumerableFilter ef) {
			sp = sp.filter(values->SQLUtils.evaluateExpression(ef.getCondition(), (Object[]) values[0]));
			if(!SQLUtils.hasDescendants(relNode, descendants)) {
				return sp.map(values->values[0]);
			}
			return sp;
		} else if(relNode instanceof EnumerableSort es) {
			sp = orderBy(sp, es);
			if(!SQLUtils.hasDescendants(relNode, descendants)) {
				return sp.map(values->values[0]);
			}
			return sp;
		} else if(relNode instanceof EnumerableHashJoin ehj) {
			sp = (StreamPipeline<Object[]>) buildJoinPredicate((StreamPipeline<Object[]>)buildStreamPipeline(null, ehj.getLeft())
					, (StreamPipeline<Object[]>)buildStreamPipeline(null,ehj.getRight())
					, ehj.getJoinType(),
					ehj.getCondition()).map(new MapFunction<Tuple2<Object[], Object[]>, Object[]>() {
						private static final long serialVersionUID = -132962119666155193L;

						@Override
						public Object[] apply(Tuple2<Object[], Object[]> tup2) {
							
							return new Object[] {concatenate(((Object[])tup2.v1()[0]), ((Object[])tup2.v2()[0])), 
									concatenate(((Object[])tup2.v1()[1]), ((Object[])tup2.v2()[1]))};		                   
						}
					});;
			if(!SQLUtils.hasDescendants(relNode, descendants)) {
				return sp.map(values->values[0]);
			}
			return sp;
		} else if(relNode instanceof EnumerableProject ep) {
			boolean hasdecendants = SQLUtils.hasDescendants(relNode, descendants);
			
			List<SqlTypeName> togeneratezerobytype = ep.getProjects().stream().map(rexnode->SQLUtils.findGreatestType(rexnode)).toList();
			return sp.map(values->{
				List<RexNode> columns = ep.getProjects();
				
		        // Extract the expressions from the Project
				List<Object> valuestoprocess = new ArrayList<>();
				List<Boolean> valuestoconsider = null;
				if(hasdecendants) {
					valuestoconsider = new ArrayList<>();
				}
				if (values[0] instanceof Object[] && values.length == 2 && values[1] instanceof Object[]) {
					for (int valueindex = 0; valueindex < columns.size(); valueindex++) {
						RexNode cols = columns.get(valueindex);
						if (SQLUtils.toEvaluateRexNode(cols, (Object[]) values[1])) {
							valuestoprocess.add(SQLUtils.evaluateRexNode(cols, (Object[]) values[0]));
							if (hasdecendants) {
								valuestoconsider.add(true);
							}
						} else {
							valuestoprocess.add(SQLUtils.generateZeroLiteral(togeneratezerobytype.get(valueindex)));
							if (hasdecendants) {
								valuestoconsider.add(false);
							}
						}
					}
				} else {
					for (int valueindex = 0; valueindex < columns.size(); valueindex++) {
						RexNode cols = columns.get(valueindex);
						valuestoprocess.add(SQLUtils.evaluateRexNode(cols, values));
					}
				}
				if(!hasdecendants) {
					return valuestoprocess.toArray(new Object[0]);
				}
				return new Object[] {valuestoprocess.toArray(new Object[0]),valuestoconsider.toArray(new Object[0])};
			});
		} else if(relNode instanceof EnumerableAggregate || relNode instanceof EnumerableSortedAggregate) {			
			return 
				sp.mapToPair(new MapToPairFunction<Object[], Tuple2<Tuple, Tuple>>() {
					private static final long serialVersionUID = 8102198486566760753L;
					EnumerableAggregateBase grpby = (EnumerableAggregateBase) relNode;
					List<Pair<AggregateCall, String>> aggfunctions = grpby.getNamedAggCalls();
					@Override
					public Tuple2<Tuple, Tuple> apply(Object[] values) {
						List<Object> fnobj = new ArrayList<>();
						int[] colindexes = SQLUtils.getGroupByColumnIndexes(grpby);
						Object[] grpbyobj = {DataSamudayaConstants.EMPTY};
						int index = 0;
						if (colindexes.length > 0) {
							grpbyobj = new Object[colindexes.length];
							for (; index < colindexes.length; index++) {
								grpbyobj[index] = ((Object[]) values[0])[colindexes[index]];
							}
						}
						index = 0;
						for ( ;index<aggfunctions.size();index++) {
							Pair<AggregateCall, String> expr = aggfunctions.get(index);
							if (expr.getKey().getAggregation().getName().equalsIgnoreCase("count")) {
								fnobj.add(1l);
							} else {
								fnobj.add(((Object[])values[0])[expr.getKey().getArgList().get(0)]);
								String functionname = expr.getKey().getAggregation().getName().toLowerCase();
								long cval = 0l;
								if ((boolean)((Object[])values[1])[expr.getKey().getArgList().get(0)]) {
									cval = 1l;
								}
								if (functionname.startsWith("avg")) {
									fnobj.add(cval);
								}
							}
						}

						return Tuple.tuple(SQLUtils.convertObjectToTuple(grpbyobj),
								SQLUtils.convertObjectToTuple(fnobj.toArray(new Object[0])));

					}
				}).reduceByKey(new ReduceByKeyFunction<Tuple>() {
					private static final long serialVersionUID = -8773950223630733894L;
					List<Pair<AggregateCall, String>> aggregatefunc = ((EnumerableAggregateBase) relNode).getNamedAggCalls();

					@Override
					public Tuple apply(Tuple tuple1, Tuple tuple2) {
						return SQLUtils.evaluateTuple(aggregatefunc, tuple1, tuple2);
					}

				}).coalesce(1, new CoalesceFunction<Tuple>() {
					private static final long serialVersionUID = -6496272568103409255L;
					List<Pair<AggregateCall, String>> aggregatefunc = ((EnumerableAggregateBase) relNode).getNamedAggCalls();

					@Override
					public Tuple apply(Tuple tuple1, Tuple tuple2) {
						return SQLUtils.evaluateTuple(aggregatefunc, tuple1, tuple2);
					}

				}).map(new MapFunction<Tuple2<Tuple, Tuple>, Object[]>() {						
					private static final long serialVersionUID = 8056744594467835712L;

					@Override
					public Object[] apply(Tuple2<Tuple, Tuple> tuple2) {						
						Object[] valuesgrpby = SQLUtils.populateObjectFromTuple(tuple2.v1);
						Object[] valuesfromfunctions = SQLUtils.populateObjectFromFunctions(tuple2.v2, ((EnumerableAggregateBase) relNode).getNamedAggCalls());
						Object[] mergeobject =  null;
						if(nonNull(valuesgrpby)) {
							mergeobject = new Object[valuesgrpby.length+valuesfromfunctions.length];
						} else {
							mergeobject = new Object[valuesfromfunctions.length];
						}
						int valuecount = 0;
						if(nonNull(valuesgrpby)) {
							for(Object value:valuesgrpby) {
								mergeobject[valuecount] = value;
								valuecount++;
							}
						}
						for(Object value:valuesfromfunctions) {
							mergeobject[valuecount] = value;
							valuecount++;
						}
						return mergeobject;
				}});
		}
		return sp;
	}
	
	
	/**
	 * Merges two object array in to single
	 * @param <T>
	 * @param a
	 * @param b
	 * @return merged object
	 */
	public static <T> Object[] concatenate(T[] a, T[] b) 
    { 
        return Stream.of(a, b) 
                    .flatMap(Stream::of) 
                    .toArray(); 
    } 

	/**
	 * Join for left, right and inner.
	 * @param pipeline1
	 * @param pipeline2
	 * @param jointable
	 * @param expression
	 * @param inner
	 * @param left
	 * @param right
	 * @return streampipeline object
	 */
	public static StreamPipeline<Tuple2<Object[], Object[]>> buildJoinPredicate(
			StreamPipeline<Object[]> pipeline1, StreamPipeline<Object[]> pipeline2,
			JoinRelType jointype,
			RexNode expression
			) throws PipelineException {
		if (jointype == JoinRelType.INNER) {
			return pipeline1.join(pipeline2, new JoinPredicate<Object[], Object[]>() {
				private static final long serialVersionUID = -1432723151946554217L;
				public boolean test(Object[] rowleft, Object[] rowright) {
					return SQLUtils.evaluateExpression(expression, ((Object[])rowleft[0]), ((Object[])rowright[0]));
				}
			});
		} else if (jointype == JoinRelType.LEFT) {
			return pipeline1.leftOuterjoin(pipeline2,
					new LeftOuterJoinPredicate<Object[], Object[]>() {
						private static final long serialVersionUID = -9071237179844212655L;

						public boolean test(Object[] rowleft, Object[] rowright) {
							return SQLUtils.evaluateExpression(expression, ((Object[])rowleft[0]), ((Object[])rowright[0]));
						}
					});
		} else if(jointype == JoinRelType.RIGHT) {
			return pipeline1.rightOuterjoin(pipeline2,
					new RightOuterJoinPredicate<Object[], Object[]>() {
						private static final long serialVersionUID = 7097332223096552391L;						
						public boolean test(Object[] rowleft, Object[] rowright) {
							return SQLUtils.evaluateExpression(expression, ((Object[])rowleft[0]), ((Object[])rowright[0]));
						}
					});
		}
		return null;
	}

	/**
	 * Exceutes the order by in sql query
	 * 
	 * @param pipelinefunction
	 * @param plainSelect
	 * @return stream of maps
	 * @throws Exception
	 */
	public StreamPipeline<Object[]> orderBy(StreamPipeline<Object[]> pipelinefunction,
			EnumerableSort es) throws Exception {
			var fcs = es.getCollation().getFieldCollations();
			pipelinefunction = pipelinefunction.sorted((obj1, obj2) -> {
				for (int i = 0;i < fcs.size();i++) {
					RelFieldCollation fc = fcs.get(i);
					String sortOrder = fc.getDirection().name();
					Object value1 = ((Object[])obj1[0])[fc.getFieldIndex()];
					Object value2 = ((Object[])obj2[0])[fc.getFieldIndex()];
					int result = SQLUtils.compareTo(value1, value2);
					if ("DESCENDING".equals(sortOrder)) {
						result = -result;
					}
					if (result != 0) {
						return result;
					}
				}
				return 0;
			});
			return pipelinefunction;
		}		

}
