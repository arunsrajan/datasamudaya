package com.github.datasamudaya.stream.pig;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.LocalExecType;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.parser.QueryParserDriver;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple10;
import org.jooq.lambda.tuple.Tuple11;
import org.jooq.lambda.tuple.Tuple12;
import org.jooq.lambda.tuple.Tuple13;
import org.jooq.lambda.tuple.Tuple14;
import org.jooq.lambda.tuple.Tuple15;
import org.jooq.lambda.tuple.Tuple16;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;
import org.jooq.lambda.tuple.Tuple4;
import org.jooq.lambda.tuple.Tuple5;
import org.jooq.lambda.tuple.Tuple6;
import org.jooq.lambda.tuple.Tuple7;
import org.jooq.lambda.tuple.Tuple8;
import org.jooq.lambda.tuple.Tuple9;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.GlobalPigServer;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.functions.CoalesceFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.stream.StreamPipeline;
import com.github.datasamudaya.stream.utils.SQLUtils;

import net.sf.jsqlparser.expression.Function;

/**
 * Utils class for PIG
 * @author Administrator
 *
 */
public class PigUtils {
	
	private PigUtils() {}

	private static final Logger log = LoggerFactory.getLogger(PigUtils.class);
	
	/**
	 * Get the Pig Query parser by passing scope
	 * @param scope
	 * @return query parser
	 * @throws Exception
	 */
	public static synchronized QueryParserDriver getQueryParserDriver(String scope) throws Exception {
		PigServer pigServer = null;
		PigContext pigcontext = null;
		if(nonNull(GlobalPigServer.getPigServer())) {
			pigServer = GlobalPigServer.getPigServer();
			pigcontext = pigServer.getPigContext();
		}
		else {
			Configuration conf = new Configuration();
	        pigcontext = new PigContext(LocalExecType.LOCAL, conf);
	        pigServer = new PigServer(pigcontext, true);
	        FuncSpec funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.AbsUDF");
			pigServer.registerFunction("abs", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LengthUDF");
			pigServer.registerFunction("length", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RoundUDF");
			pigServer.registerFunction("round", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CeilUDF");
			pigServer.registerFunction("ceil", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.FloorUDF");
			pigServer.registerFunction("floor", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.PowerUDF");
			pigServer.registerFunction("pow", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.SqrtUDF");
			pigServer.registerFunction("sqrt", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ExpUDF");
			pigServer.registerFunction("exp", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LogeUDF");
			pigServer.registerFunction("loge", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LowercaseUDF");
			pigServer.registerFunction("lowercase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.UppercaseUDF");
			pigServer.registerFunction("uppercase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.Base64EncodeUDF");
			pigServer.registerFunction("base64encode", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.Base64DecodeUDF");
			pigServer.registerFunction("base64decode", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.NormalizeSpacesUDF");
			pigServer.registerFunction("normalizespaces", funcSpec);
	        GlobalPigServer.setPigServer(pigServer);
		}
        Map<String, String> filenamemap = new HashMap<>();
        QueryParserDriver parserdriver = new QueryParserDriver(pigcontext, scope, filenamemap);
		return parserdriver;
	}

	/**
	 * Get Operators by passing pigquery and query parser
	 * @param pigquery
	 * @param queryparserdriver
	 * @return operators
	 * @throws Exception
	 */
	public static Iterator<Operator> getOperator(String pigquery, QueryParserDriver queryparserdriver) throws Exception {
		LogicalPlan logicalplan = queryparserdriver.parse(pigquery);
		return logicalplan.getOperators();
	}
	
	
	/**
	 * Get LogicalPlan by passing pigquery and query parser
	 * @param pigquery
	 * @param queryparserdriver
	 * @return logical plan
	 * @throws Exception
	 */
	public static LogicalPlan getLogicalPlan(List<String> pigQueries, QueryParserDriver queryparserdriver) throws Exception {
		try {
			StringBuilder pigCommands = new StringBuilder();
			pigQueries.forEach(pigCommands::append);
			return queryparserdriver.parse(pigCommands.toString());
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}
	
	/**
	 * Get Stream object for LOLoad operator
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @param loload
	 * @param pipelineconfig
	 * @return stream object
	 * @throws Exception
	 */
	public static StreamPipeline<?> executeLOLoad(String user, String jobid, String tejobid, LOLoad loload, PipelineConfig pipelineconfig) throws Exception {		
		String[] headers = getHeaderFromSchema(loload.getSchema());
		List<SqlTypeName> schematypes = getTypesFromSchema(loload.getSchema());
		return StreamPipeline.newCsvStreamHDFSSQL(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT), loload.getSchemaFile(),
				pipelineconfig, headers, schematypes, Arrays.asList(headers));
	}
	
	/**
	 * Convert value to for a given data type
	 * @param value
	 * @param type
	 * @return converted value
	 */
	public static Object getValue(String value, Class<?> type) {
		try {
			if(type == Integer.class) {
				return Integer.valueOf(value);
			} else if(type == Long.class) {
				return Long.valueOf(value);
			} else if(type == String.class) {
				return String.valueOf(value);
			} else if(type == Float.class) {
				return Float.valueOf(value);			
			} else if(type == Double.class) {
				return Double.valueOf(value);
			} else {
				return String.valueOf(value);
			}
		} catch(Exception ex) {
			if(type == Integer.class) {
				return Integer.valueOf(0);
			} else if(type == Long.class) {
				return Long.valueOf(0l);
			} else if(type == String.class) {
				return String.valueOf(0);
			} else if(type == Float.class) {
				return Float.valueOf(0.0f);			
			} else if(type == Double.class) {
				return Double.valueOf(0.0d);
			} else {
				return String.valueOf(0);
			}
		}
	}
	
	/**
	 * filter the data
	 * @param sp
	 * @param loFilter
	 * @return filtered data
	 * @throws Exception
	 */
	public static StreamPipeline<Object[]> executeLOFilter(StreamPipeline<Object[]> sp, LOFilter loFilter, List<String> coloralias, List<String> outcols, boolean hasdescendants) throws Exception {
		outcols.addAll(coloralias);
		LogicalExpressionPlan lep = loFilter.getFilterPlan();
		List<Operator> exp = lep.getSources();
		sp = sp
				.filter(obj -> {
					try {
						return evaluateExpression((LogicalExpression) exp.get(0), obj, coloralias);
					} catch (Exception e) {
						return false;
					}
				});
		if(!hasdescendants) {
			return sp.map(obj->((Object[])obj[0]));
		}
		return sp;
	}
	
	/**
	 * Executes the sorting stream in ascending or descending order 
	 * @param sp
	 * @param loSort
	 * @return sorted stream in ascending or in descending order of columns
	 * @throws Exception
	 */
	public static StreamPipeline<Object[]> executeLOSort(StreamPipeline<Object[]> sp, LOSort loSort, List<String> aliasorcolumns, List<String> outcols, boolean hasdescendants) throws Exception {
		outcols.addAll(aliasorcolumns);
		List<LogicalExpressionPlan> leps = loSort.getSortColPlans();
		Iterator<Boolean> asccolumns = loSort.getAscendingCols().iterator();
		List<SortOrderColumns> sortordercolumns = new ArrayList<>();
		for(LogicalExpressionPlan lep:leps) {
			ProjectExpression projectexpression = (ProjectExpression) lep.getOperators().next();
			SortOrderColumns soc = new SortOrderColumns();
			soc.setColumn(projectexpression.getColAlias());
			soc.setIsasc(asccolumns.next());
			sortordercolumns.add(soc);
		}
		sp = sp.sorted((map1, map2) -> {
			List<SortOrderColumns> columnssortorder = sortordercolumns;

			for (int i = 0;i < columnssortorder.size();i++) {
				String columnName = columnssortorder.get(i).getColumn();
				Boolean isAsc = columnssortorder.get(i).isIsasc();
				Object value1 = ((Object[])map1[0])[aliasorcolumns.indexOf(columnName)];
				Object value2 = ((Object[])map2[0])[aliasorcolumns.indexOf(columnName)];
				int result = SQLUtils.compareTo(value1, value2);
				if (!isAsc) {
					result = -result;
				}
				if (result != 0) {
					return result;
				}
			}
			return 0;
		});
		if(!hasdescendants) {
			return sp.map(obj->((Object[])obj[0]));
		}
		return sp;
	}
	
	/**
	 * Execute Distinct values 
	 * @param sp
	 * @return distinct values
	 * @throws Exception
	 */
	public static StreamPipeline<Object[]> executeLODistinct(StreamPipeline<Object[]> sp) throws Exception {
		
		return sp
				.map(new MapToPairFunction<Object[], Object[]>() {
					private static final long serialVersionUID = 918430313352259174L;
					@Override
					public Object[] apply(Object[] record) {						
						return ((Object[])record[0]);
					}

				}).map(Arrays::asList).distinct()
				.map(list -> list.toArray(new Object[0]))
				.mapToPair(new MapToPairFunction<Object[], Tuple2<Object[], Double>>() {
					private static final long serialVersionUID = -6412672309048067129L;

					@Override
					public Tuple2<Object[], Double> apply(Object[] record) {

						return new Tuple2<Object[], Double>(record, 0.0d);
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

				}).map(tup2->tup2.v1()).map(Arrays::asList).distinct()
				.map(list -> list.toArray(new Object[0]));
	}
	
	/**
	 * Join two streams based on columns
	 * @param sp1
	 * @param sp2
	 * @param loJoin
	 * @return joined stream object 
	 * @throws Exception 
	 */
	public static StreamPipeline<Object[]> executeLOJoin(StreamPipeline<Object[]> sp1,
			StreamPipeline<Object[]> sp2,
			List<String> columnsleft, List<String> columnsright,			
			LOJoin loJoin,
			List<String> reqcolsleft, 
			List<String> allcolsleft,
			List<String> reqcolsright, 
			List<String> allcolsright,
			List<String> aliasleft, 
			List<String> aliasright,
			boolean hasdescendants) throws Exception {
		
		StreamPipeline<Object[]> sp = sp1.join(sp2, new JoinPredicate<Object[], Object[]>() {
			private static final long serialVersionUID = -2218859526944624786L;
			List<String> leftablecol = columnsleft;
			List<String> righttablecol = columnsright;
			List<String> reqcolleft = reqcolsleft;
			List<String> allcolleft = allcolsleft;
			List<String> reqcolright = reqcolsright;
			List<String> allcolright = allcolsright;
			List<String> alileft = aliasleft; 
			List<String> aliright = aliasright; 
			public boolean test(Object[] rowleft, Object[] rowright) {
				for (int columnindex = 0;columnindex < leftablecol.size();columnindex++) {
					String leftcol = leftablecol.get(columnindex);
					String rightcol = righttablecol.get(columnindex);
					Object leftvalue = ((Object[])rowleft[0])[alileft.indexOf(leftcol)];
					Object rightvalue = ((Object[])rowright[0])[aliright.indexOf(rightcol)];
					if(leftvalue==null && rightvalue == null||nonNull(leftvalue) && nonNull(rightvalue) && !leftvalue.equals(rightvalue)) {
						return false;
					}
				}
				return true;
			}
		}).map(new MapFunction<Tuple2<Object[], Object[]>, Object[]>() {
			private static final long serialVersionUID = -504784749432944561L;

			@Override
			public Object[] apply(
					Tuple2<Object[], Object[]> tup2) {				
				return new Object[] {concatenate(((Object[])tup2.v1()[0]), ((Object[])tup2.v2()[0])), 
						concatenate(((Object[])tup2.v1()[1]), ((Object[])tup2.v2()[1]))};
			}
		});
		
		if(!hasdescendants) {
			return sp.map(obj->((Object[])obj[0]));
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
	 * This function evaluates to consider the expression 
	 * @param expression
	 * @param row
	 * @param aliasorcolname
	 * @return true or false
	 * @throws Exception
	 */
	public static boolean toEvaluateBinaryExpression(LogicalExpression expression, Object[] row, List<String> aliasorcolname) throws Exception {
		if(expression instanceof BinaryExpression bex) {
			String operator = expression.getName();
			LogicalExpression leftExpression = bex.getLhs();
			LogicalExpression rightExpression = bex.getRhs();
		    boolean leftValue=false;
		    boolean rightValue=false;
		    if(leftExpression instanceof UserFuncExpression fn) {
		    	leftValue = toEvaluateBinaryExpression(leftExpression, row, aliasorcolname);
		    }
		    else if (leftExpression instanceof ConstantExpression lv) {
		    	leftValue =  true;
		    } else if (leftExpression instanceof ProjectExpression pex) {
		        String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[])row[1])[aliasorcolname.indexOf(columnName)];
				leftValue =  (boolean) value;
		    } else if (leftExpression instanceof BinaryExpression) {
		    	leftValue = toEvaluateBinaryExpression(leftExpression, row, aliasorcolname);
		    }
		    
		    if(rightExpression instanceof UserFuncExpression fn) {
		    	rightValue = toEvaluateBinaryExpression(rightExpression, row, aliasorcolname);
		    }
		    else if (rightExpression instanceof ConstantExpression lv) {
		    	rightValue =  true;
		    } else if (rightExpression instanceof ProjectExpression pex) {
		        String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[])row[1])[aliasorcolname.indexOf(columnName)];
				rightValue =  (boolean) value;
		    } else if (rightExpression instanceof BinaryExpression) {
		    	rightValue = toEvaluateBinaryExpression(rightExpression, row, aliasorcolname);
		    }
		    switch (operator) {
		        case "Add":
		        case "Subtract":
		        case "Multiply":
		        case "Divide":
		        	return leftValue && rightValue;
		        default:
		            throw new IllegalArgumentException("Invalid operator: " + operator);
		    }
		} 
		else if (expression instanceof ConstantExpression lv) {
	    	return true;
	    } else if (expression instanceof ProjectExpression pex) {
	        String columnName = pex.getFieldSchema().alias;
			return (boolean)((Object[])row[1])[aliasorcolname.indexOf(columnName)];
	    }
		return false;
	}
	
	
	/**
	 * Flatten source map to formatted map
	 * @param sp
	 * @param loForEach
	 * @return formatted map stream
	 * @throws Exception
	 */
	public static StreamPipeline<Object[]> executeLOForEach(StreamPipeline<Object[]> sp, LOForEach loForEach, List<String> aliasorcolumns, List<String> outcols, boolean hasdescendants) throws Exception {
		
		List<FunctionParams> functionparams = getFunctionsWithParamsGrpBy(loForEach);
		LogicalExpression[] lexp = getLogicalExpressions(functionparams);
		LogicalExpression[] headers = getHeaders(functionparams);
		
		outcols.addAll(functionparams.stream().map(fp->fp.getAlias()).toList());
		
		Set<String> grpbyheader = new LinkedHashSet<>();
		
		if(nonNull(headers)) {
			List<String> columns = new ArrayList<>();
			for(LogicalExpression lex:headers) {
				getColumnsFromExpressions(lex, columns);
				grpbyheader.addAll(columns);
				columns.clear();
			}
		}
		
		List<String> aliases = getAlias(functionparams);
		
		List<FunctionParams> aggfunctions = getAggFunctions(functionparams);
		List<FunctionParams> nonaggfunctions = getNonAggFunctions(functionparams);
		if(CollectionUtils.isEmpty(aggfunctions) && CollectionUtils.isEmpty(nonaggfunctions)) {
			sp = sp.filter(obj->{
				try {
					List<String> aliascolumns = aliasorcolumns;
					LogicalExpression[] headera = lexp;
					boolean toevaluateexpression = true;
					for (LogicalExpression exp : headera) {
						toevaluateexpression = toevaluateexpression
								&& toEvaluateBinaryExpression(exp, obj, aliascolumns);
						if (!toevaluateexpression) {
							break;
						}
					}
					return toevaluateexpression;
				} catch(Exception ex) {
					return false;
				}
			}).map(obj -> {
					Object[] formattedvalues = new Object[lexp.length];
					Object[] formattedvaluestoconsider = new Object[lexp.length];
					List<String> aliascolumns = aliasorcolumns;
				try {
					LogicalExpression[] headera = lexp;
					int indexformatted = 0;
					for (LogicalExpression exp : headera) {
						formattedvalues[indexformatted]=evaluateBinaryExpression(exp, obj, null, aliascolumns);
						formattedvaluestoconsider[indexformatted] = true;
						indexformatted++;
					}
					Object[] finalobject = new Object[2];
					finalobject[0] = formattedvalues;
					finalobject[1] = formattedvaluestoconsider;
					return finalobject;
				} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					return new Object[2];
				});
			if(!hasdescendants) {
				return sp.map(obj->((Object[])obj[0]));
			}
			return sp;
		} else {
			
			StreamPipeline<Object[]> pipelinemap = sp;
			if(!CollectionUtils.isEmpty(nonaggfunctions)) {
				pipelinemap = pipelinemap.map(new MapFunction<Object[], Object[]>() {
					private static final long serialVersionUID = 6329566708048046421L;
					List<FunctionParams> nonagg = new ArrayList<>(nonaggfunctions);
					LogicalExpression[] grpby = headers;
					
					@Override
					public Object[] apply(Object[] mapvalues) {
						Object[] nonaggfnvalues = new Object[2];
						Object[] grpbyfnvalues = new Object[(nonNull(grpby)?grpby.length:0) + nonagg.size()];
						Object[] valuestoconsider = new Object[(nonNull(grpby)?grpby.length:0) + + nonagg.size()];
						int index = 0;
						if (nonNull(grpby) && grpby.length > 0) {
							for (LogicalExpression grpobj : grpby) {
								try {
									grpbyfnvalues[index] =  evaluateBinaryExpression(grpobj, mapvalues, null, aliasorcolumns);
									valuestoconsider[index] = true;
								} catch (Exception e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
								index++;
							}
						}
						for (FunctionParams fn : nonagg) {
							Object value = null;
							try {
								value = evaluateBinaryExpression(fn.getParams(), mapvalues, fn.getFunctionName(), aliasorcolumns);
								valuestoconsider[index] = true;
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
							grpbyfnvalues[index] =  value;
							index++;
						}
						nonaggfnvalues[0] = grpbyfnvalues;
						nonaggfnvalues[1] = valuestoconsider;
						return nonaggfnvalues;
	
					}
				});
			}
			if(!CollectionUtils.isEmpty(aggfunctions)) {
				List<List<String>> columnstoeval = new ArrayList<>();
				for (FunctionParams fn : aggfunctions) {
					LogicalExpression expression = fn.getParams();
						if(nonNull(expression)) {
							List<String> columnsfromexp = new ArrayList<>();
							getColumnsFromExpressions(expression, columnsfromexp);
							columnstoeval.add(columnsfromexp);
						} else {
							columnstoeval.add(new ArrayList<>());
						}						
				}
				pipelinemap = pipelinemap.mapToPair(new MapToPairFunction<Object[], Tuple2<Tuple, Tuple>>() {
					private static final long serialVersionUID = 8102198486566760753L;
					List<FunctionParams> aggfunc = aggfunctions;
					LogicalExpression[] grpby = headers;
					List<List<String>> columnsevaluation = columnstoeval;

					@Override
					public Tuple2<Tuple, Tuple> apply(Object[] mapvalues) {
						List<Object> fnobj = new ArrayList<>();
						Object[] grpbyobj = null;

						int index = 0;
						if (nonNull(grpby) && grpby.length > 0) {
							grpbyobj = new Object[grpby.length];
							for (LogicalExpression grpobj : grpby) {
								try {
									grpbyobj[index] = evaluateBinaryExpression(grpobj, mapvalues, null, aliasorcolumns);
								} catch (Exception e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
								index++;
							}
						} else {
							grpbyobj = new Object[1];
							grpbyobj[0] = DataSamudayaConstants.EMPTY;
						}
						index = 0;
						for (FunctionParams functionParam : aggfunc) {
							if (functionParam.getFunctionName().equals("count")) {
								fnobj.add(1);
							} else {
								try {
									fnobj.add(evaluateBinaryExpression(functionParam.getParams(), mapvalues, functionParam.getFunctionName(), aliasorcolumns));
									long cval = 1;
									if (functionParam.getFunctionName().startsWith("avg")) {
										for (String column : columnsevaluation.get(index)) {
											boolean valuetocount = (boolean) ((Object[])mapvalues[1])[aliasorcolumns.indexOf(column)];
											if (!valuetocount) {
												cval = 0;
												break;
											}
										}
										fnobj.add(cval);
									}
								} catch (Exception e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							index++;
						}

						return Tuple.tuple(SQLUtils.convertObjectToTuple(grpbyobj),
								SQLUtils.convertObjectToTuple(fnobj.toArray(new Object[1])));

					}
				}).reduceByKey(new ReduceByKeyFunction<Tuple>() {
					private static final long serialVersionUID = -8773950223630733894L;
					List<FunctionParams> functionParams = aggfunctions;

					@Override
					public Tuple apply(Tuple tuple1, Tuple tuple2) {
						return evaluateTuple(tuple1, tuple2, functionParams);
					}

				}).coalesce(1, new CoalesceFunction<Tuple>() {
					private static final long serialVersionUID = -6496272568103409255L;
					List<FunctionParams> functionParams = aggfunctions;

					@Override
					public Tuple apply(Tuple tuple1, Tuple tuple2) {
						return evaluateTuple(tuple1, tuple2, functionParams);
					}

				}).map(new MapFunction<Tuple2<Tuple, Tuple>, Object[]>() {							
					private static final long serialVersionUID = 9098846821052824347L;
					List<FunctionParams> functionParam = aggfunctions;
					List<String> grpby = new ArrayList<>(grpbyheader);
					@Override
					public Object[] apply(Tuple2<Tuple, Tuple> tuple2) {
						Object[] valueobject = new Object[2];
						valueobject[0] = new Object[aliases.size()];
						valueobject[1] = new Object[aliases.size()];
						for(int count=0;count<aliases.size();count++) {
							((Object[])valueobject[1])[count]=true;
						}
						try {
							populateGroupByFunction((Object[]) valueobject[0], tuple2.v1, grpby, aliases);
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
						populateMapFromFunctions((Object[]) valueobject[0], tuple2.v2, functionParam, aliases);
						return valueobject;
					}
				});
			}
			if(!hasdescendants) {
				return pipelinemap.map(obj->((Object[])obj[0]));
			}
			return pipelinemap;
		}
	}
	
	/**
	 * This function populates group by value in value object
	 * @param valueobject
	 * @param grpbyvalues
	 * @param grpby
	 * @param aliases
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 */
	protected static void populateGroupByFunction(Object[] valueobject, Tuple grpbyvalues, List<String> grpby, List<String> aliases) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?> cls = grpbyvalues.getClass();
		for(int grpindex=1;grpindex<=grpby.size();grpindex++) {
			java.lang.reflect.Method method = cls.getMethod("v"+grpindex);
			Object valuesum = method.invoke(grpbyvalues);
			valueobject[aliases.indexOf(grpby.get(grpindex-1))] = valuesum;
		}
	}
	
	/**
	 * Evaluates Binary Expression.
	 * @param expression
	 * @param row
	 * @return Evaluated value
	 * @throws Exception
	 */
	public static Object evaluateBinaryExpression(LogicalExpression expression, Object[] row, String name, List<String> aliasorcolname) throws Exception {
		if(expression instanceof UserFuncExpression fn) {
			switch (name) {
				case "sum":
                // Get the absolute value of the first parameter	               
                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "sum", fn.getEvalFunc());
				case "count":
	                // Get the absolute value of the first parameter	               
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "count", fn.getEvalFunc());
				case "avg":
	                // Get the absolute value of the first parameter	               
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "avg", fn.getEvalFunc());
				case "abs":
	                // Get the absolute value of the first parameter	               
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "abs", fn.getEvalFunc());
				case "length":
	                // Get the length of string value	                
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "length", fn.getEvalFunc());
	                
				case "round":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "round", fn.getEvalFunc());
				case "ceil":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "ceil", fn.getEvalFunc());
				case "floor":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "floor", fn.getEvalFunc());
				case "pow":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), evaluateBinaryExpression(fn.getArguments().get(1), row, name, aliasorcolname), "pow", fn.getEvalFunc());
				case "sqrt":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "sqrt", fn.getEvalFunc());
				case "exp":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "exp", fn.getEvalFunc());
				case "loge":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "loge", fn.getEvalFunc());
				case "lowercase":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "lowercase", fn.getEvalFunc());
				case "uppercase":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "uppercase", fn.getEvalFunc());
				case "base64encode":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "base64encode", fn.getEvalFunc());
				case "base64decode":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "base64decode", fn.getEvalFunc());
				case "normalizespaces":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname), null, "normalizespaces", fn.getEvalFunc());
				case "substring":
	                // Get the absolute value of the first parameter
					ConstantExpression pos = (ConstantExpression) fn.getArguments().get(1);
					ConstantExpression length = (ConstantExpression) fn.getArguments().get(2);
					String val = (String) evaluateBinaryExpression(fn.getArguments().get(0), row, name, aliasorcolname);
	                return val.substring((int) pos.getValue(), Math.min(((String) val).length(), (int) pos.getValue() + (int) length.getValue()));
			}
		} else if(expression instanceof BinaryExpression bex) {
			String operator = expression.getName();
			LogicalExpression leftExpression = bex.getLhs();
			LogicalExpression rightExpression = bex.getRhs();
		    Object leftValue=null;
		    Object rightValue=null;
		    if(leftExpression instanceof UserFuncExpression fn) {
		    	leftValue = evaluateBinaryExpression(leftExpression, row, null, aliasorcolname);
		    }
		    else if (leftExpression instanceof ConstantExpression lv) {
		    	leftValue =  lv.getValue();
		    } else if (leftExpression instanceof ProjectExpression pex) {
		        String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[])row[0])[aliasorcolname.indexOf(columnName)];
				leftValue =  value;
		    } else if (leftExpression instanceof BinaryExpression) {
		    	leftValue = evaluateBinaryExpression(leftExpression, row, null, aliasorcolname);
		    }
		    
		    if(rightExpression instanceof UserFuncExpression fn) {
		    	rightValue = evaluateBinaryExpression(rightExpression, row, null, aliasorcolname);
		    }
		    else if (rightExpression instanceof ConstantExpression lv) {
		    	rightValue =  lv.getValue();
		    } else if (rightExpression instanceof ProjectExpression pex) {
		        String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[])row[0])[aliasorcolname.indexOf(columnName)];
				rightValue =  value;
		    } else if (rightExpression instanceof BinaryExpression) {
		    	rightValue = evaluateBinaryExpression(rightExpression, row, null, aliasorcolname);
		    }
		    switch (operator) {
		        case "Add":
		            return evaluateValuesByOperator(leftValue, rightValue, "+");
		        case "Subtract":
		            return evaluateValuesByOperator(leftValue, rightValue, "-");
		        case "Multiply":
		            return evaluateValuesByOperator(leftValue, rightValue, "*");
		        case "Divide":
		            return evaluateValuesByOperator(leftValue, rightValue, "/");
		        default:
		            throw new IllegalArgumentException("Invalid operator: " + operator);
		    }
		} 
		else if (expression instanceof ConstantExpression lv) {
	    	return lv.getValue();
	    } else if (expression instanceof ProjectExpression pex) {
	        String columnName = pex.getFieldSchema().alias;
			return ((Object[])row[0])[aliasorcolname.indexOf(columnName)];
	    }
		return Double.valueOf(0.0d);
	}

	/**
	 * This function collects all columns from expression
	 * @param lexp
	 * @param columns
	 * @throws FrontendException 
	 */
	public static void getColumnsFromExpressions(LogicalExpression lexp, List<String> columns) throws FrontendException {
		if(lexp instanceof BinaryExpression bex) {
			LogicalExpression leftExpression = bex.getLhs();
			LogicalExpression rightExpression = bex.getRhs();
		    if (leftExpression instanceof ProjectExpression pex) {
		        columns.add(pex.getFieldSchema().alias);
		    } else if (leftExpression instanceof BinaryExpression) {
		    	getColumnsFromExpressions(leftExpression, columns);
		    } else if (leftExpression instanceof UserFuncExpression) {
		    	getColumnsFromExpressions(leftExpression, columns);
		    }
		    
		    if (rightExpression instanceof ProjectExpression pex) {
		    	 columns.add(pex.getFieldSchema().alias);
		    } else if (rightExpression instanceof BinaryExpression) {
		    	getColumnsFromExpressions(rightExpression, columns);
		    } else if (leftExpression instanceof UserFuncExpression) {
		    	getColumnsFromExpressions(leftExpression, columns);
		    }	   
		} else if(lexp instanceof ProjectExpression pex) {
			columns.add(pex.getFieldSchema().alias);
		} else if (lexp instanceof UserFuncExpression userfuncexp) {
			if (!"org.apache.pig.builtin.COUNT"
					.equals(userfuncexp.getFuncSpec().getClassName())) {
				Iterator<Operator> operators = lexp.getPlan().getOperators();
				for (; operators.hasNext();) {
					Object pexp = operators.next();
					if (pexp instanceof ProjectExpression expression) {
						getColumnsFromExpressions(expression, columns);
					}
				}
			}
	    }
	}
	
	
	/**
	 * Evaluates value with for the function 
	 * @param value
	 * @param powerval
	 * @param name
	 * @param evalfunc
	 * @return evaluated value
	 * @throws Exception
	 */
	public static Object evaluateFunctionsWithType(Object value, Object powerval, String name, EvalFunc<?> evalfunc) throws Exception {
		switch (name) {
		case "sum":
		case "avg":
		case "count":
			return value;
		case "abs":
			// Get the absolute value of the first parameter			
		case "length":
			// Get the length of string value
		case "round":
			// Get the absolute value of the first parameter			
		case "ceil":
			// Get the absolute value of the first parameter			
		case "floor":
			// Get the absolute value of the first parameter
		case "sqrt":
			// Get the absolute value of the first parameter			
		case "exp":
			// Get the absolute value of the first parameter\			
		case "loge":
			// Get the absolute value of the first parameter			
		case "lowercase":
			// Get the absolute value of the first parameter			
		case "uppercase":
			// Get the absolute value of the first parameter
		case "base64encode":
			// Get the absolute value of the first parameter			
		case "base64decode":
			// Get the absolute value of the first parameter
		case "normalizespaces":
			// Get the absolute value of the first parameter
			org.apache.pig.data.Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(value));
			return evalfunc.exec(tuple);
		case "pow":
			// Get the absolute value of the first parameter
			tuple = TupleFactory.getInstance().newTuple(Arrays.asList(value, powerval));
			return evalfunc.exec(tuple);
		}
		return name;
	}
	
	/**
	 * Get Record Count from tuple
	 * @param tuple
	 * @return count in string format
	 */
	public static String getCountFromTuple(Tuple tuple) {
		if(tuple instanceof Tuple1 tup) {
			return String.valueOf(tup.v1);
		} else if(tuple instanceof Tuple2 tup) {
			return String.valueOf(tup.v2);
		} else if(tuple instanceof Tuple3 tup) {
			return String.valueOf(tup.v3);
		} else if(tuple instanceof Tuple4 tup) {
			return String.valueOf(tup.v4);
		} else if(tuple instanceof Tuple5 tup) {
			return String.valueOf(tup.v5);
		} else if(tuple instanceof Tuple6 tup) {
			return String.valueOf(tup.v6);
		} else if(tuple instanceof Tuple7 tup) {
			return String.valueOf(tup.v7);
		} else if(tuple instanceof Tuple8 tup) {
			return String.valueOf(tup.v8);
		} else if(tuple instanceof Tuple9 tup) {
			return String.valueOf(tup.v9);
		} else if(tuple instanceof Tuple10 tup) {
			return String.valueOf(tup.v10);
		} else if(tuple instanceof Tuple11 tup) {
			return String.valueOf(tup.v11);
		} else if(tuple instanceof Tuple12 tup) {
			return String.valueOf(tup.v12);
		} else if(tuple instanceof Tuple13 tup) {
			return String.valueOf(tup.v13);
		} else if(tuple instanceof Tuple14 tup) {
			return String.valueOf(tup.v14);
		} else if(tuple instanceof Tuple15 tup) {
			return String.valueOf(tup.v15);
		} else if(tuple instanceof Tuple16 tup) {
			return String.valueOf(tup.v16);
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}
	
	/**
	 * Populate Map values from function.
	 * @param mapvalues
	 * @param tuple
	 * @param functions
	 * @param functionalias
	 */
	public static void populateMapFromFunctions(Object[] values, Tuple tuple, List<FunctionParams> functions, List<String> aliasorcolname) {
		try {
		Class<?> cls = tuple.getClass();
		for(int funcindex=0,valueindex=1;funcindex<functions.size();funcindex++) {
			FunctionParams func = functions.get(funcindex);
			String funname = func.getFunctionName();
			if(funname.toLowerCase().startsWith("avg")) {
				java.lang.reflect.Method method = cls.getMethod("v"+valueindex);
				Object valuesum = method.invoke(tuple);
				valueindex++;
				method = cls.getMethod("v"+valueindex);
				Object valuecount = method.invoke(tuple);
				values[aliasorcolname.indexOf(getAliasForFunction(func))] = evaluateValuesByOperator(valuesum, valuecount, "/");
			} else {
				java.lang.reflect.Method method = cls.getMethod("v"+valueindex);
				Object value = method.invoke(tuple);
				values[aliasorcolname.indexOf(getAliasForFunction(func))] = value;				
			}
			valueindex++;
		}
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}
	
	/**
	 * Get Alias For Function
	 * @param functionParams
	 * @return
	 */
	public static String getAliasForFunction(FunctionParams functionParams) {		
		return functionParams.getAlias();
	}
	
	/**
	 * Evaluate Tuple for functions
	 * @param tuple1
	 * @param tuple2
	 * @param aggfunctions
	 * @return Tuple
	 */
	public static Tuple evaluateTuple(Tuple tuple1, Tuple tuple2, List<FunctionParams> aggfunctions) {
		try {
            // Get the class of the Tuple
            Class<?> tupleClass = tuple1.getClass();
            // Create a new instance of the Tuple
            Tuple result = (Tuple) tupleClass.getConstructor(tuple2.getClass()).newInstance(tuple1);
            // Get all the fields of the Tuple class
            java.lang.reflect.Field[] fields = tuple1.getClass().getFields();
            int index = 0;
            boolean avgindex = false;
            FunctionParams func = aggfunctions.get(index);
            // Iterate over the fields and perform summation
            for (java.lang.reflect.Field field : fields) {
                // Make the field accessible, as it might be private
                field.setAccessible(true);

                // Get the values of the fields from both tuples
                Object value1 = field.get(tuple1);
                Object value2 = field.get(tuple2);
                field.set(result,evaluateFunction(value1, value2, func));
                // Set the sum of the values in the result tuple                
                if(index+1<aggfunctions.size()) {
                	if(avgindex) {
                		func = aggfunctions.get(index);
                    	avgindex = false;
                    	index++;
                	} else if(!aggfunctions.get(index).getFunctionName().equalsIgnoreCase("avg")) {
                		func = aggfunctions.get(index+1);
                		index++;
                		avgindex = false;
                	} else {
                    	func = aggfunctions.get(index);
                    	avgindex = true;
                    }
                } 
            }

            return result;
        } catch (Exception ex) {
        	log.error(DataSamudayaConstants.EMPTY, ex);
        }
		return null;
	}
	
	/**
	 * Evaluate values based on functions
	 * @param leftValue
	 * @param rightValue
	 * @param function
	 * @return evaluated value
	 */
	public static Object evaluateFunction(Object leftValue, Object rightValue, FunctionParams functionParam) {
		String functionname = functionParam.getFunctionName();
		if (functionname.startsWith("count") || functionname.startsWith("sum") || functionname.startsWith("avg")) {
			return evaluateValuesByOperator(leftValue, rightValue, "+");
		}
		return null;
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
			} else if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv + rv;
			} else if(leftValue instanceof String lv && rightValue instanceof Long rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof String rv) {
				return lv + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof String rv) {
				return lv + rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv + rv;
			} else if (leftValue instanceof String lv && rightValue instanceof String rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv + rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv + rv;
			}
		case "-":
			if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv - rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv - rv;
			}  else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv - rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv - rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv - rv;
			}
		case "*":
			if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv * rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv * rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Integer rv) {
				return lv * rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Double rv) {
				return lv * rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv * rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv * rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv * rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv * rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv * rv;
			}
		case "/":
			if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv / rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv / rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Integer rv) {
				return lv / rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Double rv) {
				return lv / rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv / rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv / (double) rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv / (double) rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv / (double) rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv / (double) rv;
			}
		default:
			throw new IllegalArgumentException("Invalid operator: " + operator);
		}
		
	}
	
	/**
	 * get headers by passing list of function params
	 * @param functionparams
	 * @return array of grpby headers
	 */
	public static LogicalExpression[] getHeaders(List<FunctionParams> functionparams) {
		List<LogicalExpression> headersl = functionparams.stream().filter(fp -> fp.getFunctionName() == null).map(fp -> fp.getParams()).collect(Collectors.toList());
		if(headersl.size()>0) {
			return headersl.toArray(new LogicalExpression[1]);
		} else {
			return null;
		}
	}
	/**
	 * This functions returns all the expressions including columns
	 * @param functionparams
	 * @return allexpressions
	 */
	public static LogicalExpression[] getLogicalExpressions(List<FunctionParams> functionparams) {
		List<LogicalExpression> headersl = functionparams.stream().map(fp -> fp.getParams()).collect(Collectors.toList());
		if(headersl.size()>0) {
			return headersl.toArray(new LogicalExpression[1]);
		} else {
			return null;
		}
	}
	
	/**
	 * get aliases by passing list of function params
	 * @param functionparams
	 * @return array of grpby aliases
	 */
	public static List<String> getAlias(List<FunctionParams> functionparams) {
		return functionparams.stream().map(fp -> fp.getAlias()).collect(Collectors.toList());		
	}
	
	/**
	 * get functions by passing list of function params
	 * @param functionparams
	 * @return list of functions
	 */
	public static List<FunctionParams> getAggFunctions(List<FunctionParams> functionparams) {
		List<FunctionParams> functions = functionparams.stream().filter(fp -> fp.getFunctionName() != null
				&& (fp.getFunctionName().equals("sum")
				|| fp.getFunctionName().equals("count")
				|| fp.getFunctionName().equals("avg"))).collect(Collectors.toList());
		return functions;
	}
	
	/**
	 * get functions by passing list of function params
	 * @param functionparams
	 * @return list of functions
	 */
	public static List<FunctionParams> getNonAggFunctions(List<FunctionParams> functionparams) {
		List<FunctionParams> functions = functionparams.stream().filter(fp -> fp.getFunctionName() != null
				&& (fp.getFunctionName().equals("abs")
				|| fp.getFunctionName().equals("length")
				|| fp.getFunctionName().equals("round")
				|| fp.getFunctionName().equals("ceil")
				|| fp.getFunctionName().equals("floor")
				|| fp.getFunctionName().equals("pow")
				|| fp.getFunctionName().equals("sqrt")
				|| fp.getFunctionName().equals("exp")
				|| fp.getFunctionName().equals("loge")
				|| fp.getFunctionName().equals("lowercase")
				|| fp.getFunctionName().equals("uppercase")
				|| fp.getFunctionName().equals("base64encode")
				|| fp.getFunctionName().equals("base64decode")
				|| fp.getFunctionName().equals("normalizespaces")
				)).collect(Collectors.toList());
		return functions;
	}
	
	/**
	 * Get Functions with params and grp by columns
	 * @param loForEach
	 * @return list of functions with params
	 * @throws Exception
	 */
	public static List<FunctionParams> getFunctionsWithParamsGrpBy(LOForEach loForEach) throws Exception {
		OperatorPlan forEachInnerPlan = loForEach.getInnerPlan();
		Iterator<Operator> operators = forEachInnerPlan.getOperators();
		List<FunctionParams> functionParams = new ArrayList<>();
		for (;operators.hasNext();) {
			Operator innerOperator = (LogicalRelationalOperator) operators.next();
			if (innerOperator instanceof LOGenerate loGenerate) {
				List<LogicalExpressionPlan> leps = loGenerate.getOutputPlans();
				List<LogicalSchema> outputschemas = loGenerate.getOutputPlanSchemas();
				int schemaindex = 0;
				for (LogicalExpressionPlan lep :leps) {
					Iterator<Operator> funcoper = lep.getOperators();
					FunctionParams param = new FunctionParams();
					param.setAlias(outputschemas.get(schemaindex).getFields().get(0).alias);
					param.setParams((LogicalExpression) lep.getSources().get(0));
					schemaindex++;
					functionParams.add(param);
					while (funcoper.hasNext()) {
						Operator operexp = funcoper.next();	
						if (operexp instanceof UserFuncExpression funcExpression) {
							// Check if this is the function call with your custom function
							if ("org.apache.pig.builtin.COUNT"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("count");
							} else if ("org.apache.pig.builtin.AVG"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("avg");
							} else if ("org.apache.pig.builtin.SUM"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("sum");
							} else if ("com.github.datasamudaya.stream.pig.udf.AbsUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("abs");
							} else if ("com.github.datasamudaya.stream.pig.udf.Base64DecodeUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("base64decode");
							} else if ("com.github.datasamudaya.stream.pig.udf.Base64EncodeUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("base64encode");
							} else if ("com.github.datasamudaya.stream.pig.udf.CeilUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("ceil");
							} else if ("com.github.datasamudaya.stream.pig.udf.ExpUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("exp");
							} else if ("com.github.datasamudaya.stream.pig.udf.FloorUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("floor");
							} else if ("com.github.datasamudaya.stream.pig.udf.LengthUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("length");
							} else if ("com.github.datasamudaya.stream.pig.udf.LogeUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("loge");
							} else if ("com.github.datasamudaya.stream.pig.udf.LowercaseUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("lowercase");
							} else if ("com.github.datasamudaya.stream.pig.udf.NormalizeSpacesUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("normalizespaces");
							} else if ("com.github.datasamudaya.stream.pig.udf.PowerUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("pow");
							} else if ("com.github.datasamudaya.stream.pig.udf.RoundUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("round");
							} else if ("com.github.datasamudaya.stream.pig.udf.SqrtUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("sqrt");
							} else if ("com.github.datasamudaya.stream.pig.udf.UppercaseUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("uppercase");
							}
						}
					}
				}
			}
		}
		return functionParams;
	}
	
	/**
	 * Stores the data to hdfs
	 * @param sp
	 * @param loStore
	 * @throws Exception
	 */
	public static void executeLOStore(StreamPipeline<?> sp, LOStore loStore) throws Exception {		
		sp.map(data -> data).saveAsTextFilePig(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), DataSamudayaConstants.FORWARD_SLASH + loStore.getOutputSpec().getFileName()
				+ DataSamudayaConstants.FORWARD_SLASH + "pig-" + System.currentTimeMillis());
	}
	
	/**
	 * Group by columns
	 * @param sp
	 * @param loCogroup
	 * @return
	 * @throws Exception
	 */
	public static StreamPipeline<Tuple2<Object[], List<Object[]>>> executeLOCoGroup(StreamPipeline<Object[]> sp, LOCogroup loCogroup,
			List<String> columnoralias, List<String> outcols, boolean hasdescendants) throws Exception {
		List<LogicalExpressionPlan> leps = loCogroup.getExpressionPlans().get(0);
		List<String> groupcolumns = new ArrayList<>();
		
		for(LogicalExpressionPlan lep:leps) {
			Iterator<Operator> operators = lep.getOperators();
			while(operators.hasNext()) {
				groupcolumns.add(((ProjectExpression) operators.next()).getColAlias());
			}
		}
		outcols.addAll(groupcolumns);
		String[] groupcols = groupcolumns.toArray(new String[1]);
		return sp.groupBy(map -> {
			Object[] groupmap = new Object[groupcols.length];
			String[] grpcols = groupcols;
			for (String grpcol :grpcols)
				groupmap[columnoralias.indexOf(grpcol)] = map[columnoralias.indexOf(grpcol)];
			return groupmap;
		});
	}
	
	
	/**
	 * Collect the data from the alias
	 * @param sp
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @param pipelineconfig
	 * @return processed data
	 * @throws Exception
	 */
	public static void executeDump(StreamPipeline<?> sp, String user, String jobid, String tejobid, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
		pipelineconfig.setUseglobaltaskexecutors(true);		
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setJobid(jobid);
		sp.map(data -> data).dumpPigResults(true, null);
	}
	
	/**
	 * Collect Results
	 * @param sp
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @param pipelineconfig
	 * @return Results
	 * @throws Exception
	 */
	public static Object executeCollect(LogicalPlan lp, String alias, String user, String jobid, String tejobid, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
		pipelineconfig.setUseglobaltaskexecutors(true);		
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setJobid(jobid);
		Set<String> requiredcolumns = new LinkedHashSet<>();
		Set<String> allcolumns = new LinkedHashSet<>();
		List<String> aliascols = new ArrayList<>();
		StreamPipeline<?> sp = PigQueryExecutor.traversePlan(lp, false, alias, user, jobid, tejobid, pipelineconfig, requiredcolumns, allcolumns, aliascols, alias);
		return sp.map(val->val).collect(true, null);
	}
	
	/**
	 * This function executes latest store pig query.
	 * @param lp
	 * @param alias
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @param pipelineconfig
	 * @throws Exception
	 */
	public static void executeStore(LogicalPlan lp, String alias, String user, String jobid, String tejobid, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
		pipelineconfig.setUseglobaltaskexecutors(true);		
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setJobid(jobid);
		Set<String> requiredcolumns = new LinkedHashSet<>();
		Set<String> allcolumns = new LinkedHashSet<>();
		List<String> aliascols = new ArrayList<>();
		PigQueryExecutor.traversePlan(lp, true, alias, user, jobid, tejobid, pipelineconfig, requiredcolumns, allcolumns, aliascols, alias);
	}
	
	/**
	 * Get Fields from Logical Schema
	 * @param schema
	 * @return array of fields
	 */
	public static String[] getHeaderFromSchema(LogicalSchema schema) {
		List<String> schemafields = schema.getFields().stream().map(loschemafields -> loschemafields.alias).collect(Collectors.toList());
		return schemafields.toArray(new String[1]);
		
	}
	
	/**
	 * Returns Data Types for given shema
	 * @param schema
	 * @return array of data types
	 */
	public static List<SqlTypeName> getTypesFromSchema(LogicalSchema schema) {
		List<SqlTypeName> schemafields = schema.getFields().stream().map(loschemafields -> {
			if (loschemafields.type == 10) {
				return SqlTypeName.INTEGER;
			} else if (loschemafields.type == 55) {
				return SqlTypeName.VARCHAR;
			} 
			return SqlTypeName.VARCHAR;
		}).collect(Collectors.toList());
		return schemafields;
		
	}
	
	/**
	 * Evaluates expression in filter
	 * @param expression
	 * @param row
	 * @return true or false
	 * @throws Exception
	 */
	public static boolean evaluateExpression(LogicalExpression expression, Object[] row, List<String> coloraliasname) throws Exception {
		if (expression instanceof BinaryExpression binaryExpression) {
			String opType = expression.getName();
			LogicalExpression leftExpression = binaryExpression.getLhs();
			LogicalExpression rightExpression = binaryExpression.getRhs();
			String operator = opType;
			
			switch (operator) {
			case "And":
				return evaluateExpression(leftExpression, row, coloraliasname) && evaluateExpression(rightExpression, row, coloraliasname);
			case "Or":
				return evaluateExpression(leftExpression, row , coloraliasname) || evaluateExpression(rightExpression, row, coloraliasname);
			case "GreaterThan":
				Object leftValue = getValueString(leftExpression, row, coloraliasname);
				Object rightValue = getValueString(rightExpression, row, coloraliasname);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "GreaterThanEqual":
				leftValue = getValueString(leftExpression, row, coloraliasname);
				rightValue = getValueString(rightExpression, row, coloraliasname);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "LessThan":
				leftValue = getValueString(leftExpression, row, coloraliasname);
				rightValue = getValueString(rightExpression, row, coloraliasname);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "LessThanEqual":
				leftValue = getValueString(leftExpression, row, coloraliasname);
				rightValue = getValueString(rightExpression, row, coloraliasname);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "Equal":
				leftValue = getValueString(leftExpression, row, coloraliasname);
				rightValue = getValueString(rightExpression, row, coloraliasname);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "NotEqual":
				leftValue = getValueString(leftExpression, row, coloraliasname);
				rightValue = getValueString(rightExpression, row, coloraliasname);
				return evaluatePredicate(leftValue, rightValue, operator);			
			default:
				throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else {
			Object value = getValueString(expression, row, coloraliasname);
			return Boolean.parseBoolean((String) value);
		}
	}
	
	/**
	 * Returns value from expression
	 * @param expression
	 * @param row
	 * @return returns value from expression
	 */
	private static Object getValueString(LogicalExpression expression, Object[] row, List<String> coloralias) {
		if (expression instanceof ConstantExpression constantexpression) {
			return constantexpression.getValue();
		}
		else {
			ProjectExpression column = (ProjectExpression) expression;
			String columnName = column.getColAlias();
			Object value = ((Object[])row[0])[coloralias.indexOf(columnName)];
			if(value instanceof String stringval) {
				return String.valueOf(stringval);
			}
			else if(value instanceof Double doubleval) {
				return doubleval;
			}
			else if(value instanceof Integer intval) {
				return intval;
			} else if(value instanceof Long longval) {
				return longval;
			}else if (value instanceof String stringval && NumberUtils.isParsable(stringval)) {
				return Double.valueOf(stringval);
			}
			return String.valueOf(value);
		}
	}
	
	/**
	 * Evaluates the boolean condition
	 * @param leftvalue
	 * @param rightvalue
	 * @param operator
	 * @return true or false
	 */
	public static boolean evaluatePredicate(Object leftvalue, Object rightvalue, String operator) {
		switch (operator.trim()) {
		case "GreaterThan":
			if(leftvalue instanceof Double lv && rightvalue instanceof Double rv) {
				return lv > rv;
			} else if(leftvalue instanceof Long lv && rightvalue instanceof Double rv) {
				return lv > rv;
			} else if(leftvalue instanceof Double lv && rightvalue instanceof Long rv) {
				return lv > rv;
			} else if(leftvalue instanceof Integer lv && rightvalue instanceof Double rv) {
				return lv > rv;
			} else if(leftvalue instanceof Double lv && rightvalue instanceof Integer rv) {
				return lv > rv;
			} else if(leftvalue instanceof Integer lv && rightvalue instanceof Integer rv) {
				return lv > rv;
			} else if(leftvalue instanceof Integer lv && rightvalue instanceof Long rv) {
				return lv > rv;
			} else if(leftvalue instanceof Long lv && rightvalue instanceof Integer rv) {
				return lv > rv;
			} else if(leftvalue instanceof Long lv && rightvalue instanceof Long rv) {
				return lv > rv;
			} else {
				return false;
			}
		case "GreaterThanEqual":
			if(leftvalue instanceof Double lvgt && rightvalue instanceof Double rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Long lvgt && rightvalue instanceof Double rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Double lvgt && rightvalue instanceof Long rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Integer lvgt && rightvalue instanceof Double rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Double lvgt && rightvalue instanceof Integer rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Integer lvgt && rightvalue instanceof Integer rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Integer lvgt && rightvalue instanceof Long rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Long lvgt && rightvalue instanceof Integer rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Long lvgt && rightvalue instanceof Long rvgt) {
				return lvgt >= rvgt;
			} else {
				return false;
			}
		case "LessThan":
			if(leftvalue instanceof Double lvlt && rightvalue instanceof Double rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Long lvlt && rightvalue instanceof Double rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Double lvlt && rightvalue instanceof Long rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Integer lvlt && rightvalue instanceof Double rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Double lvlt && rightvalue instanceof Integer rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Integer lvlt && rightvalue instanceof Integer rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Integer lvlt && rightvalue instanceof Long rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Long lvlt && rightvalue instanceof Integer rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Long lvlt && rightvalue instanceof Long rvlt) {
				return lvlt < rvlt;
			} else {
				return false;
			}
		case "LessThanEqual":
			if(leftvalue instanceof Double lvle && rightvalue instanceof Double rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Long lvle && rightvalue instanceof Double rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Double lvle && rightvalue instanceof Long rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Integer lvle && rightvalue instanceof Double rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Double lvle && rightvalue instanceof Integer rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Integer lvle && rightvalue instanceof Integer rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Integer lvle && rightvalue instanceof Long rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Long lvle && rightvalue instanceof Integer rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Long lvle && rightvalue instanceof Long rvle) {
				return lvle <= rvle;
			} else {
				return false;
			}
		case "Equal":
			if(leftvalue instanceof Double lveq && rightvalue instanceof Double rveq) {
				return lveq == rveq;
			} else if(leftvalue instanceof Long lveq && rightvalue instanceof Double rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Double lveq && rightvalue instanceof Long rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Integer lveq && rightvalue instanceof Double rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Double lveq && rightvalue instanceof Integer rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Integer lveq && rightvalue instanceof Long rveq) {
				return lveq.longValue() == rveq.longValue();
			} else if(leftvalue instanceof Long lveq && rightvalue instanceof Integer rveq) {
				return lveq.longValue() == rveq.longValue();
			} else if(leftvalue instanceof Integer lveq && rightvalue instanceof Integer rveq) {
				return lveq.intValue() == rveq.intValue();
			} else if(leftvalue instanceof Long lveq && rightvalue instanceof Long rveq) {
				return lveq.longValue() == rveq.longValue();
			} else if(leftvalue instanceof String lveq && rightvalue instanceof String rveq) {
				return lveq.equals(rveq);
			} else {
				return false;
			}
		case "NotEqual":
			if(leftvalue instanceof Double lvne && rightvalue instanceof Double rvne) {
				return lvne != rvne;
			} else if(leftvalue instanceof Long lvne && rightvalue instanceof Double rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Double lvne && rightvalue instanceof Long rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Integer lvne && rightvalue instanceof Double rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Double lvne && rightvalue instanceof Integer rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Long lvne && rightvalue instanceof Integer rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Integer lvne && rightvalue instanceof Long rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Integer lvne && rightvalue instanceof Integer rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Long lvne && rightvalue instanceof Long rvne) {
				return lvne != rvne;
			} else if(leftvalue instanceof String lvne && rightvalue instanceof String rvne) {
				return !lvne.equals(rvne);
			} else {
				return false;
			}
		default:
			throw new UnsupportedOperationException("Unsupported operator: " + operator);
		}
		
	}
	/**
	 * Get Aliases of operator
	 * @param operator
	 * @param aliases
	 * @throws Exception 
	 */
	public static void getAliaseForJoin(Operator operator, List<String> aliases) throws Exception {
		if (operator instanceof LOForEach loForEach) {
			for(LogicalFieldSchema lfs : loForEach.getSchema().getFields()){
				aliases.add(lfs.alias);
			}
		}
	}	
}
