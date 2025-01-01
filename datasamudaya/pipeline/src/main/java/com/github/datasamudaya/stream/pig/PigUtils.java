package com.github.datasamudaya.stream.pig;

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
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.stream.StreamPipeline;
import com.github.datasamudaya.stream.pig.udf.EvalFuncName;
import com.github.datasamudaya.stream.utils.SQLUtils;

/**
 * Utils class for PIG
 * @author Administrator
 *
 */
public class PigUtils {

	private PigUtils() {
	}

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
		if (nonNull(GlobalPigServer.getPigServer())) {
			pigServer = GlobalPigServer.getPigServer();
			pigcontext = pigServer.getPigContext();
		} else {
			Configuration conf = new Configuration();
			pigcontext = new PigContext(LocalExecType.LOCAL, conf);
			pigServer = new PigServer(pigcontext, true);
			FuncSpec funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.AbsUDF", "abs");
			pigServer.registerFunction("abs", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LengthUDF", "length");
			pigServer.registerFunction("length", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RoundUDF", "round");
			pigServer.registerFunction("round", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CeilUDF", "ceil");
			pigServer.registerFunction("ceil", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.FloorUDF", "floor");
			pigServer.registerFunction("floor", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.PowerUDF", "pow");
			pigServer.registerFunction("pow", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.SqrtUDF", "sqrt");
			pigServer.registerFunction("sqrt", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ExpUDF", "exp");
			pigServer.registerFunction("exp", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LogeUDF", "loge");
			pigServer.registerFunction("loge", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LowercaseUDF", "lowercase");
			pigServer.registerFunction("lowercase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.UppercaseUDF", "uppercase");
			pigServer.registerFunction("uppercase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.Base64EncodeUDF", "base64encode");
			pigServer.registerFunction("base64encode", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.Base64DecodeUDF", "base64decode");
			pigServer.registerFunction("base64decode", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.NormalizeSpacesUDF", "normalizespaces");
			pigServer.registerFunction("normalizespaces", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CurrentISODateUDF", "currentisodate");
			pigServer.registerFunction("currentisodate", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RandUDF", "rand");
			pigServer.registerFunction("rand", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ACosUDF", "acos");
			pigServer.registerFunction("acos", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ASinUDF", "asin");
			pigServer.registerFunction("asin", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ATanUDF", "atan");
			pigServer.registerFunction("atan", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.SinUDF", "sin");
			pigServer.registerFunction("sin", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CosUDF", "cos");
			pigServer.registerFunction("cos", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.TanUDF", "tan");
			pigServer.registerFunction("tan", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CosecUDF", "cosec");
			pigServer.registerFunction("cosec", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.SecUDF", "sec");
			pigServer.registerFunction("sec", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CotUDF", "cot");
			pigServer.registerFunction("cot", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CbrtUDF", "cbrt");
			pigServer.registerFunction("cbrt", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.PiiUDF", "pii");
			pigServer.registerFunction("pii", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.DegreesUDF", "degrees");
			pigServer.registerFunction("degrees", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RadiansUDF", "radians");
			pigServer.registerFunction("radians", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.TrimstrUDF", "trimstr");
			pigServer.registerFunction("trimstr", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.SubstringUDF", "substring");
			pigServer.registerFunction("substring", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.OverlayUDF", "overlay");
			pigServer.registerFunction("overlay", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LocateUDF", "locate");
			pigServer.registerFunction("locate", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ConcatUDF", "concat");
			pigServer.registerFunction("concat", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.PositionUDF", "position");
			pigServer.registerFunction("position", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.InitcapUDF", "initcap");
			pigServer.registerFunction("initcap", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.AsciiUDF", "ascii");
			pigServer.registerFunction("ascii", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CharacterUDF", "character");
			pigServer.registerFunction("character", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.InsertstrUDF", "insertstr");
			pigServer.registerFunction("insertstr", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LeftcharsUDF", "leftchars");
			pigServer.registerFunction("leftchars", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RightcharsUDF", "rightchars");
			pigServer.registerFunction("rightchars", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ReverseUDF", "reverse");
			pigServer.registerFunction("reverse", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LtrimUDF", "ltrim");
			pigServer.registerFunction("ltrim", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RtrimUDF", "rtrim");
			pigServer.registerFunction("rtrim", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.NowUDF", "now");
			pigServer.registerFunction("now", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.YearUDF", "year");
			pigServer.registerFunction("year", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.MonthUDF", "month");
			pigServer.registerFunction("month", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.DayUDF", "day");
			pigServer.registerFunction("day", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.IndexOfUDF", "indexof");
			pigServer.registerFunction("indexof", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.IndexOfStartPosUDF", "indexofstartpos");
			pigServer.registerFunction("indexofstartpos", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.IndexOfAnyUDF", "indexofany");
			pigServer.registerFunction("indexofany", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.IndexOfAnyButUDF", "indexofanybut");
			pigServer.registerFunction("indexofanybut", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.IndexOfDiffUDF", "indexofdiff");
			pigServer.registerFunction("indexofdiff", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.IndexOfIgnoreCaseUDF", "indexofignorecase");
			pigServer.registerFunction("indexofignorecase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.IndexOfIgnoreCaseStartPosUDF", "indexofignorecasestartpos");
			pigServer.registerFunction("indexofignorecasestartpos", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LastIndexOfUDF", "lastindexof");
			pigServer.registerFunction("lastindexof", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LastIndexOfStartPosUDF", "lastindexofstartpos");
			pigServer.registerFunction("lastindexofstartpos", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LeftPadUDF", "leftpad");
			pigServer.registerFunction("leftpad", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.LeftPadStringUDF", "leftpadstring");
			pigServer.registerFunction("leftpadstring", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RemoveUDF", "remove");
			pigServer.registerFunction("remove", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RemoveEndUDF", "removeend");
			pigServer.registerFunction("removeend", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RemoveEndIgnoreCaseUDF", "removeendignorecase");
			pigServer.registerFunction("removeendignorecase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RemoveIgnoreCaseUDF", "removeignorecase");
			pigServer.registerFunction("removeignorecase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RemoveStartUDF", "removestart");
			pigServer.registerFunction("removestart", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RemoveStartIgnoreCaseUDF", "removestartignorecase");
			pigServer.registerFunction("removestartignorecase", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RepeatUDF", "repeat");
			pigServer.registerFunction("repeat", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RepeatSeparatorUDF", "repeatseparator");
			pigServer.registerFunction("repeatseparator", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.ChopUDF", "chop");
			pigServer.registerFunction("chop", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.GetDigitsUDF", "getdigits");
			pigServer.registerFunction("getdigits", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RightPadUDF", "rightpad");
			pigServer.registerFunction("rightpad", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RightPadStringUDF", "rightpadstring");
			pigServer.registerFunction("rightpadstring", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.RotateUDF", "rotate");
			pigServer.registerFunction("rotate", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.WrapUDF", "wrap");
			pigServer.registerFunction("wrap", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.WrapIfMissingUDF", "wrapifmissing");
			pigServer.registerFunction("wrapifmissing", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.UnWrapUDF", "unwrap");
			pigServer.registerFunction("unwrap", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.UncapitalizeUDF", "uncapitalize");
			pigServer.registerFunction("uncapitalize", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.SumUDF", "csum");
			pigServer.registerFunction("csum", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.CountUDF", "ccount");
			pigServer.registerFunction("ccount", funcSpec);
			funcSpec = new FuncSpec("com.github.datasamudaya.stream.pig.udf.AvgUDF", "cavg");
			pigServer.registerFunction("cavg", funcSpec);
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
		} catch (Exception ex) {
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
				pipelineconfig, headers, schematypes, Arrays.asList(headers), null);
	}

	/**
	 * Convert value to for a given data type
	 * @param value
	 * @param type
	 * @return converted value
	 */
	public static Object getValue(String value, Class<?> type) {
		try {
			if (type == Integer.class) {
				return Integer.valueOf(value);
			} else if (type == Long.class) {
				return Long.valueOf(value);
			} else if (type == String.class) {
				return String.valueOf(value);
			} else if (type == Float.class) {
				return Float.valueOf(value);
			} else if (type == Double.class) {
				return Double.valueOf(value);
			} else {
				return String.valueOf(value);
			}
		} catch (Exception ex) {
			if (type == Integer.class) {
				return Integer.valueOf(0);
			} else if (type == Long.class) {
				return Long.valueOf(0l);
			} else if (type == String.class) {
				return String.valueOf(0);
			} else if (type == Float.class) {
				return Float.valueOf(0.0f);
			} else if (type == Double.class) {
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
		if (!hasdescendants) {
			return sp.map(obj -> ((Object[]) obj[0]));
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
		for (LogicalExpressionPlan lep :leps) {
			ProjectExpression projectexpression = (ProjectExpression) lep.getOperators().next();
			SortOrderColumns soc = new SortOrderColumns();
			soc.setColumn(aliasorcolumns.indexOf(projectexpression.getColAlias()));
			soc.setIsasc(asccolumns.next());
			sortordercolumns.add(soc);
		}
		sp = sp.sorted(new PigSortedComparator(sortordercolumns));
		if (!hasdescendants) {
			return sp.map(obj -> ((Object[]) obj[0]));
		}
		return sp;
	}

	/**
	 * Execute Distinct values 
	 * @param sp
	 * @return distinct values
	 * @throws Exception
	 */
	public static StreamPipeline<Object[]> executeLODistinct(StreamPipeline<Object[]> spparent, boolean hasdescendants) throws Exception {

		StreamPipeline<Object[]> spdistinct = spparent
				.map(new MapFunction<Object[], List<Object>>(){
					private static final long serialVersionUID = 4839257897910999653L;

					@Override
					public List<Object> apply(Object[] distobj) {
						return Arrays.asList(((Object[]) distobj[0]));
					}
				}).distinct()
				.mapToPair(new MapToPairFunction<List<Object>, Tuple2<List<Object>, Double>>() {
					private static final long serialVersionUID = -6412672309048067129L;

					@Override
					public Tuple2<List<Object>, Double> apply(List<Object> record) {
						return new Tuple2<>(record, 1.0d);
					}

				}).reduceByKey(new ReduceByKeyFunction<Double>() {
			private static final long serialVersionUID = -2395505885613892042L;

			@Override
			public Double apply(Double t, Double u) {
				return t + u;
			}

		}).coalesce(1, new CoalesceFunction<Double>() {
			private static final long serialVersionUID = 8667714333390649847L;

			@Override
			public Double apply(Double t, Double u) {
				return t + u;
			}

		}).map(new MapFunction<Tuple2<List<Object>, Double>, List<Object>>() {
			private static final long serialVersionUID = -7888406734785506543L;

			@Override
			public List<Object> apply(Tuple2<List<Object>, Double> tup2) {
				return tup2.v1();
			}

		}).distinct()
				.map(new MapFunction<List<Object>, Object[]>() {
					private static final long serialVersionUID = 5319711801798549984L;

					@Override
					public Object[] apply(List<Object> list) {
						Object[] objdistinct = new Object[2];
						objdistinct[0] = list.toArray(new Object[0]);
						objdistinct[1] = new Object[list.size()];
						for (int index = 0;index < list.size();index++) {
							((Object[]) objdistinct[1])[index] = true;
						}
						return objdistinct;
					}

				});
		if (!hasdescendants) {
			return spdistinct.map(obj -> ((Object[]) obj[0]));
		}
		return spdistinct;
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
					Object leftvalue = ((Object[]) rowleft[0])[alileft.indexOf(leftcol)];
					Object rightvalue = ((Object[]) rowright[0])[aliright.indexOf(rightcol)];
					if (leftvalue == null && rightvalue == null || nonNull(leftvalue) && nonNull(rightvalue) && !leftvalue.equals(rightvalue)) {
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
				return new Object[]{concatenate(((Object[]) tup2.v1()[0]), ((Object[]) tup2.v2()[0])),
						concatenate(((Object[]) tup2.v1()[1]), ((Object[]) tup2.v2()[1]))};
			}
		});

		if (!hasdescendants) {
			return sp.map(obj -> ((Object[]) obj[0]));
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
		if (expression instanceof BinaryExpression bex) {
			String operator = expression.getName();
			LogicalExpression leftExpression = bex.getLhs();
			LogicalExpression rightExpression = bex.getRhs();
			boolean leftValue = false;
			boolean rightValue = false;
			if (leftExpression instanceof UserFuncExpression fn) {
				leftValue = toEvaluateBinaryExpression(leftExpression, row, aliasorcolname);
			} else if (leftExpression instanceof ConstantExpression lv) {
				leftValue = true;
			} else if (leftExpression instanceof ProjectExpression pex) {
				String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[]) row[1])[aliasorcolname.indexOf(columnName)];
				leftValue = (boolean) value;
			} else if (leftExpression instanceof BinaryExpression) {
				leftValue = toEvaluateBinaryExpression(leftExpression, row, aliasorcolname);
			}
			if (rightExpression instanceof UserFuncExpression fn) {
				rightValue = toEvaluateBinaryExpression(rightExpression, row, aliasorcolname);
			} else if (rightExpression instanceof ConstantExpression lv) {
				rightValue = true;
			} else if (rightExpression instanceof ProjectExpression pex) {
				String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[]) row[1])[aliasorcolname.indexOf(columnName)];
				rightValue = (boolean) value;
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
		} else if (expression instanceof ConstantExpression lv) {
			return true;
		} else if (expression instanceof ProjectExpression pex) {
			String columnName = pex.getFieldSchema().alias;
			return (boolean) ((Object[]) row[1])[aliasorcolname.indexOf(columnName)];
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

		outcols.addAll(functionparams.stream().map(fp -> fp.getAlias()).toList());

		Set<String> grpbyheader = new LinkedHashSet<>();

		if (nonNull(headers)) {
			List<String> columns = new ArrayList<>();
			for (LogicalExpression lex :headers) {
				getColumnsFromExpressions(lex, columns);
				grpbyheader.addAll(columns);
				columns.clear();
			}
		}

		List<String> aliases = getAlias(functionparams);

		List<FunctionParams> aggfunctions = getAggFunctions(functionparams);
		List<FunctionParams> nonaggfunctions = getNonAggFunctions(functionparams);
		if (CollectionUtils.isEmpty(aggfunctions) && CollectionUtils.isEmpty(nonaggfunctions)) {
			sp = sp.filter(new PredicateSerializable<Object[]>() {
				private static final long serialVersionUID = 1042338514393215380L;
				List<String> aliascolumns = aliasorcolumns;
				LogicalExpression[] headera = lexp;

				public boolean test(Object[] obj) {
					try {
						boolean toevaluateexpression = true;
						for (LogicalExpression exp : headera) {
							toevaluateexpression = toevaluateexpression
									&& toEvaluateBinaryExpression(exp, obj, aliascolumns);
							if (!toevaluateexpression) {
								break;
							}
						}
						return toevaluateexpression;
					} catch (Exception ex) {
						return false;
					}
				}
			}).map(new MapFunction<Object[], Object[]>() {
				private static final long serialVersionUID = -2439404591638356925L;
				LogicalExpression[] lexpression = lexp;
				List<String> aliascolumns = aliasorcolumns;

				@Override
				public Object[] apply(Object[] obj) {
					Object[] formattedvalues = new Object[lexpression.length];
					Object[] formattedvaluestoconsider = new Object[lexpression.length];
					try {
						LogicalExpression[] headera = lexpression;
						int indexformatted = 0;
						for (LogicalExpression exp : headera) {
							formattedvalues[indexformatted] = evaluateBinaryExpression(exp, obj, aliascolumns);
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
				}
			});
			if (!hasdescendants) {
				return sp.map(obj -> ((Object[]) obj[0]));
			}
			return sp;
		} else {

			StreamPipeline<Object[]> pipelinemap = sp;
			if (!CollectionUtils.isEmpty(nonaggfunctions)) {
				pipelinemap = pipelinemap.map(new MapFunction<Object[], Object[]>() {
					private static final long serialVersionUID = 6329566708048046421L;
					List<FunctionParams> nonagg = new ArrayList<>(nonaggfunctions);
					LogicalExpression[] grpby = headers;
					List<String> aliasorcols = aliasorcolumns;

					@Override
					public Object[] apply(Object[] mapvalues) {
						Object[] nonaggfnvalues = new Object[2];
						Object[] grpbyfnvalues = new Object[(nonNull(grpby) ? grpby.length : 0) + nonagg.size()];
						Object[] valuestoconsider = new Object[(nonNull(grpby) ? grpby.length : 0) + +nonagg.size()];
						int index = 0;
						if (nonNull(grpby) && grpby.length > 0) {
							for (LogicalExpression grpobj : grpby) {
								try {
									grpbyfnvalues[index] = evaluateBinaryExpression(grpobj, mapvalues, aliasorcols);
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
								value = evaluateBinaryExpression(fn.getParams(), mapvalues, aliasorcols);
								valuestoconsider[index] = true;
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
							grpbyfnvalues[index] = value;
							index++;
						}
						nonaggfnvalues[0] = grpbyfnvalues;
						nonaggfnvalues[1] = valuestoconsider;
						return nonaggfnvalues;

					}
				});
			}
			if (!CollectionUtils.isEmpty(aggfunctions)) {
				List<List<String>> columnstoeval = new ArrayList<>();
				for (FunctionParams fn : aggfunctions) {
					LogicalExpression expression = fn.getParams();
					if (nonNull(expression)) {
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
					List<String> aliascolumns = aliasorcolumns;

					@Override
					public Tuple2<Tuple, Tuple> apply(Object[] mapvalues) {
						List<Object> fnobj = new ArrayList<>();
						Object[] grpbyobj = null;

						int index = 0;
						if (nonNull(grpby) && grpby.length > 0) {
							grpbyobj = new Object[grpby.length];
							for (LogicalExpression grpobj : grpby) {
								try {
									grpbyobj[index] = evaluateBinaryExpression(grpobj, mapvalues, aliascolumns);
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
							if (functionParam.getFunctionName().equals("ccount")) {
								fnobj.add(1);
							} else {
								try {
									fnobj.add(evaluateBinaryExpression(functionParam.getParams(), mapvalues, aliascolumns));
									long cval = 1;
									if (functionParam.getFunctionName().startsWith("cavg")) {
										for (String column : columnsevaluation.get(index)) {
											boolean valuetocount = (boolean) ((Object[]) mapvalues[1])[aliascolumns.indexOf(column)];
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
					List<String> alias = aliases;

					@Override
					public Object[] apply(Tuple2<Tuple, Tuple> tuple2) {
						Object[] valueobject = new Object[2];
						valueobject[0] = new Object[alias.size()];
						valueobject[1] = new Object[alias.size()];
						for (int count = 0;count < alias.size();count++) {
							((Object[]) valueobject[1])[count] = true;
						}
						try {
							populateGroupByFunction((Object[]) valueobject[0], tuple2.v1, grpby, alias);
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
						populateMapFromFunctions((Object[]) valueobject[0], tuple2.v2, functionParam, alias);
						return valueobject;
					}
				});
			}
			if (!hasdescendants) {
				return pipelinemap.map(obj -> ((Object[]) obj[0]));
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
		for (int grpindex = 1;grpindex <= grpby.size();grpindex++) {
			java.lang.reflect.Method method = cls.getMethod("v" + grpindex);
			Object valuesum = method.invoke(grpbyvalues);
			valueobject[aliases.indexOf(grpby.get(grpindex - 1))] = valuesum;
		}
	}

	/**
	 * Evaluates Binary Expression.
	 * @param expression
	 * @param row
	 * @param aliasorcolname
	 * @return evaluated value
	 * @throws Exception
	 */
	public static Object evaluateBinaryExpression(LogicalExpression expression, Object[] row, List<String> aliasorcolname) throws Exception {
		if (expression instanceof UserFuncExpression fn) {
			EvalFuncName evalfunname = (EvalFuncName) fn.getEvalFunc();
			String name = evalfunname.getName();
			switch (name) {
				case "csum":
					Object[] values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)}; 
					// Get the absolute value of the first parameter	               
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "ccount":
					// Get the absolute value of the first parameter
					return evaluateFunctionsWithType(row, name, fn.getEvalFunc());
				case "cavg":
					// Get the absolute value of the first parameter	               
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "abs":
					// Get the absolute value of the first parameter	               
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values,name, fn.getEvalFunc());
				case "length":
					// Get the length of string value	                
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "round":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "ceil":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "floor":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());				
				case "sqrt":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "exp":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "loge":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "lowercase":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "uppercase":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "base64encode":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "base64decode":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "normalizespaces":
					// Get the absolute value of the first parameter
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname)};
					return evaluateFunctionsWithType(values, "normalizespaces", fn.getEvalFunc());
				case "pow","concat", "leftchars", "rightchars", "indexof":
				case "indexofany", "indexofanybut", "indexofdiff", "indexofignorecase":
				case "leftpad", "leftpadstring", "remove", "removeend", "removeendignorecase":
				case "removeignorecase", "removestart", "removestartignorecase", "repeat":
				case "rightpad", "rotate", "wrap", "wrapifmissing", "unwrap":
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname),
							evaluateBinaryExpression(fn.getArguments().get(1), row, aliasorcolname)};
					// Get the absolute value of the first parameter
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "substring", "position", "indexofstartpos", "indexofignorecasestartpos":
				case "rightpadstring", "repeatseparator":
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname),
							evaluateBinaryExpression(fn.getArguments().get(1), row, aliasorcolname),
							evaluateBinaryExpression(fn.getArguments().get(2), row, aliasorcolname)};
					// Get the absolute value of the first parameter
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "overlay", "locate", "insertstr":
					values = new Object[] {evaluateBinaryExpression(fn.getArguments().get(0), row, aliasorcolname),
							evaluateBinaryExpression(fn.getArguments().get(1), row, aliasorcolname),
							evaluateBinaryExpression(fn.getArguments().get(2), row, aliasorcolname),
							evaluateBinaryExpression(fn.getArguments().get(3), row, aliasorcolname)};
					// Get the absolute value of the first parameter
					return evaluateFunctionsWithType(values, name, fn.getEvalFunc());
				case "currentisodate", "rand", "pii", "now":
					return evaluateFunctionsWithType(null, name, fn.getEvalFunc());
			}
		} else if (expression instanceof BinaryExpression bex) {
			String operator = expression.getName();
			LogicalExpression leftExpression = bex.getLhs();
			LogicalExpression rightExpression = bex.getRhs();
			Object leftValue = null;
			Object rightValue = null;
			if (leftExpression instanceof UserFuncExpression fn) {
				leftValue = evaluateBinaryExpression(leftExpression, row, aliasorcolname);
			} else if (leftExpression instanceof ConstantExpression lv) {
				leftValue = lv.getValue();
			} else if (leftExpression instanceof ProjectExpression pex) {
				String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[]) row[0])[aliasorcolname.indexOf(columnName)];
				leftValue = value;
			} else if (leftExpression instanceof BinaryExpression) {
				leftValue = evaluateBinaryExpression(leftExpression, row, aliasorcolname);
			}
			if (rightExpression instanceof UserFuncExpression fn) {
				rightValue = evaluateBinaryExpression(rightExpression, row, aliasorcolname);
			} else if (rightExpression instanceof ConstantExpression lv) {
				rightValue = lv.getValue();
			} else if (rightExpression instanceof ProjectExpression pex) {
				String columnName = pex.getFieldSchema().alias;
				Object value = ((Object[]) row[0])[aliasorcolname.indexOf(columnName)];
				rightValue = value;
			} else if (rightExpression instanceof BinaryExpression) {
				rightValue = evaluateBinaryExpression(rightExpression, row, aliasorcolname);
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
		} else if (expression instanceof ConstantExpression lv) {
			return lv.getValue();
		} else if (expression instanceof ProjectExpression pex) {
			String columnName = pex.getFieldSchema().alias;
			return ((Object[]) row[0])[aliasorcolname.indexOf(columnName)];
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
		if (lexp instanceof BinaryExpression bex) {
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
		} else if (lexp instanceof ProjectExpression pex) {
			columns.add(pex.getFieldSchema().alias);
		} else if (lexp instanceof UserFuncExpression userfuncexp) {
			if (!"org.apache.pig.builtin.COUNT"
					.equals(userfuncexp.getFuncSpec().getClassName())) {
				Iterator<Operator> operators = lexp.getPlan().getOperators();
				for (;operators.hasNext();) {
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
	public static Object evaluateFunctionsWithType(Object[] value, String name, EvalFunc<?> evalfunc) throws Exception {
		switch (name) {
		case "csum":
		case "cavg":
		case "ccount":
			return value[0];
		case "abs":
		case "length":
		case "round":
		case "ceil":
		case "floor":
		case "sqrt":
		case "exp":
		case "loge":
		case "lowercase":
		case "uppercase":
		case "base64encode":
		case "base64decode":
		case "normalizespaces":
		case "acos", "asin", "atan":
		case "cos", "sin", "tan", "cosec", "sec", "cot":
		case "cbrt":
		case "degrees", "radians":
		case "trimstr":
		case "initcap", "ascii", "character":
		case "reverse":
		case "ltrim", "rtrim":
		case "year", "month", "day":
		case "getdigits", "chop", "uncapitalize":
			// Get the absolute value of the first parameter
			org.apache.pig.data.Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(value[0]));
			return evalfunc.exec(tuple);
		case "pow", "concat", "leftchars", "rightchars", "indexof":
		case "indexofany", "indexofanybut", "indexofdiff", "indexofignorecase":
		case "leftpad", "leftpadstring", "remove", "removeend", "removeendignorecase":
		case "removeignorecase", "removestart", "removestartignorecase", "repeat":
		case "rightpad", "rotate", "wrap", "wrapifmissing", "unwrap":	
			// Get the absolute value of the first parameter
			tuple = TupleFactory.getInstance().newTuple(Arrays.asList(value[0], value[1]));
			return evalfunc.exec(tuple);
		case "substring", "position", "indexofstartpos", "indexofignorecasestartpos":
		case "rightpadstring", "repeatseparator":	
			// Get the absolute value of the first parameter
			tuple = TupleFactory.getInstance().newTuple(Arrays.asList(value[0], value[1], value[2]));
			return evalfunc.exec(tuple);
		case "overlay", "locate", "insertstr":
			// Get the absolute value of the first parameter
			tuple = TupleFactory.getInstance().newTuple(Arrays.asList(value[0], value[1], value[2], value[3]));
			return evalfunc.exec(tuple);
		case "currentisodate", "rand", "pii", "now":
			tuple = TupleFactory.getInstance().newTuple(null);
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
		if (tuple instanceof Tuple1 tup) {
			return String.valueOf(tup.v1);
		} else if (tuple instanceof Tuple2 tup) {
			return String.valueOf(tup.v2);
		} else if (tuple instanceof Tuple3 tup) {
			return String.valueOf(tup.v3);
		} else if (tuple instanceof Tuple4 tup) {
			return String.valueOf(tup.v4);
		} else if (tuple instanceof Tuple5 tup) {
			return String.valueOf(tup.v5);
		} else if (tuple instanceof Tuple6 tup) {
			return String.valueOf(tup.v6);
		} else if (tuple instanceof Tuple7 tup) {
			return String.valueOf(tup.v7);
		} else if (tuple instanceof Tuple8 tup) {
			return String.valueOf(tup.v8);
		} else if (tuple instanceof Tuple9 tup) {
			return String.valueOf(tup.v9);
		} else if (tuple instanceof Tuple10 tup) {
			return String.valueOf(tup.v10);
		} else if (tuple instanceof Tuple11 tup) {
			return String.valueOf(tup.v11);
		} else if (tuple instanceof Tuple12 tup) {
			return String.valueOf(tup.v12);
		} else if (tuple instanceof Tuple13 tup) {
			return String.valueOf(tup.v13);
		} else if (tuple instanceof Tuple14 tup) {
			return String.valueOf(tup.v14);
		} else if (tuple instanceof Tuple15 tup) {
			return String.valueOf(tup.v15);
		} else if (tuple instanceof Tuple16 tup) {
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
			for (int funcindex = 0,valueindex = 1;funcindex < functions.size();funcindex++) {
				FunctionParams func = functions.get(funcindex);
				String funname = func.getFunctionName();
				if (funname.toLowerCase().startsWith("cavg")) {
					java.lang.reflect.Method method = cls.getMethod("v" + valueindex);
					Object valuesum = method.invoke(tuple);
					valueindex++;
					method = cls.getMethod("v" + valueindex);
					Object valuecount = method.invoke(tuple);
					values[aliasorcolname.indexOf(getAliasForFunction(func))] = evaluateValuesByOperator(valuesum, valuecount, "/");
				} else {
					java.lang.reflect.Method method = cls.getMethod("v" + valueindex);
					Object value = method.invoke(tuple);
					values[aliasorcolname.indexOf(getAliasForFunction(func))] = value;
				}
				valueindex++;
			}
		} catch (Exception ex) {
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
				field.set(result, evaluateFunction(value1, value2, func));
				// Set the sum of the values in the result tuple                
				if (index + 1 < aggfunctions.size()) {
					if (avgindex) {
						func = aggfunctions.get(index);
						avgindex = false;
						index++;
					} else if (!aggfunctions.get(index).getFunctionName().equalsIgnoreCase("cavg")) {
						func = aggfunctions.get(index + 1);
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
		if (functionname.startsWith("ccount") || functionname.startsWith("csum") || functionname.startsWith("cavg")) {
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
		if (headersl.size() > 0) {
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
		if (headersl.size() > 0) {
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
				&& (fp.getFunctionName().equals("csum")
				|| fp.getFunctionName().equals("ccount")
				|| fp.getFunctionName().equals("cavg"))).collect(Collectors.toList());
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
				|| fp.getFunctionName().equals("concat")
				|| fp.getFunctionName().equals("leftchars")
				|| fp.getFunctionName().equals("rightchars")
				|| fp.getFunctionName().equals("indexof")
				|| fp.getFunctionName().equals("indexofany")
				|| fp.getFunctionName().equals("indexofanybut")
				|| fp.getFunctionName().equals("indexofdiff")
				|| fp.getFunctionName().equals("indexofignorecase")
				|| fp.getFunctionName().equals("leftpad")
				|| fp.getFunctionName().equals("leftpadstring")
				|| fp.getFunctionName().equals("remove")
				|| fp.getFunctionName().equals("removeend")
				|| fp.getFunctionName().equals("removeendignorecase")
				|| fp.getFunctionName().equals("removeignorecase")
				|| fp.getFunctionName().equals("removestart")
				|| fp.getFunctionName().equals("removestartignorecase")
				|| fp.getFunctionName().equals("normalizespaces")
				|| fp.getFunctionName().equals("repeat")
				|| fp.getFunctionName().equals("rightpad")
				|| fp.getFunctionName().equals("rotate")
				|| fp.getFunctionName().equals("wrap")
				|| fp.getFunctionName().equals("wrapifmissing")
				|| fp.getFunctionName().equals("unwrap")
				|| fp.getFunctionName().equals("substring")
				|| fp.getFunctionName().equals("position")
				|| fp.getFunctionName().equals("indexofstartpos")
				|| fp.getFunctionName().equals("indexofignorecasestartpos")
				|| fp.getFunctionName().equals("rightpadstring")
				|| fp.getFunctionName().equals("repeatseparator")
				|| fp.getFunctionName().equals("overlay")
				|| fp.getFunctionName().equals("locate")
				|| fp.getFunctionName().equals("insertstr")
				|| fp.getFunctionName().equals("currentisodate")
				|| fp.getFunctionName().equals("rand")
				|| fp.getFunctionName().equals("pii")
				|| fp.getFunctionName().equals("now")
				|| fp.getFunctionName().equals("acos")
				|| fp.getFunctionName().equals("asin")
				|| fp.getFunctionName().equals("atan")
				|| fp.getFunctionName().equals("cos")
				|| fp.getFunctionName().equals("sin")
				|| fp.getFunctionName().equals("tan")
				|| fp.getFunctionName().equals("cosec")
				|| fp.getFunctionName().equals("sec")
				|| fp.getFunctionName().equals("cot")
				|| fp.getFunctionName().equals("cbrt")
				|| fp.getFunctionName().equals("degrees")
				|| fp.getFunctionName().equals("radians")
				|| fp.getFunctionName().equals("trimstr")
				|| fp.getFunctionName().equals("initcap")
				|| fp.getFunctionName().equals("ascii")
				|| fp.getFunctionName().equals("character")
				|| fp.getFunctionName().equals("reverse")
				|| fp.getFunctionName().equals("ltrim")
				|| fp.getFunctionName().equals("rtrim")
				|| fp.getFunctionName().equals("year")
				|| fp.getFunctionName().equals("month")
				|| fp.getFunctionName().equals("day")
				|| fp.getFunctionName().equals("getdigits")
				|| fp.getFunctionName().equals("chop")
				|| fp.getFunctionName().equals("uncapitalize")
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
							if ("com.github.datasamudaya.stream.pig.udf.CountUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("ccount");
							} else if ("com.github.datasamudaya.stream.pig.udf.AvgUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("cavg");
							} else if ("com.github.datasamudaya.stream.pig.udf.SumUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("csum");
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
							} else if ("com.github.datasamudaya.stream.pig.udf.CurrentISODateUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("currentisodate");
							} else if ("com.github.datasamudaya.stream.pig.udf.RandUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("rand");
							} else if ("com.github.datasamudaya.stream.pig.udf.ACosUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("acos");
							} else if ("com.github.datasamudaya.stream.pig.udf.ASinUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("asin");
							} else if ("com.github.datasamudaya.stream.pig.udf.ATanUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("atan");
							} else if ("com.github.datasamudaya.stream.pig.udf.CosUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("cos");
							} else if ("com.github.datasamudaya.stream.pig.udf.SinUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("sin");
							} else if ("com.github.datasamudaya.stream.pig.udf.TanUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("tan");
							} else if ("com.github.datasamudaya.stream.pig.udf.CosecUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("cosec");
							} else if ("com.github.datasamudaya.stream.pig.udf.SecUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("sec");
							} else if ("com.github.datasamudaya.stream.pig.udf.CotUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("cot");
							} else if ("com.github.datasamudaya.stream.pig.udf.CbrtUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("cbrt");
							} else if ("com.github.datasamudaya.stream.pig.udf.PiiUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("pii");
							} else if ("com.github.datasamudaya.stream.pig.udf.DegreesUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("degrees");
							} else if ("com.github.datasamudaya.stream.pig.udf.RadiansUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("radians");
							} else if ("com.github.datasamudaya.stream.pig.udf.TrimstrUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("trimstr");
							} else if ("com.github.datasamudaya.stream.pig.udf.SubstringUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("substring");
							} else if ("com.github.datasamudaya.stream.pig.udf.OverlayUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("overlay");
							} else if ("com.github.datasamudaya.stream.pig.udf.LocateUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("locate");
							} else if ("com.github.datasamudaya.stream.pig.udf.ConcatUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("concat");
							} else if ("com.github.datasamudaya.stream.pig.udf.PositionUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("position");
							} else if ("com.github.datasamudaya.stream.pig.udf.InitcapUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("initcap");
							} else if ("com.github.datasamudaya.stream.pig.udf.AsciiUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("ascii");
							} else if ("com.github.datasamudaya.stream.pig.udf.CharacterUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("charac");
							} else if ("com.github.datasamudaya.stream.pig.udf.InsertstrUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("insertstr");
							} else if ("com.github.datasamudaya.stream.pig.udf.LeftcharsUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("leftchars");
							} else if ("com.github.datasamudaya.stream.pig.udf.RightcharsUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("rightchars");
							} else if ("com.github.datasamudaya.stream.pig.udf.ReverseUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("reverse");
							} else if ("com.github.datasamudaya.stream.pig.udf.LtrimUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("ltrim");
							} else if ("com.github.datasamudaya.stream.pig.udf.RtrimUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("rtrim");
							} else if ("com.github.datasamudaya.stream.pig.udf.NowUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("now");
							} else if ("com.github.datasamudaya.stream.pig.udf.YearUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("year");
							} else if ("com.github.datasamudaya.stream.pig.udf.MonthUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("month");
							} else if ("com.github.datasamudaya.stream.pig.udf.DayUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("day");
							} else if ("com.github.datasamudaya.stream.pig.udf.IndexOfUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("indexof");
							} else if ("com.github.datasamudaya.stream.pig.udf.IndexOfStartPosUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("indexofstartpos");
							} else if ("com.github.datasamudaya.stream.pig.udf.IndexOfAnyUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("indexofany");
							} else if ("com.github.datasamudaya.stream.pig.udf.IndexOfAnyButUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("indexofanybut");
							} else if ("com.github.datasamudaya.stream.pig.udf.IndexOfDiffUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("indexofdiff");
							} else if ("com.github.datasamudaya.stream.pig.udf.IndexOfIgnoreCaseUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("indexofignorecase");
							} else if ("com.github.datasamudaya.stream.pig.udf.IndexOfIgnoreCaseStartPosUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("indexofignorecasestartpos");
							} else if ("com.github.datasamudaya.stream.pig.udf.LastIndexOfUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("lastindexof");
							} else if ("com.github.datasamudaya.stream.pig.udf.LastIndexOfStartPosUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("lastindexofstartpos");
							} else if ("com.github.datasamudaya.stream.pig.udf.LeftPadUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("leftpad");
							} else if ("com.github.datasamudaya.stream.pig.udf.LeftPadStringUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("leftpadstr");
							} else if ("com.github.datasamudaya.stream.pig.udf.RemoveUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("remove");
							} else if ("com.github.datasamudaya.stream.pig.udf.RemoveEndUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("removeend");
							} else if ("com.github.datasamudaya.stream.pig.udf.RemoveEndIgnoreCaseUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("removeendignorecase");
							} else if ("com.github.datasamudaya.stream.pig.udf.RemoveIgnoreCaseUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("removeignorecase");
							} else if ("com.github.datasamudaya.stream.pig.udf.RemoveStartUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("removestart");
							} else if ("com.github.datasamudaya.stream.pig.udf.RemoveStartIgnoreCaseUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("removestartignorecase");
							} else if ("com.github.datasamudaya.stream.pig.udf.RepeatUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("repeat");
							} else if ("com.github.datasamudaya.stream.pig.udf.RepeatSeparatorUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("repeatseparator");
							} else if ("com.github.datasamudaya.stream.pig.udf.ChopUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("chop");
							} else if ("com.github.datasamudaya.stream.pig.udf.GetDigitsUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("getdigits");
							} else if ("com.github.datasamudaya.stream.pig.udf.RightPadUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("rightpad");
							} else if ("com.github.datasamudaya.stream.pig.udf.RightPadStringUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("rightpadstring");
							} else if ("com.github.datasamudaya.stream.pig.udf.RotateUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("rotate");
							} else if ("com.github.datasamudaya.stream.pig.udf.WrapUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("wrap");
							} else if ("com.github.datasamudaya.stream.pig.udf.WrapIfMissingUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("wrapifmissing");
							} else if ("com.github.datasamudaya.stream.pig.udf.UnWrapUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("unwrap");
							} else if ("com.github.datasamudaya.stream.pig.udf.UncapitalizeUDF"
									.equals(funcExpression.getFuncSpec().getClassName())) {
								param.setFunctionName("uncapitalize");
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

		for (LogicalExpressionPlan lep :leps) {
			Iterator<Operator> operators = lep.getOperators();
			while (operators.hasNext()) {
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
		sp.map(new MapFunction() {
			@Override
			public Object apply(Object obj) {
				return obj;
			}

		}).dumpPigResults(true, null);
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
		return sp.map(val -> val).collect(true, null);
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
					return evaluateExpression(leftExpression, row, coloraliasname) || evaluateExpression(rightExpression, row, coloraliasname);
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
	public static Object getValueString(LogicalExpression expression, Object[] row, List<String> coloralias) {
		if (expression instanceof ConstantExpression constantexpression) {
			return constantexpression.getValue();
		} else {
			ProjectExpression column = (ProjectExpression) expression;
			String columnName = column.getColAlias();
			Object value = ((Object[]) row[0])[coloralias.indexOf(columnName)];
			if (value instanceof String stringval) {
				return String.valueOf(stringval);
			} else if (value instanceof Double doubleval) {
				return doubleval;
			} else if (value instanceof Integer intval) {
				return intval;
			} else if (value instanceof Long longval) {
				return longval;
			} else if (value instanceof String stringval && NumberUtils.isParsable(stringval)) {
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
				if (leftvalue instanceof Double lv && rightvalue instanceof Double rv) {
					return lv > rv;
				} else if (leftvalue instanceof Long lv && rightvalue instanceof Double rv) {
					return lv > rv;
				} else if (leftvalue instanceof Double lv && rightvalue instanceof Long rv) {
					return lv > rv;
				} else if (leftvalue instanceof Integer lv && rightvalue instanceof Double rv) {
					return lv > rv;
				} else if (leftvalue instanceof Double lv && rightvalue instanceof Integer rv) {
					return lv > rv;
				} else if (leftvalue instanceof Integer lv && rightvalue instanceof Integer rv) {
					return lv > rv;
				} else if (leftvalue instanceof Integer lv && rightvalue instanceof Long rv) {
					return lv > rv;
				} else if (leftvalue instanceof Long lv && rightvalue instanceof Integer rv) {
					return lv > rv;
				} else if (leftvalue instanceof Long lv && rightvalue instanceof Long rv) {
					return lv > rv;
				} else {
					return false;
				}
			case "GreaterThanEqual":
				if (leftvalue instanceof Double lvgt && rightvalue instanceof Double rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Long lvgt && rightvalue instanceof Double rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Double lvgt && rightvalue instanceof Long rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Integer lvgt && rightvalue instanceof Double rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Double lvgt && rightvalue instanceof Integer rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Integer lvgt && rightvalue instanceof Integer rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Integer lvgt && rightvalue instanceof Long rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Long lvgt && rightvalue instanceof Integer rvgt) {
					return lvgt >= rvgt;
				} else if (leftvalue instanceof Long lvgt && rightvalue instanceof Long rvgt) {
					return lvgt >= rvgt;
				} else {
					return false;
				}
			case "LessThan":
				if (leftvalue instanceof Double lvlt && rightvalue instanceof Double rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Long lvlt && rightvalue instanceof Double rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Double lvlt && rightvalue instanceof Long rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Integer lvlt && rightvalue instanceof Double rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Double lvlt && rightvalue instanceof Integer rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Integer lvlt && rightvalue instanceof Integer rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Integer lvlt && rightvalue instanceof Long rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Long lvlt && rightvalue instanceof Integer rvlt) {
					return lvlt < rvlt;
				} else if (leftvalue instanceof Long lvlt && rightvalue instanceof Long rvlt) {
					return lvlt < rvlt;
				} else {
					return false;
				}
			case "LessThanEqual":
				if (leftvalue instanceof Double lvle && rightvalue instanceof Double rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Long lvle && rightvalue instanceof Double rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Double lvle && rightvalue instanceof Long rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Integer lvle && rightvalue instanceof Double rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Double lvle && rightvalue instanceof Integer rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Integer lvle && rightvalue instanceof Integer rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Integer lvle && rightvalue instanceof Long rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Long lvle && rightvalue instanceof Integer rvle) {
					return lvle <= rvle;
				} else if (leftvalue instanceof Long lvle && rightvalue instanceof Long rvle) {
					return lvle <= rvle;
				} else {
					return false;
				}
			case "Equal":
				if (leftvalue instanceof Double lveq && rightvalue instanceof Double rveq) {
					return lveq == rveq;
				} else if (leftvalue instanceof Long lveq && rightvalue instanceof Double rveq) {
					return lveq.doubleValue() == rveq.doubleValue();
				} else if (leftvalue instanceof Double lveq && rightvalue instanceof Long rveq) {
					return lveq.doubleValue() == rveq.doubleValue();
				} else if (leftvalue instanceof Integer lveq && rightvalue instanceof Double rveq) {
					return lveq.doubleValue() == rveq.doubleValue();
				} else if (leftvalue instanceof Double lveq && rightvalue instanceof Integer rveq) {
					return lveq.doubleValue() == rveq.doubleValue();
				} else if (leftvalue instanceof Integer lveq && rightvalue instanceof Long rveq) {
					return lveq.longValue() == rveq.longValue();
				} else if (leftvalue instanceof Long lveq && rightvalue instanceof Integer rveq) {
					return lveq.longValue() == rveq.longValue();
				} else if (leftvalue instanceof Integer lveq && rightvalue instanceof Integer rveq) {
					return lveq.intValue() == rveq.intValue();
				} else if (leftvalue instanceof Long lveq && rightvalue instanceof Long rveq) {
					return lveq.longValue() == rveq.longValue();
				} else if (leftvalue instanceof String lveq && rightvalue instanceof String rveq) {
					return lveq.equals(rveq);
				} else {
					return false;
				}
			case "NotEqual":
				if (leftvalue instanceof Double lvne && rightvalue instanceof Double rvne) {
					return lvne != rvne;
				} else if (leftvalue instanceof Long lvne && rightvalue instanceof Double rvne) {
					return lvne.longValue() != rvne.longValue();
				} else if (leftvalue instanceof Double lvne && rightvalue instanceof Long rvne) {
					return lvne.longValue() != rvne.longValue();
				} else if (leftvalue instanceof Integer lvne && rightvalue instanceof Double rvne) {
					return lvne.longValue() != rvne.longValue();
				} else if (leftvalue instanceof Double lvne && rightvalue instanceof Integer rvne) {
					return lvne.longValue() != rvne.longValue();
				} else if (leftvalue instanceof Long lvne && rightvalue instanceof Integer rvne) {
					return lvne.longValue() != rvne.longValue();
				} else if (leftvalue instanceof Integer lvne && rightvalue instanceof Long rvne) {
					return lvne.longValue() != rvne.longValue();
				} else if (leftvalue instanceof Integer lvne && rightvalue instanceof Integer rvne) {
					return lvne.longValue() != rvne.longValue();
				} else if (leftvalue instanceof Long lvne && rightvalue instanceof Long rvne) {
					return lvne != rvne;
				} else if (leftvalue instanceof String lvne && rightvalue instanceof String rvne) {
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
			for (LogicalFieldSchema lfs : loForEach.getSchema().getFields()) {
				aliases.add(lfs.alias);
			}
		}
	}
}
