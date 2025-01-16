package com.github.datasamudaya.stream.sql.dataframe.build;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.stream.StreamPipelineBaseTestCommon;

public class DataFrameTest extends StreamPipelineBaseTestCommon {
	List<String> airlineheader = Arrays.asList("AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay");
	List<SqlTypeName> airlineheadertypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT);
	List<String> carrierheader = Arrays.asList("Code", "Description");
	List<SqlTypeName> carrierheadertypes = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	Logger log = LogManager.getLogger(DataFrameTest.class);
	List<String> airportsheader = Arrays.asList("iata", "airport", "city", "state", "country", "latitude", "longitude");
	List<SqlTypeName> airportstype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);

	@Test
	public void testDataFrameAllColumns() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(29, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAllColumnsWithFilter() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		List<List<Object[]>> output = (List<List<Object[]>>) df
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10 || ((long) values[1]) == 11);
				assertEquals(29, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAllColumnsWithFilterOrSelect() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		List<List<Object[]>> output = (List<List<Object[]>>) df
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.select("DayofMonth", "MonthOfYear", "AirlineYear").execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10 || ((long) values[1]) == 11);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumns() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumnsWithFilterWithEqualsCondition() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear")
				.filter(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10);
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumnsWithFilterWithGreaterThanCondition() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear")
				.filter(new Expression(Operator.GREATER_THAN, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) > 10);
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumnsWithFilterWithGreaterThanEqualsCondition() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear")
				.filter(new Expression(Operator.GREATER_THAN_EQUALS, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) >= 10);
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumnsWithFilterWithLessThanCondition() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear")
				.filter(new Expression(Operator.LESS_THAN, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) < 10);
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumnsWithFilterWithLessThanEqualsCondition() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear")
				.filter(new Expression(Operator.LESS_THAN_EQUALS, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) <= 10);
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumnsWithFilterAndExpression() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear")
				.filter(new AndPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.LESS_THAN_EQUALS, new Column("MonthOfYear"), new Literal(11))));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10 && ((long) values[1]) <= 11);
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameRequiredColumnsWithFilterOrExpression() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("AirlineYear", "MonthOfYear")
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10 || ((long) values[1]) == 11);
				assertEquals(2, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAggregateFunction() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdelay", new Object[] { new Column("ArrDelay") }).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "ArrDelay")
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.aggregate(builder, "AirlineYear", "MonthOfYear").select(0, 1, 2, 3, 4);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10 || ((long) values[1]) == 11);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAggregateFunctionAliasOrColumnInSelect() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdelay", new Object[] { new Column("ArrDelay") }).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "ArrDelay")
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.aggregate(builder, "AirlineYear", "MonthOfYear").select("avgdelay", "sumdelay", "cnt");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAggregateFunctionSort() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdelay", new Object[] { new Column("ArrDelay") }).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "DayofMonth", "ArrDelay")
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.aggregate(builder, "AirlineYear", "MonthOfYear", "DayofMonth")
				.sortBy(new Tuple2<String, String>("AirlineYear", "ASC"),
						new Tuple2<String, String>("DayofMonth", "DESC"),
						new Tuple2<String, String>("MonthOfYear", "ASC"));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10 || ((long) values[1]) == 11);
				assertEquals(6, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAggregateFunctionSortOrdinal() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdelay", new Object[] { new Column("ArrDelay") }).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "DayofMonth", "ArrDelay")
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.aggregate(builder, "AirlineYear", "MonthOfYear", "DayofMonth")
				.sortByOrdinal(new Tuple2<Integer, String>(0, "ASC"), new Tuple2<Integer, String>(1, "DESC"),
						new Tuple2<Integer, String>(5, "ASC"));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((long) values[1]) == 10 || ((long) values[1]) == 11);
				assertEquals(6, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAggregateFunctionAndNonAggregateLoge() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdelay", new Object[] { new Column("ArrDelay") }).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "ArrDelay")
				.filter(new OrPredicate(new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
						new Expression(Operator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.aggregate(builder, "AirlineYear", "MonthOfYear").select("avgdelay", "sumdelay", "cnt");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "avgdelay" })
				.addField(null, new String[] { "sumdelay" }).addField(null, new String[] { "cnt" })
				.addFunction("loge", "logfunc", new Object[] { new Column("cnt") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(Math.log(Double.valueOf(DataSamudayaConstants.EMPTY + values[2])) == (double) values[3]);
				assertEquals(4, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateLength() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("length", "lengthorigin", new Object[] { new Column("Origin") })
				.addFunction("length", "lengthdest", new Object[] { new Column("Dest") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertTrue(((String) values[1]).length() == (long) values[3]);
				assertTrue(((String) values[2]).length() == (long) values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameAggregateAbsNonAggSum() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("abs", "absarrdelay", new Object[] { new Column("ArrDelay") })
				.addFunction("abs", "absdepdelay", new Object[] { new Column("DepDelay") });
		df.selectWithFunc(nonaggfuncbuilder)
				.aggregate(AggregateFunctionBuilder.builder()
						.sum("sumarrdelay", new Object[] { new Column("absarrdelay") })
						.sum("sumdepdelay", new Object[] { new Column("absdepdelay") }), "UniqueCarrier");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateRoundAggSum() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("round", "roundarrdelay", new Object[] { new Column("ArrDelay") })
				.addFunction("round", "rounddepdelay", new Object[] { new Column("DepDelay") });
		df.selectWithFunc(nonaggfuncbuilder)
				.aggregate(AggregateFunctionBuilder.builder()
						.sum("sumarrdelay", new Object[] { new Column("roundarrdelay") })
						.sum("sumdepdelay", new Object[] { new Column("rounddepdelay") }), "UniqueCarrier");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateCeilAggAvg() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("ceil", "ceilavgarrdelay", new Object[] { new Column("avgarrdelay") })
				.addFunction("ceil", "ceilavgdepdelay", new Object[] { new Column("avgdepdelay") });
		df.aggregate(AggregateFunctionBuilder.builder().avg("avgarrdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdepdelay", new Object[] { new Column("DepDelay") }), "UniqueCarrier")
				.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateFloorAggAvg() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("floor", "flooravgarrdelay", new Object[] { new Column("avgarrdelay") })
				.addFunction("floor", "flooravgdepdelay", new Object[] { new Column("avgdepdelay") });
		df.aggregate(AggregateFunctionBuilder.builder().avg("avgarrdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdepdelay", new Object[] { new Column("DepDelay") }), "UniqueCarrier")
				.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregatePowAggAvg() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("pow", "powavgarrdelay", new Object[] { new Column("avgarrdelay"), new Literal(2) })
				.addFunction("pow", "powavgdepdelay", new Object[] { new Column("avgdepdelay"), new Literal(2) });
		df.aggregate(AggregateFunctionBuilder.builder().avg("avgarrdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdepdelay", new Object[] { new Column("DepDelay") }), "UniqueCarrier")
				.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregatePowSqrtAggAvg() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("pow", "powavgarrdelay", new Object[] { new Column("avgarrdelay"), new Literal(2) })
				.addFunction("pow", "powavgdepdelay", new Object[] { new Column("avgdepdelay"), new Literal(2) });
		df.aggregate(AggregateFunctionBuilder.builder().avg("avgarrdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdepdelay", new Object[] { new Column("DepDelay") }), "UniqueCarrier")
				.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("sqrt", "sqrtpowavgarrdelay", new Object[] { new Column("powavgarrdelay") })
				.addFunction("sqrt", "sqrtpowavgdepdelay", new Object[] { new Column("powavgdepdelay") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateExpAggAvg() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("exp", "expavgarrdelay", new Object[] { new Column("avgarrdelay") })
				.addFunction("exp", "expavgdepdelay", new Object[] { new Column("avgdepdelay") });
		df.aggregate(AggregateFunctionBuilder.builder().avg("avgarrdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdepdelay", new Object[] { new Column("DepDelay") }), "UniqueCarrier")
				.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateLnAggAvg() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("loge", "lnavgarrdelay", new Object[] { new Column("avgarrdelay") })
				.addFunction("loge", "lnavgdepdelay", new Object[] { new Column("avgdepdelay") });
		df.aggregate(AggregateFunctionBuilder.builder().avg("avgarrdelay", new Object[] { new Column("ArrDelay") })
				.avg("avgdepdelay", new Object[] { new Column("DepDelay") }), "UniqueCarrier")
				.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertEquals(3, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateLowercase() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("lowercase", "lcorigin", new Object[] { new Column("Origin") })
				.addFunction("lowercase", "lcdest", new Object[] { new Column("Dest") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(((String) values[1]).toLowerCase(), values[3]);
				assertEquals(((String) values[2]).toLowerCase(), values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateBase64encode() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("base64encode", "b64encorigin", new Object[] { new Column("Origin") })
				.addFunction("base64encode", "b64encdest", new Object[] { new Column("Dest") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(Base64.getEncoder().encodeToString(((String) values[1]).getBytes()), values[3]);
				assertEquals(Base64.getEncoder().encodeToString(((String) values[2]).getBytes()), values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateBase64decode() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("base64encode", "b64encorigin", new Object[] { new Column("Origin") })
				.addFunction("base64encode", "b64encdest", new Object[] { new Column("Dest") });
		df.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("base64decode", "b64decb64encorigin", new Object[] { new Column("b64encorigin") })
				.addFunction("base64decode", "b64decb64encdest", new Object[] { new Column("b64encdest") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(values[1], values[3]);
				assertEquals(values[2], values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateNormalizespaces() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("concat", "concatorigin", new Object[] { new Column("Origin"), new Literal("     ") })
				.addFunction("concat", "concatdest", new Object[] { new Column("Dest"), new Literal("     ") });
		df.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("normalizespaces", "normspacesorigin", new Object[] { new Column("concatorigin") })
				.addFunction("normalizespaces", "normspacesdest", new Object[] { new Column("concatdest") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(values[1], values[3]);
				assertEquals(values[2], values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateCurrentIsoDate() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("currentisodate", "isodate", new Object[] {});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertNotNull(values[1]);
				assertNotNull(values[2]);
				assertNotNull(values[3]);
				assertEquals(4, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateConcat() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("concat", "concatorigin", new Object[] { new Column("Origin"), new Literal("ABC") })
				.addFunction("concat", "concatdest", new Object[] { new Column("Dest"), new Literal("XYZ") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(values[1] + "ABC", values[3]);
				assertEquals(values[2] + "XYZ", values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateTrimstring() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("concat", "concatorigin", new Object[] { new Column("Origin"), new Literal("   ") })
				.addFunction("concat", "concatdest", new Object[] { new Column("Dest"), new Literal("   ") });
		df.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("trimstr", "trimstrorigin", new Object[] { new Column("concatorigin") })
				.addFunction("trimstr", "trimstrdest", new Object[] { new Column("concatdest") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(values[1], values[3]);
				assertEquals(values[2], values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameNonAggregateUppercase() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" })
				.addFunction("uppercase", "ucorigin", new Object[] { new Column("Origin") })
				.addFunction("uppercase", "ucdest", new Object[] { new Column("Dest") });
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(((String) values[1]).toUpperCase(), values[3]);
				assertEquals(((String) values[2]).toUpperCase(), values[4]);
				assertEquals(5, values.length);
			}
		}
	}

	@Test
	public void testDataFrameJoin() throws Exception {
		DataFrame dfleft = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes).setDb("db")
				.setFileFormat("csv").setHdfs(hdfsfilepath).build();
		DataFrame dfright = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(carriers, carrierheader.toArray(new String[0]), "carriers", carrierheadertypes).setDb("db")
				.setFileFormat("csv").setHdfs(hdfsfilepath).build();
		dfleft.scantable("airlines");
		dfleft.select("UniqueCarrier", "Origin", "Dest");
		dfright.scantable("carriers");
		dfright.select("Code", "Description");
		dfleft.innerjoin(dfright, new Expression(Operator.EQUALS, new Column("UniqueCarrier"), new Column("Code")));
		dfleft.select("UniqueCarrier", "Origin", "Dest", "Description");
		List<List<Object[]>> output = (List<List<Object[]>>) dfleft.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(4, values.length);
			}
		}
	}
	
	
	@Test
	public void testDataFrameLeftJoin() throws Exception {
		DataFrame dfleft = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes).setDb("db")
				.setFileFormat("csv").setHdfs(hdfsfilepath).build();
		DataFrame dfright = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(carriers, carrierheader.toArray(new String[0]), "carriers", carrierheadertypes).setDb("db")
				.setFileFormat("csv").setHdfs(hdfsfilepath).build();
		dfleft.scantable("airlines");
		dfleft.select("UniqueCarrier", "Origin", "Dest");
		dfright.scantable("carriers");
		dfright.select("Code", "Description");
		dfleft.leftjoin(dfright, new Expression(Operator.EQUALS, new Column("UniqueCarrier"), new Column("Code")));
		dfleft.select("UniqueCarrier", "Origin", "Dest", "Description");
		List<List<Object[]>> output = (List<List<Object[]>>) dfleft.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRightJoin() throws Exception {
		DataFrame dfleft = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes).setDb("db")
				.setFileFormat("csv").setHdfs(hdfsfilepath).build();
		DataFrame dfright = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(carriers, carrierheader.toArray(new String[0]), "carriers", carrierheadertypes).setDb("db")
				.setFileFormat("csv").setHdfs(hdfsfilepath).build();
		dfleft.scantable("airlines");
		dfleft.select("UniqueCarrier", "Origin", "Dest");
		dfright.scantable("carriers");
		dfright.select("Code", "Description");
		dfleft.rightjoin(dfright, new Expression(Operator.EQUALS, new Column("UniqueCarrier"), new Column("Code")));
		dfleft.select("UniqueCarrier", "Origin", "Dest", "Description");
		List<List<Object[]>> output = (List<List<Object[]>>) dfleft.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateTrimConcatstring() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("trimstr", "trimstrconcatorigin", 
				new Object[] {nonaggfuncbuilder
						.getNestedFunction("concat", new Object[] { new Column("Origin"), new Literal("   ") })});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(values[1], values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateLengthConcatstring() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("length", "lengthconcatorigin", 
				new Object[] {nonaggfuncbuilder
						.getNestedFunction("concat", new Object[] { new Column("Origin"), new Literal("   ") })});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(6l, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameAggSumNonAggregateLengthConcatstring() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("length", "lengthconcatorigin", 
				new Object[] {nonaggfuncbuilder
						.getNestedFunction("concat", new Object[] { new Column("Origin"), new Literal("   ") })});
		df.selectWithFunc(nonaggfuncbuilder);
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumlength", new Object[] { new Column("lengthconcatorigin") });
		df.aggregate(builder, "UniqueCarrier");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(278160l, values[1]);
				assertEquals(2, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateOverlay() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("overlay", "overlayorigin", 
				new Object[] {new Column("Origin"), new Literal("ABC"),new Literal(0), new Literal(3)});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals("ABC", values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateInitcap() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("initcap", "initcaporigin", 
				new Object[] {new Column("Origin")});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				String origin = (String) values[1];
				assertEquals((origin.charAt(0)+"").toUpperCase() + origin.substring(1), values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregatePosition() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("position", "position", 
				new Object[] {new Column("Origin"), new Column("Origin")});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(0, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateAcos() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("acos", "acos", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double acos = Math.acos(Double.valueOf(String.valueOf(values[1])));
				assertEquals(acos, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateAsin() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("asin", "asin", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double asin = Math.asin(Double.valueOf(String.valueOf(values[1])));
				assertEquals(asin, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateATan() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("atan", "atan", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double atan = Math.atan(Double.valueOf(String.valueOf(values[1])));
				assertEquals(atan, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateCos() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("cos", "cos", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double cos = Math.cos(Double.valueOf(String.valueOf(values[1])));
				assertEquals(cos, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateSin() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("sin", "sin", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double sin = Math.sin(Double.valueOf(String.valueOf(values[1])));
				assertEquals(sin, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateTan() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("tan", "tan", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double tan = Math.tan(Double.valueOf(String.valueOf(values[1])));
				assertEquals(tan, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateCosec() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("cosec", "cosec", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double cosec = 1.0/Math.sin(Double.valueOf(String.valueOf(values[1])));
				assertEquals(cosec, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateSec() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("sec", "sec", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double sec = 1.0/Math.cos(Double.valueOf(String.valueOf(values[1])));
				assertEquals(sec, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateCot() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("cot", "cot", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double cot = 1.0/Math.tan(Double.valueOf(String.valueOf(values[1])));
				assertEquals(cot, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateCbrt() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("cbrt", "cbrt", new Object[] {nonaggfuncbuilder.getNestedFunction("abs", new Object[] {new Column("MonthOfYear")})});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double cbrt = Math.cbrt(Double.valueOf(String.valueOf(values[1])));
				assertEquals(cbrt, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregatePii() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("pii", "pii", new Object[] {});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				double pii = Math.PI;
				assertEquals(pii, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateDegrees() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("degrees", "degrees", new Object[] {new Column("MonthOfYear")});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(Double.valueOf(String.valueOf(values[1])) / Math.PI * 180, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateRadians() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(airlinesamplesql, airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(hdfsfilepath).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "MonthOfYear", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "MonthOfYear" }).addField(null, new String[] { "Dest" });
		nonaggfuncbuilder
		.addFunction("radians", "radians", new Object[] {new Column("MonthOfYear")});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(Double.valueOf(String.valueOf(values[1])) / 180 * Math.PI, values[3]);
				assertEquals(4, values.length);
			}
		}
	}
	
}
