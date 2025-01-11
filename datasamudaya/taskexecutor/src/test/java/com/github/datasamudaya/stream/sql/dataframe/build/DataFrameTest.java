package com.github.datasamudaya.stream.sql.dataframe.build;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		List<List<Object[]>> output = (List<List<Object[]>>) df.filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		List<List<Object[]>> output = (List<List<Object[]>>) df.filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear", "MonthOfYear").filter(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)));
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear", "MonthOfYear").filter(new NumericExpressionPredicate(NumericOperator.GREATER_THAN,
				new Column("MonthOfYear"), new Literal(10)));
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear", "MonthOfYear").filter(new NumericExpressionPredicate(
				NumericOperator.GREATER_THAN_EQUALS, new Column("MonthOfYear"), new Literal(10)));
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear", "MonthOfYear").filter(
				new NumericExpressionPredicate(NumericOperator.LESS_THAN, new Column("MonthOfYear"), new Literal(10)));
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear", "MonthOfYear").filter(new NumericExpressionPredicate(NumericOperator.LESS_THAN_EQUALS,
				new Column("MonthOfYear"), new Literal(10)));
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear", "MonthOfYear")
				.filter(new AndPredicate(
						new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"),
								new Literal(10)),
						new NumericExpressionPredicate(NumericOperator.LESS_THAN_EQUALS, new Column("MonthOfYear"),
								new Literal(11))));
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear", "MonthOfYear").filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))));
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[]{new Column("ArrDelay")}).avg("avgdelay", new Object[]{new Column("ArrDelay")}).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "ArrDelay").filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[]{new Column("ArrDelay")}).avg("avgdelay", new Object[]{new Column("ArrDelay")}).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "ArrDelay").filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[]{new Column("ArrDelay")}).avg("avgdelay", new Object[]{new Column("ArrDelay")}).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "DayofMonth", "ArrDelay").filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[]{new Column("ArrDelay")}).avg("avgdelay", new Object[]{new Column("ArrDelay")}).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "DayofMonth", "ArrDelay").filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", new Object[]{new Column("ArrDelay")}).avg("avgdelay", new Object[]{new Column("ArrDelay")}).count("cnt");
		df.select("AirlineYear", "MonthOfYear", "ArrDelay").filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("abs", "absarrdelay", new Object[] { new Column("ArrDelay") })
				.addFunction("abs", "absdepdelay", new Object[] { new Column("DepDelay") });
		df.selectWithFunc(nonaggfuncbuilder).aggregate(AggregateFunctionBuilder.builder()
				.sum("sumarrdelay", new Object[]{new Column("absarrdelay")})
				.sum("sumdepdelay", new Object[]{new Column("absdepdelay")}), "UniqueCarrier");
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("round", "roundarrdelay", new Object[] { new Column("ArrDelay") })
				.addFunction("round", "rounddepdelay", new Object[] { new Column("DepDelay") });
		df.selectWithFunc(nonaggfuncbuilder).aggregate(AggregateFunctionBuilder.builder()
				.sum("sumarrdelay", new Object[]{new Column("roundarrdelay")})
				.sum("sumdepdelay", new Object[]{new Column("rounddepdelay")}), "UniqueCarrier");
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("ceil", "ceilavgarrdelay", new Object[] { new Column("avgarrdelay") })
				.addFunction("ceil", "ceilavgdepdelay", new Object[] { new Column("avgdepdelay") });
		df.aggregate(AggregateFunctionBuilder.builder()
				.avg("avgarrdelay", new Object[]{new Column("ArrDelay")})
				.avg("avgdepdelay", new Object[]{new Column("DepDelay")}), "UniqueCarrier")
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("floor", "flooravgarrdelay", new Object[] { new Column("avgarrdelay") })
				.addFunction("floor", "flooravgdepdelay", new Object[] { new Column("avgdepdelay") });
		df.aggregate(AggregateFunctionBuilder.builder()
				.avg("avgarrdelay", new Object[]{new Column("ArrDelay")})
				.avg("avgdepdelay", new Object[]{new Column("DepDelay")}), "UniqueCarrier")
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("pow", "powavgarrdelay", new Object[] { new Column("avgarrdelay"), new Literal(2) })
				.addFunction("pow", "powavgdepdelay", new Object[] { new Column("avgdepdelay"), new Literal(2) });
		df.aggregate(AggregateFunctionBuilder.builder()
				.avg("avgarrdelay", new Object[]{new Column("ArrDelay")})
				.avg("avgdepdelay", new Object[]{new Column("DepDelay")}), "UniqueCarrier")
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("pow", "powavgarrdelay", new Object[] { new Column("avgarrdelay"), new Literal(2) })
				.addFunction("pow", "powavgdepdelay", new Object[] { new Column("avgdepdelay"), new Literal(2) });
		df.aggregate(AggregateFunctionBuilder.builder()
				.avg("avgarrdelay", new Object[]{new Column("ArrDelay")})
				.avg("avgdepdelay", new Object[]{new Column("DepDelay")}), "UniqueCarrier")
		.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("sqrt", "sqrtpowavgarrdelay", new Object[] { new Column("powavgarrdelay")})
				.addFunction("sqrt", "sqrtpowavgdepdelay", new Object[] { new Column("powavgdepdelay")});
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("exp", "expavgarrdelay", new Object[] { new Column("avgarrdelay")})
				.addFunction("exp", "expavgdepdelay", new Object[] { new Column("avgdepdelay")});
		df.aggregate(AggregateFunctionBuilder.builder()
				.avg("avgarrdelay", new Object[]{new Column("ArrDelay")})
				.avg("avgdepdelay", new Object[]{new Column("DepDelay")}), "UniqueCarrier")
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })				
				.addFunction("loge", "lnavgarrdelay", new Object[] { new Column("avgarrdelay")})
				.addFunction("loge", "lnavgdepdelay", new Object[] { new Column("avgdepdelay")});
		df.aggregate(AggregateFunctionBuilder.builder()
				.avg("avgarrdelay", new Object[]{new Column("ArrDelay")})
				.avg("avgdepdelay", new Object[]{new Column("DepDelay")}), "UniqueCarrier")
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("lowercase", "lcorigin", new Object[] { new Column("Origin")})
				.addFunction("lowercase", "lcdest", new Object[] { new Column("Dest")});		
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(((String)values[1]).toLowerCase(), values[3]);
				assertEquals(((String)values[2]).toLowerCase(), values[4]);
				assertEquals(5, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateBase64encode() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("base64encode", "b64encorigin", new Object[] { new Column("Origin")})
				.addFunction("base64encode", "b64encdest", new Object[] { new Column("Dest")});		
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(Base64.getEncoder().encodeToString(((String)values[1]).getBytes()), values[3]);
				assertEquals(Base64.getEncoder().encodeToString(((String)values[2]).getBytes()), values[4]);
				assertEquals(5, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateBase64decode() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("base64encode", "b64encorigin", new Object[] { new Column("Origin")})
				.addFunction("base64encode", "b64encdest", new Object[] { new Column("Dest")});		
		df.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("base64decode", "b64decb64encorigin", new Object[] { new Column("b64encorigin")})
				.addFunction("base64decode", "b64decb64encdest", new Object[] { new Column("b64encdest")});
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("concat", "concatorigin", new Object[] { new Column("Origin"), new Literal("     ")})
				.addFunction("concat", "concatdest", new Object[] { new Column("Dest"), new Literal("     ")});		
		df.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("normalizespaces", "normspacesorigin", new Object[] { new Column("concatorigin")})
				.addFunction("normalizespaces", "normspacesdest", new Object[] { new Column("concatdest")});		
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("concat", "concatorigin", new Object[] { new Column("Origin"), new Literal("ABC")})
				.addFunction("concat", "concatdest", new Object[] { new Column("Dest"), new Literal("XYZ")});		
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(values[1]+"ABC", values[3]);
				assertEquals(values[2]+"XYZ", values[4]);
				assertEquals(5, values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameNonAggregateTrimstring() throws Exception {
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("concat", "concatorigin", new Object[] { new Column("Origin"), new Literal("   ")})
				.addFunction("concat", "concatdest", new Object[] { new Column("Dest"), new Literal("   ")});		
		df.selectWithFunc(nonaggfuncbuilder);
		nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("trimstr", "trimstrorigin", new Object[] { new Column("concatorigin")})
				.addFunction("trimstr", "trimstrdest", new Object[] { new Column("concatdest")});		
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
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig).setTablename("airline").setDb("db")
				.setColumns(airlineheader.toArray(new String[0])).setFileFormat("csv").setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("UniqueCarrier", "Origin", "Dest");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addField(null, new String[] { "Origin"})
				.addField(null, new String[] { "Dest" })
				.addFunction("uppercase", "ucorigin", new Object[] { new Column("Origin")})
				.addFunction("uppercase", "ucdest", new Object[] { new Column("Dest")});		
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
				assertEquals(((String)values[1]).toUpperCase(), values[3]);
				assertEquals(((String)values[2]).toUpperCase(), values[4]);
				assertEquals(5, values.length);
			}
		}
	}
}
