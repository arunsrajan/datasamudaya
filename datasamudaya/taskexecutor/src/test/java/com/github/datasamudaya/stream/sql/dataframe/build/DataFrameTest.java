package com.github.datasamudaya.stream.sql.dataframe.build;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
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
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
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
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
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
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
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
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
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
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
		df.select("AirlineYear", "MonthOfYear", "ArrDelay").filter(new OrPredicate(
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)),
				new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(11))))
				.aggregate(builder, "AirlineYear", "MonthOfYear").select("avgdelay", "sumdelay", "cnt");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "avgdelay" })
				.addField(null, new String[] { "sumdelay" }).addField(null, new String[] { "cnt" })
				.addFunction("loge", "logfunc", new String[] { "cnt" });
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
				.addFunction("length", "lengthorigin", new String[] { "Origin" })
				.addFunction("length", "lengthdest", new String[] { "Dest" });
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
}
