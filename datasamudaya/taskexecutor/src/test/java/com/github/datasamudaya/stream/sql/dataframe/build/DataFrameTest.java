package com.github.datasamudaya.stream.sql.dataframe.build;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;
import org.junit.Test;

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
	Logger log = Logger.getLogger(DataFrameTest.class);
	List<String> airportsheader = Arrays.asList("iata", "airport", "city", "state", "country", "latitude", "longitude");
	List<SqlTypeName> airportstype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	@Test
	public void testDataFrameAllColumns() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(29,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameAllColumnsWithFilter() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		List<List<Object[]>> output = (List<List<Object[]>>) df
				.filter(new OrPredicate(new NumericExpressionPredicate(NumericOperator.EQUALS, 
						new Column("MonthOfYear"), 
						new Literal(10)),new NumericExpressionPredicate(NumericOperator.EQUALS, 
								new Column("MonthOfYear"), 
								new Literal(11))))
				.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(29,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameAllColumnsWithFilterAndSelect() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		List<List<Object[]>> output = (List<List<Object[]>>) df
				.filter(new OrPredicate(new NumericExpressionPredicate(NumericOperator.EQUALS, 
						new Column("MonthOfYear"), 
						new Literal(10)),new NumericExpressionPredicate(NumericOperator.EQUALS, 
								new Column("MonthOfYear"), 
								new Literal(11))))
				.select("DayofMonth","MonthOfYear","AirlineYear")
				.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(3,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumns() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumnsWithFilterWithEqualsCondition() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear")
		.filter(new NumericExpressionPredicate(NumericOperator.EQUALS, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumnsWithFilterWithGreaterThanCondition() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear")
		.filter(new NumericExpressionPredicate(NumericOperator.GREATER_THAN, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumnsWithFilterWithGreaterThanEqualsCondition() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear")
		.filter(new NumericExpressionPredicate(NumericOperator.GREATER_THAN_EQUALS, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumnsWithFilterWithLessThanCondition() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear")
		.filter(new NumericExpressionPredicate(NumericOperator.LESS_THAN, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumnsWithFilterWithLessThanEqualsCondition() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear")
		.filter(new NumericExpressionPredicate(NumericOperator.LESS_THAN_EQUALS, new Column("MonthOfYear"), new Literal(10)));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumnsWithFilterAndExpression() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear")
		.filter(new AndPredicate(new NumericExpressionPredicate(NumericOperator.EQUALS, 
				new Column("MonthOfYear"), 
				new Literal(10)),new NumericExpressionPredicate(NumericOperator.LESS_THAN_EQUALS, 
						new Column("MonthOfYear"), 
						new Literal(11))));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameRequiredColumnsWithFilterOrExpression() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		df.select("AirlineYear","MonthOfYear")
		.filter(new OrPredicate(new NumericExpressionPredicate(NumericOperator.EQUALS, 
				new Column("MonthOfYear"), 
				new Literal(10)),new NumericExpressionPredicate(NumericOperator.EQUALS, 
						new Column("MonthOfYear"), 
						new Literal(11))));
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(2,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameAggregateFunction() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
		df.select("AirlineYear","MonthOfYear", "ArrDelay")
		.filter(new OrPredicate(new NumericExpressionPredicate(NumericOperator.EQUALS, 
				new Column("MonthOfYear"), 
				new Literal(10)),new NumericExpressionPredicate(NumericOperator.EQUALS, 
						new Column("MonthOfYear"), 
						new Literal(11))))
		.aggregate(builder, "AirlineYear", "MonthOfYear").select(0, 1, 2, 3, 4);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(5,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameAggregateFunctionAliasOrColumnInSelect() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
		df.select("AirlineYear","MonthOfYear", "ArrDelay")
		.filter(new OrPredicate(new NumericExpressionPredicate(NumericOperator.EQUALS, 
				new Column("MonthOfYear"), 
				new Literal(10)),new NumericExpressionPredicate(NumericOperator.EQUALS, 
						new Column("MonthOfYear"), 
						new Literal(11))))
		.aggregate(builder, "AirlineYear", "MonthOfYear").select("avgdelay", "sumdelay", "cnt");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(3,values.length);
			}
		}
	}
	
	@Test
	public void testDataFrameAggregateFunctionAndNonAggregate() throws Exception{
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.setTablename("airline")
				.setDb("db")
				.setColumns(airlineheader.toArray(new String[0]))
				.setFileFormat("csv")
				.setHdfs(hdfsfilepath)
				.setFolder(airlinesamplesql).setTypes(airlineheadertypes).build();
		AggregateFunctionBuilder builder = AggregateFunctionBuilder.builder();
		builder.sum("sumdelay", "ArrDelay").avg("avgdelay", "ArrDelay").count("cnt");
		df.select("AirlineYear","MonthOfYear", "ArrDelay")
		.filter(new OrPredicate(new NumericExpressionPredicate(NumericOperator.EQUALS, 
				new Column("MonthOfYear"), 
				new Literal(10)),new NumericExpressionPredicate(NumericOperator.EQUALS, 
						new Column("MonthOfYear"), 
						new Literal(11))))
		.aggregate(builder, "AirlineYear", "MonthOfYear")
		.select("avgdelay", "sumdelay", "cnt");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder()
				.addField(null, new String[]{"avgdelay"})
				.addField(null, new String[]{"sumdelay"})
				.addField(null, new String[]{"cnt"})
				.addFunction("loge", "logfunc", new String[]{"cnt"});
		df.selectWithFunc(nonaggfuncbuilder);
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for(List<Object[]> valuel:output) {
			for(Object[] values:valuel) {
				log.info(Arrays.toString(values));
				assertEquals(4,values.length);
			}
		}
	}
}
