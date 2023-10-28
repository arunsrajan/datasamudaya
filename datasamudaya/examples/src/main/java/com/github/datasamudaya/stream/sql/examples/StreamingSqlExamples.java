package com.github.datasamudaya.stream.sql.examples;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.Pipeline;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;

public class StreamingSqlExamples implements Serializable, Pipeline{	
	private static final long serialVersionUID = 9184876383199143760L;
	static String carriers = "/carriers";
	static String airportssample = "/airports";
	static String sqloutput = "/sqloutput-";
	List<String> airlineheader = Arrays.asList("AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay");
	List<SqlTypeName> airlineheadertypes = Arrays.asList(SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER, SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.VARCHAR,SqlTypeName.INTEGER,SqlTypeName.VARCHAR,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.VARCHAR,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER,SqlTypeName.INTEGER);
	List<String> carrierheader = Arrays.asList("Code", "Description");
	List<SqlTypeName> carrierheadertypes = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	List<String> airportsheader = Arrays.asList("iata", "airport", "city", "state", "country", "latitude", "longitude");
	List<SqlTypeName> airportstype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	private static Logger log = LoggerFactory.getLogger(StreamingSqlExamples.class);
	
	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		if(args[3].equals("yarn")) {
			pipelineconfig.setYarn("true");
		} else {
			pipelineconfig.setYarn("false");
		}
		if(args[3].equals("jgroups")) {
			pipelineconfig.setJgroups("true");
		} else {
			pipelineconfig.setJgroups("false");
		}
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		testAllColumns(args, pipelineconfig);
		testAllColumnsWithWhere(args, pipelineconfig);
		testRequiredColumns(args, pipelineconfig);
		testRequiredColumnsWithWhere(args, pipelineconfig);
		testRequiredColumnsWithWhereGreaterThan(args, pipelineconfig);
		testRequiredColumnsWithWhereLessThan(args, pipelineconfig);
		testRequiredColumnsWithWhereGreaterThanEquals(args, pipelineconfig);
		testRequiredColumnsWithWhereLessThanEquals(args, pipelineconfig);
		testRequiredColumnsWithWhereLiteralFirst(args, pipelineconfig);
		testRequiredColumnsWithWhereColumnEquals(args, pipelineconfig);
		testAllColumnsCount(args, pipelineconfig);
		testAllColumnsCountWithWhere(args, pipelineconfig);
		testAllColumnsSumWithWhere(args, pipelineconfig);
		testAllColumnsMinWithWhere(args, pipelineconfig);
		testAllColumnsMaxWithWhere(args, pipelineconfig);
		testRequiredColumnsJoin(args, pipelineconfig);
		testRequiredColumnsJoinCarrierSpecific(args, pipelineconfig);
		testCountAllColumnsWithWhereAndJoin(args, pipelineconfig);
		testPrintAllColumnsCountWithWhereAndJoin(args, pipelineconfig);
		testRequiredColumnsJoinTwoTables(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesWhere(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesCount(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesCountWhere(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesColumnCountWhere(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesColumnSumWhere(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesColumnMinWhere(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesColumnMaxWhere(args, pipelineconfig);
		testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter(args, pipelineconfig);
		testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter(args, pipelineconfig);
		testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter(args, pipelineconfig);
		testMultipleAllColumnsAndOrCondition(args, pipelineconfig);
		testMultipleAllColumnsOrAndCondition(args, pipelineconfig);
		testNumberOfFlightsByCarrier(args, pipelineconfig);
		testNumberOfFlightsByDayOfWeek(args, pipelineconfig);
		testNumberOfFlightsCancelled(args, pipelineconfig);
		testNumberOfFlightsDiverted(args, pipelineconfig);
		testTotalDistanceFlownByCarrier(args, pipelineconfig);
		testDayOfWeekWithMostFlights(args, pipelineconfig);
		testMonthOfYearWithMostFlights(args, pipelineconfig);
		testAirportsWithDepartures(args, pipelineconfig);
		testAirportsWithArrivals(args, pipelineconfig);
		testDelayTimeByDayOfWeek(args, pipelineconfig);
		testTotalDelayTimeByMonthOfYear(args, pipelineconfig);
		testAverageDelayByDestinationAirport(args, pipelineconfig);
		testFlightsCancelledAndCancellationCode(args, pipelineconfig);
		testFlightsCancelledDueToWeather(args, pipelineconfig);
		testFlightsDivertedDueToWeather(args, pipelineconfig);
		testFlightsDivertedDueToWeatherSortBy(args, pipelineconfig);
		testFlightsjoinGroupBy(args, pipelineconfig);
		testFlightsDistinctUniqueCarrier(args, pipelineconfig);
		testFlightsDistinctUniqueCarrierWithWhere(args, pipelineconfig);
		testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere(args, pipelineconfig);
		testAllColumnsAvg(args, pipelineconfig);
		testAllColumnsAvgArrDelayPerCarrier(args, pipelineconfig);
		testAllColumnsAvgArrDelayPerCarrierWithWhere(args, pipelineconfig);
		testRequiredColumnsMonthDayAvgArrDelayPerCarrier(args, pipelineconfig);
		testCountAvgMinMaxSumArrDelayPerCarrier(args, pipelineconfig);
		testColumnLength(args, pipelineconfig);
		testRequiredColumnWithLength(args, pipelineconfig);
		testRequiredColumnWithMultipleLengths(args, pipelineconfig);
		testRequiredColumnWithLengthsAndLowercase(args, pipelineconfig);
		testRequiredColumnWithLengthsAndUppercase(args, pipelineconfig);
		testRequiredColumnTrim(args, pipelineconfig);
		testRequiredColumnBase64Encode(args, pipelineconfig);
		testRequiredColumnSubStringAlias(args, pipelineconfig);
		testRequiredColumnSubString(args, pipelineconfig);
		testRequiredColumnNormailizeSpaces(args, pipelineconfig);
		testDate(args, pipelineconfig);
		testDateWithCount(args, pipelineconfig);
		testSumWithMultuplication(args, pipelineconfig);
		testSumWithAddition(args, pipelineconfig);
		testSumWithSubtraction(args, pipelineconfig);
		testSumWithBase64Encode(args, pipelineconfig);
		testSumWithColumnAndLength(args, pipelineconfig);
		testSelectWithAggFunctionWithGroupBy(args, pipelineconfig);
		testSumWithDivision(args, pipelineconfig);
		testSumWithSubtractionAndMultiplication(args, pipelineconfig);
		testSumWithAdditionAndMultiplication(args, pipelineconfig);
		testSelectWithWhereIn(args, pipelineconfig);
		testSelectCountWithWhereLike(args, pipelineconfig);
		testSelectSumWithWhereLike(args, pipelineconfig);
		testSelectSumWithWhereInAndLikeClause(args, pipelineconfig);
		testSelectCountWithWhereInAndLikeClause(args, pipelineconfig);
		testSelectCountWithWhereLikeAndBetweenClause(args, pipelineconfig);
		testSelectSumWithWhereLikeAndBetweenClause(args, pipelineconfig);
		testSelectSumWithNestedAbsFunction(args, pipelineconfig);
		testSelectSumWithNestedAbsFunctions(args, pipelineconfig);
		testSelectSumWithNestedAbsAndLengthFunctions(args, pipelineconfig);
		testColumnAbs(args, pipelineconfig);
		testColumnRound(args, pipelineconfig);
		testColumnCeil(args, pipelineconfig);
		testColumnFloor(args, pipelineconfig);
		testColumnPower(args, pipelineconfig);
		testColumnSqrt(args, pipelineconfig);
		testColumnExponential(args, pipelineconfig);
		testColumnLoge(args, pipelineconfig);
		testSelectSumWithNestedRound(args, pipelineconfig);
		testSelectSumWithNestedCeil(args, pipelineconfig);
		testSelectSumWithNestedFloor(args, pipelineconfig);
		testSelectSumWithNestedPower(args, pipelineconfig);
		testSelectSumWithNestedSqrt(args, pipelineconfig);
		testSelectSumWithNestedExponential(args, pipelineconfig);
		testSelectSumWithNestedloge(args, pipelineconfig);
		testSelectGroupConcatGroupBy(args, pipelineconfig);
		testColumnLengthWithExp(args, pipelineconfig);
		testColumnAbsLengthWithExp(args, pipelineconfig);
		testColumnRoundLengthWithExp(args, pipelineconfig);
		testColumnRoundLengthWithExpWithInc(args, pipelineconfig);
		testColumnCeilLengthWithExpWithInc(args, pipelineconfig);
		testColumnCeilLengthWithExp(args, pipelineconfig);
		testColumnFloorLengthWithExpWithInc(args, pipelineconfig);
		testColumnFloorLengthWithExp(args, pipelineconfig);
		testColumnLengthWithParanthesisExp(args, pipelineconfig);
		testColumnPowerLengthWithExp(args, pipelineconfig);
		testColumnSqrtLengthWithExp(args, pipelineconfig);
		testColumnExpLengthWithExp(args, pipelineconfig);
		testColumnLogLengthWithExp(args, pipelineconfig);
		testColumnLowerCaseWithUpperCaseWithExp(args, pipelineconfig);
		testColumnUpperCaseWithLowerCaseWithExp(args, pipelineconfig);
		testColumnTrimUpperCaseWithLowerCaseWithExp(args, pipelineconfig);
		testColumnBase64_EncUpperCaseWithLowerCaseWithExp(args, pipelineconfig);
		testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp(args, pipelineconfig);
		testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp(args, pipelineconfig);
		testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp(args, pipelineconfig);
		testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp(args, pipelineconfig);
		testRequiredColumnsLeftJoin(args, pipelineconfig);
		testRequiredColumnsRightJoin(args, pipelineconfig);
		testFlightsAndOr(args, pipelineconfig);
		testFlightsAndOrAnd(args, pipelineconfig);
		testFlightsAndOrAndParanthesis(args, pipelineconfig);
		testFlightsAndOrAndParanthesisOr(args, pipelineconfig);
		testFlightsAndOrAndParanthesisOrDayOfMonthPlus2(args, pipelineconfig);
		testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight(args, pipelineconfig);
		testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight(args, pipelineconfig);
		testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight(args, pipelineconfig);
		testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight(args, pipelineconfig);
		testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight(args, pipelineconfig);
		testRequiredColumnsSubSelect(args, pipelineconfig);
		testRequiredColumnsFunctionsSubSelect(args, pipelineconfig);
		testRequiredColumnsSubSelectFunctions(args, pipelineconfig);
		testRequiredColumnsSubSelectFunctionsSumCount(args, pipelineconfig);
		testRequiredColumnsFunctionsAvgSubSelect(args, pipelineconfig);
		testRequiredColumnsFunctionsAvgSumCountSubSelect(args, pipelineconfig);
		testRequiredColumnsFunctionAvgDelayFunctionsAvgSumCountSubSelect(args, pipelineconfig);
		testAllColumnsSubSelectAllColumns(args, pipelineconfig);
		testAllColumnsSubSelectAllColumnsWithWhere(args, pipelineconfig);
		testAllColumnsWithWhereSubSelectAllColumnsWithWhere(args, pipelineconfig);
		testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere(args, pipelineconfig);
		testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere(args, pipelineconfig);
		testRequiredColumnsJoinSubSelect(args, pipelineconfig);
		testRequiredColumnsJoinSubSelectAliasTable(args, pipelineconfig);
		testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable(args, pipelineconfig);
		testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere(args, pipelineconfig);
		testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere(args, pipelineconfig);
		
		
	}
	
	public void testAllColumns(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM airline ";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumns() method Exit");		
	}
	
	
	
	public void testAllColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsWithWhere() method Exit");		
	}
	
	
	
	public void testRequiredColumns(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline ";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumns() method Exit");		
	}
	
	
	
	
	public void testRequiredColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhere() method Exit");		
	}
	
	
	
	
	
	public void testRequiredColumnsWithWhereGreaterThan(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth>8.0 and airline.MonthOfYear>6.0";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Exit");
	}

	
	
	public void testRequiredColumnsWithWhereLessThan(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThan() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth<8 and airline.MonthOfYear<6";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhereLessThan() method Exit");
	}

	
	
	public void testRequiredColumnsWithWhereGreaterThanEquals(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth>=8.0 and airline.MonthOfYear>=6.0";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Exit");
	}

	
	
	public void testRequiredColumnsWithWhereLessThanEquals(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth<=8.0 and airline.MonthOfYear<=6.0";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Exit");
	}
	
	
	
	public void testRequiredColumnsWithWhereLiteralFirst(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In RequiredColumnsWithWhere() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE 8.0=airline.DayofMonth and 12.0=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In RequiredColumnsWithWhere() method Exit");
	}

	
	
	public void testRequiredColumnsWithWhereColumnEquals(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Exit");
	}
	
	
	
	public void testAllColumnsCount(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsCount() method Entry");
		
		String statement = "SELECT count(*) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnsCount() method Exit");
	}
	
	
	
	public void testAllColumnsCountWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsCountWithWhere() method Entry");
		
		String statement = "SELECT count(*) FROM airline WHERE airline.DayofMonth=airline.MonthOfYear";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsCountWithWhere() method Exit");
	}
	
	
	
	public void testAllColumnsSumWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsSumWithWhere() method Entry");
		
		String statement = "SELECT sum(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsSumWithWhere() method Exit");
	}
	
	
	
	
	public void testAllColumnsMinWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsMinWithWhere() method Entry");
		
		String statement = "SELECT min(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testAllColumnsMinWithWhere() method Exit");
	}
	
	
	
	public void testAllColumnsMaxWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsMaxWithWhere() method Entry");
		
		String statement = "SELECT max(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsMaxWithWhere() method Exit");
	}

	public void testRequiredColumnsJoin(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoin() method Entry");
		
		String statement = "SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoin() method Exit");
	}
	
	
	public void testRequiredColumnsJoinCarrierSpecific(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinCarrierSpecific() method Entry");
		
		String statement = "SELECT airline.ArrDelay,airline.DepDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Code,carriers.Description "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinCarrierSpecific() method Exit");
	}
	
	
	public void testCountAllColumnsWithWhereAndJoin(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testCountAllColumnsWithWhereAndJoin() method Entry");
		
		String statement = "SELECT count(*) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ' and carriers.Code<>'Code'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testCountAllColumnsWithWhereAndJoin() method Exit");
	}
	
	
	
	public void testPrintAllColumnsCountWithWhereAndJoin(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testPrintAllColumnsCountWithWhereAndJoin() method Entry");
		
		String statement = "SELECT * "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ' and carriers.Code<>'Code'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testPrintAllColumnsCountWithWhereAndJoin() method Exit");
	}
	
	
	
	public void testRequiredColumnsJoinTwoTables(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTables() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Description,airline.Origin,airports.airport "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin ";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTables() method Exit");
	}
	
	
	
	public void testRequiredColumnsJoinTwoTablesWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Description,airline.Origin,airports.airport "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin "
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesWhere() method Exit");
	}
	
	
	
	
	public void testRequiredColumnsJoinTwoTablesCount(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesCount() method Entry");
		String statement = "SELECT count(*) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin ";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesCount() method Exit");
	}
	
	
	
	public void testRequiredColumnsJoinTwoTablesCountWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesCountWhere() method Entry");
		String statement = "SELECT count(*) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin " 
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesCountWhere() method Exit");
	}

	
	
	public void testRequiredColumnsJoinTwoTablesColumnCountWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnCountWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,count(*) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin " 
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesColumnCountWhere() method Exit");
	}
	
	
	public void testRequiredColumnsJoinTwoTablesColumnSumWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin " 
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhere() method Exit");
	}
	
	
	
	public void testRequiredColumnsJoinTwoTablesColumnMinWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnMinWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,min(airline.ArrDelay) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin " 
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesColumnMinWhere() method Exit");
	}
	
	
	public void testRequiredColumnsJoinTwoTablesColumnMaxWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnMaxWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,max(airline.ArrDelay) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin " 
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesColumnMaxWhere() method Exit");
	}
	
	
	public void testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere() method Entry");
		String statement = "SELECT airports.iata,airline.UniqueCarrier,sum(airline.ArrDelay) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin " 
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airports.iata,airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere() method Exit");
	}
	
	
	
	
	public void testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay) "
				+ "FROM airline group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter() method Exit");
	}
	
	
	
	
	
	public void testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Entry");
		String statement = "SELECT airline.UniqueCarrier,min(airline.ArrDelay),count(*),max(airline.ArrDelay),sum(airline.ArrDelay) "
				+ "FROM airline where airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Exit");
	}
	
	
	
	
	public void testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Entry");
		String statement = "SELECT airports.iata,airline.UniqueCarrier,sum(airline.ArrDelay),min(airline.ArrDelay),max(airline.ArrDelay),count(*) "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code "
				+ " inner join airports on airports.iata = airline.Origin " 
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airports.iata,airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.add(airportssample, "airports", airportsheader, airportstype)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Exit");
	}
	
	
	
	
	public void testMultipleAllColumnsAndOrCondition(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testMultipleAllColumnsAndOrCondition() method Entry");
		String statement = "SELECT * from airline "				
				+ "WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testMultipleAllColumnsAndOrCondition() method Exit");
	}
	
	
	
	
	public void testMultipleAllColumnsOrAndCondition(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testMultipleAllColumnsOrAndCondition() method Entry");
		String statement = "SELECT * from airline "				
				+ "WHERE (airline.DayofMonth=8 or airline.MonthOfYear=12)";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testMultipleAllColumnsOrAndCondition() method Exit");
	}
	
	
	
	
	public void testNumberOfFlightsByCarrier(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testNumberOfFlightsByCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier, count(*) FROM airline GROUP BY airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testNumberOfFlightsByCarrier() method Exit");
	}
	
	
	
	public void testNumberOfFlightsByDayOfWeek(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testNumberOfFlightsByDayOfWeek() method Entry");
		String statement = "SELECT airline.DayOfWeek, count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testNumberOfFlightsByDayOfWeek() method Exit");
	}
	
	
	
	public void testNumberOfFlightsCancelled(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testNumberOfFlightsCancelled() method Entry");
		String statement = "SELECT count(*) FROM airline WHERE airline.Cancelled = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testNumberOfFlightsCancelled() method Exit");
	}
	
	
	
	public void testNumberOfFlightsDiverted(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testNumberOfFlightsDiverted() method Entry");
		String statement = "SELECT count(*) FROM airline WHERE airline.Diverted = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testNumberOfFlightsDiverted() method Exit");
	}
	
	
	
	
	public void testTotalDistanceFlownByCarrier(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testTotalDistanceFlownByCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier, sum(airline.Distance) FROM airline GROUP BY airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testTotalDistanceFlownByCarrier() method Exit");
	}
	
	
	
	public void testDayOfWeekWithMostFlights(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testDayOfWeekWithMostFlights() method Entry");
		String statement = "SELECT airline.DayOfWeek, count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testDayOfWeekWithMostFlights() method Exit");
	}
	
	
	
	public void testMonthOfYearWithMostFlights(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testMonthOfYearWithMostFlights() method Entry");
		String statement = "SELECT airline.MonthOfYear, count(*) FROM airline GROUP BY airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testMonthOfYearWithMostFlights() method Exit");
	}
	
	
	
	public void testAirportsWithDepartures(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAirportsWithDepartures() method Entry");
		String statement = "SELECT airline.Origin, count(*) FROM airline GROUP BY airline.Origin";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAirportsWithDepartures() method Exit");
	}
	
	
	
	public void testAirportsWithArrivals(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAirportsWithArrivals() method Entry");
		String statement = "SELECT airline.Dest, count(*) FROM airline GROUP BY airline.Dest";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAirportsWithArrivals() method Exit");
	}
	
	
	
	public void testDelayTimeByDayOfWeek(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testDelayTimeByDayOfWeek() method Entry");
		String statement = "SELECT airline.DayOfWeek, sum(airline.ArrDelay),count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testDelayTimeByDayOfWeek() method Exit");
	}
	
	
	
	
	public void testTotalDelayTimeByMonthOfYear(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testTotalDelayTimeByMonthOfYear() method Entry");
		String statement = "SELECT airline.MonthOfYear, sum(airline.ArrDelay), count(*) FROM airline GROUP BY airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testTotalDelayTimeByMonthOfYear() method Exit");
	}
	
	
	
	public void testAverageDelayByDestinationAirport(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAverageDelayByDestinationAirport() method Entry");
		String statement = "SELECT airline.Dest, sum(airline.ArrDelay),avg(airline.ArrDelay) AvgDelay FROM airline GROUP BY airline.Dest";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAverageDelayByDestinationAirport() method Exit");
	}
	
	
	
	public void testFlightsCancelledAndCancellationCode(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsCancelledAndCancellationCode() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Cancelled,airline.CancellationCode FROM airline WHERE airline.Cancelled = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsCancelledAndCancellationCode() method Exit");
	}
	
	
	
	
	public void testFlightsCancelledDueToWeather(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsCancelledDueToWeather() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Cancelled,airline.CancellationCode FROM airline WHERE airline.Cancelled = 1 AND airline.CancellationCode = 'B'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsCancelledDueToWeather() method Exit");
	}
	
	
	
	public void testFlightsDivertedDueToWeather(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsDivertedDueToWeather() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Diverted,airline.WeatherDelay FROM airline WHERE airline.Diverted = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsDivertedDueToWeather() method Exit");
	}
	
	
	
	
	public void testFlightsDivertedDueToWeatherSortBy(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsDivertedDueToWeatherSortBy() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 or airline.MonthOfYear=12 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsDivertedDueToWeatherSortBy() method Exit");
	}
	
	
	
	
	public void testFlightsjoinGroupBy(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsjoinGroupBy() method Entry");
		String statement = "SELECT airlines.Origin,airports.airport,count(*) FROM airlines inner join airports on airports.iata = airlines.Origin GROUP BY airlines.Origin,airports.airport";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airlines", airlineheader, airlineheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsjoinGroupBy() method Exit");
	}
	
	
	
	
	public void testFlightsDistinctUniqueCarrier(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsDistinctUniqueCarrier() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier from airlines";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airlines", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsDistinctUniqueCarrier() method Exit");
	}
	
	
	
	
	public void testFlightsDistinctUniqueCarrierWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier from airlines where airlines.UniqueCarrier <> 'UniqueCarrier'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airlines", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsDistinctUniqueCarrierWithWhere() method Exit");
	}
	
	
	
	public void testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier,airlines.AirlineYear from airlines where airlines.UniqueCarrier <> 'UniqueCarrier' order by airlines.AirlineYear,airlines.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airlines", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Exit");
	}
	
	
	
	public void testAllColumnsAvg(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsAvg() method Entry");
		
		String statement = "SELECT avg(airline.ArrDelay) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsAvg() method Exit");
	}
	
	
	
	public void testAllColumnsAvgArrDelayPerCarrier(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsAvgArrDelayPerCarrier() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,avg(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsAvgArrDelayPerCarrier() method Exit");
	}
	
	
	
	public void testAllColumnsAvgArrDelayPerCarrierWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsAvgArrDelayPerCarrierWithWhere() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,avg(airline.ArrDelay) FROM airline where airline.DayOfWeek=1 group by airline.UniqueCarrier";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsAvgArrDelayPerCarrierWithWhere() method Exit");
	}
	
	
	
	public void testRequiredColumnsMonthDayAvgArrDelayPerCarrier(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsMonthDayAvgArrDelayPerCarrier() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,avg(airline.ArrDelay) avgarrdelay, "
				+ "sum(airline.ArrDelay) as sumarrdelay, count(*) as ct, min(airline.ArrDelay) as minarrdelay, max(airline.ArrDelay) as maxarrdelay"
				+ " FROM airline group by airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear order by airline.UniqueCarrier, avgarrdelay";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsMonthDayAvgArrDelayPerCarrier() method Exit");
	}
	
	
	public void testCountAvgMinMaxSumArrDelayPerCarrier(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testCountAvgMinMaxSumArrDelayPerCarrier() method Entry");
		
		String statement = "SELECT avg(airline.ArrDelay) avgarrdelay, "
				+ "sum(airline.ArrDelay) as sumarrdelay, count(*) as ct, min(airline.ArrDelay) as minarrdelay, max(airline.ArrDelay) as maxarrdelay"
				+ " FROM airline group by airline.MonthOfYear order by avgarrdelay";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testCountAvgMinMaxSumArrDelayPerCarrier() method Exit");
	}
	
	
	
	public void testColumnLength(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnLength() method Entry");
		
		String statement = "SELECT length(airline.Origin)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnLength() method Exit");
	}
	
	
	
	public void testRequiredColumnWithLength(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnWithLength() method Entry");
		
		String statement = "SELECT airline.Origin,length(airline.Origin)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnWithLength() method Exit");
	}
	
	
	
	
	public void testRequiredColumnWithMultipleLengths(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnWithMultipleLengths() method Entry");
		
		String statement = "SELECT airline.Origin,length(airline.Origin),length(airline.Dest)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnWithMultipleLengths() method Exit");
	}
	
	
	
	
	public void testRequiredColumnWithLengthsAndLowercase(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnWithLengthsAndLowercase() method Entry");
		
		String statement = "SELECT lowercase(airline.Origin),lowercase(airline.Dest),length(airline.Origin),length(airline.Dest)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnWithLengthsAndLowercase() method Exit");
	}
	
	
	
	public void testRequiredColumnWithLengthsAndUppercase(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnWithLengthsAndUppercase() method Entry");
		
		String statement = "SELECT uppercase(airline.Origin),uppercase(airline.Dest),length(airline.Origin),length(airline.Dest)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnWithLengthsAndUppercase() method Exit");
	}
	
	
	
	public void testRequiredColumnTrim(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnTrim() method Entry");
		
		String statement = "SELECT trim(airline.Origin),trim(airline.Dest)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnTrim() method Exit");
	}
	
	
	public void testRequiredColumnBase64Encode(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnBase64Encode() method Entry");
		
		String statement = "SELECT base64encode(airline.Origin),base64encode(airline.Dest)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnBase64Encode() method Exit");
	}
	
	
	public void testRequiredColumnSubStringAlias(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnSubStringAlias() method Entry");
		
		String statement = "SELECT airline.Origin,substring(airline.Origin,0,1) as substr  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnSubStringAlias() method Exit");
	}
	
	
	public void testRequiredColumnSubString(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnSubString() method Entry");
		
		String statement = "SELECT airline.Origin,substring(airline.Origin,0,1),airline.Dest,substring(airline.Dest,0,2) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnSubString() method Exit");
	}
	
	
	public void testRequiredColumnNormailizeSpaces(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnNormailizeSpaces() method Entry");
		
		String statement = "SELECT normalizespaces(airline.Dest),normalizespaces(' This is   good  work') eg FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testRequiredColumnNormailizeSpaces() method Exit");
	}
	
	
	public void testDate(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testDate() method Entry");
		
		String statement = "SELECT normalizespaces(' This is   good  work') normspace,currentisodate() isodate FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testDate() method Exit");
	}
	
	
	
	public void testDateWithCount(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testDateWithCount() method Entry");
		
		String statement = "SELECT normalizespaces(' This is   good  work') normspace,currentisodate() isodate,count(*) numrec FROM airline group by airline.AirlineYear";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testDateWithCount() method Exit");
	}
	
	
	public void testSumWithMultuplication(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testDate() method Entry");
		
		String statement = "SELECT sum(airline.ArrDelay * 2) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testDate() method Exit");
	}
	
	
	
	public void testSumWithAddition(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSumWithAddition() method Entry");
		
		String statement = "SELECT sum(airline.ArrDelay + 2) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testSumWithAddition() method Exit");
	}
	
	
	public void testSumWithSubtraction(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSumWithSubtraction() method Entry");
		
		String statement = "SELECT sum(airline.ArrDelay - 2) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testSumWithSubtraction() method Exit");
	}
	
	
	public void testSumWithBase64Encode(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSumWithBase64Encode() method Entry");
		
		String statement = "SELECT base64encode(airline.Origin) originalias,sum(airline.ArrDelay - 2) FROM airline group by airline.Origin";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSumWithBase64Encode() method Exit");
	}
	
	
	
	public void testSumWithColumnAndLength(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSumWithColumnAndLength() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,length(airline.UniqueCarrier),sum(airline.ArrDelay - 2) FROM airline group by airline.UniqueCarrier";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSumWithColumnAndLength() method Exit");
	}
	
	
	public void testSelectWithAggFunctionWithGroupBy(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectWithAggFunctionColumnsWithoutGroupBy() method Entry");
		String statement = "SELECT sum(airline.ArrDelay - 2) FROM airline group by airline.MonthOfYear,airline.DayofMonth";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectWithAggFunctionColumnsWithoutGroupBy() method Exit");
	}
	
	
	
	public void testSumWithDivision(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSumWithDivision() method Entry");
		
		String statement = "SELECT sum(airline.ArrDelay / 2) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testSumWithDivision() method Exit");
	}
	
	
	public void testSumWithSubtractionAndMultiplication(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSumWithSubtractionAndMultiplication() method Entry");
		
		String statement = "SELECT sum((airline.ArrDelay - 2) * 3.5) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testSumWithSubtractionAndMultiplication() method Exit");
	}
	
	
	
	public void testSumWithAdditionAndMultiplication(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSumWithAdditionAndMultiplication() method Entry");
		
		String statement = "SELECT sum((airline.ArrDelay + 2) * 2.5) FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testSumWithAdditionAndMultiplication() method Exit");
	}
	
	
	public void testSelectWithWhereIn(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectWithWhereIn() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11,12) ";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11) ";
		spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		
		statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (12) ";
		spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectWithWhereIn() method Exit");
	}
	
	
	public void testSelectCountWithWhereLike(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectCountWithWhereLike() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectCountWithWhereLike() method Exit");
	}
	
	
	public void testSelectSumWithWhereLike(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithWhereLike() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithWhereLike() method Exit");
	}
	
	
	public void testSelectSumWithWhereInAndLikeClause(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithWhereInAndLikeClause() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11,12) and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithWhereInAndLikeClause() method Exit");
	}
	
	
	
	public void testSelectCountWithWhereInAndLikeClause(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectCountWithWhereInAndLikeClause() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.MonthOfYear in (11,12) and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectCountWithWhereInAndLikeClause() method Exit");
	}
	
	
	public void testSelectCountWithWhereLikeAndBetweenClause(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectCountWithWhereLikeAndBetweenClause() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectCountWithWhereLikeAndBetweenClause() method Exit");
	}
	
	
	public void testSelectSumWithWhereLikeAndBetweenClause(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithWhereLikeAndBetweenClause() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithWhereLikeAndBetweenClause() method Exit");
	}
	
	
	
	public void testSelectSumWithNestedAbsFunction(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedAbsFunction() method Entry");
		String statement = "SELECT sum(abs(airline.MonthOfYear) + airline.ArrDelay) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedAbsFunction() method Exit");
	}
	
	
	
	public void testSelectSumWithNestedAbsFunctions(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedAbsFunctions() method Entry");
		String statement = "SELECT sum(abs(airline.MonthOfYear) + abs(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedAbsFunctions() method Exit");
	}
	
	
	public void testSelectSumWithNestedAbsAndLengthFunctions(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedAbsAndLengthFunctions() method Entry");
		String statement = "SELECT sum(abs(length(airline.Origin)) + abs(length(airline.Dest))) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedAbsAndLengthFunctions() method Exit");
	}
	
	
	
	public void testColumnAbs(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnAbs() method Entry");
		
		String statement = "SELECT abs(airline.ArrDelay)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnAbs() method Exit");
	}
	
	
	
	public void testColumnRound(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnRound() method Entry");
		
		String statement = "SELECT airline.ArrDelay, round(airline.ArrDelay)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnRound() method Exit");
	}
	
	
	
	public void testColumnCeil(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnCeil() method Entry");
		
		String statement = "SELECT airline.ArrDelay, ceil(airline.ArrDelay)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnCeil() method Exit");
	}
	
	
	
	public void testColumnFloor(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnFloor() method Entry");
		
		String statement = "SELECT airline.ArrDelay,floor(airline.ArrDelay)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnFloor() method Exit");
	}
	
	
	
	public void testColumnPower(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnPower() method Entry");
		
		String statement = "SELECT airline.ArrDelay, pow(airline.ArrDelay, 2) powcal  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnPower() method Exit");
	}
	
	
	
	public void testColumnSqrt(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnSqrt() method Entry");
		
		String statement = "SELECT airline.MonthOfYear, sqrt(airline.MonthOfYear)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnSqrt() method Exit");
	}
	
	
	
	public void testColumnExponential(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnExponential() method Entry");
		
		String statement = "SELECT exp(airline.MonthOfYear)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnExponential() method Exit");
	}
	
	
	
	
	public void testColumnLoge(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnloge() method Entry");
		
		String statement = "SELECT loge(airline.MonthOfYear) as log  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnloge() method Exit");
	}
	
	
	public void testSelectSumWithNestedRound(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedRound() method Entry");
		String statement = "SELECT sum(round(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedRound() method Exit");
	}
	
	
	public void testSelectSumWithNestedCeil(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedCeil() method Entry");
		String statement = "SELECT sum(ceil(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedCeil() method Exit");
	}
	
	
	public void testSelectSumWithNestedFloor(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedFloor() method Entry");
		String statement = "SELECT sum(floor(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedFloor() method Exit");
	}
	
	
	public void testSelectSumWithNestedPower(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedPower() method Entry");
		String statement = "SELECT sum(pow(airline.MonthOfYear, 2)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedPower() method Exit");
	}
	
	
	public void testSelectSumWithNestedSqrt(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedSqrt() method Entry");
		String statement = "SELECT sum(sqrt(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedSqrt() method Exit");
	}
	
	
	public void testSelectSumWithNestedExponential(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedExponential() method Entry");
		String statement = "SELECT sum(exp(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedExponential() method Exit");
	}
	
	
	
	public void testSelectSumWithNestedloge(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectSumWithNestedloge() method Entry");
		String statement = "SELECT sum(loge(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectSumWithNestedloge() method Exit");
	}
	
	
	public void testSelectGroupConcatGroupBy(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testSelectGroupConcatGroupBy() method Entry");
		String statement = "SELECT airline.DayofMonth, grpconcat(airline.TailNum, '||') FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG' group by airline.DayofMonth,airline.TailNum";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testSelectGroupConcatGroupBy() method Exit");
	}
	
	
	
	public void testColumnLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnLengthWithExp() method Entry");
		
		String statement = "SELECT length(airline.Origin + airline.Dest)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnLengthWithExp() method Exit");
	}
	
	
	
	
	public void testColumnAbsLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnAbsLengthWithExp() method Entry");
		
		String statement = "SELECT abs(length(airline.Origin + airline.Dest))  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnAbsLengthWithExp() method Exit");
	}
	
	
	
	public void testColumnRoundLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnRoundLengthWithExp() method Entry");
		
		String statement = "SELECT round(length(airline.Origin + airline.Dest) + 0.4)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnRoundLengthWithExp() method Exit");
	}
	
	
	
	
	public void testColumnRoundLengthWithExpWithInc(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnRoundLengthWithExpWithInc() method Entry");
		
		String statement = "SELECT round(length(airline.Origin + airline.Dest) + 0.6)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnRoundLengthWithExpWithInc() method Exit");
	}
	
	
	
	public void testColumnCeilLengthWithExpWithInc(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnCeilLengthWithExpWithInc() method Entry");
		
		String statement = "SELECT ceil(length(airline.Origin + airline.Dest) + 0.6)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnCeilLengthWithExpWithInc() method Exit");
	}
	
	
	
	public void testColumnCeilLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnCeilLengthWithExp() method Entry");
		
		String statement = "SELECT ceil(length(airline.Origin + airline.Dest) + 0.4)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnCeilLengthWithExp() method Exit");
	}
	
	
	
	
	public void testColumnFloorLengthWithExpWithInc(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnFloorLengthWithExpWithInc() method Entry");
		
		String statement = "SELECT floor(length(airline.Origin + airline.Dest) + 0.6)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnFloorLengthWithExpWithInc() method Exit");
	}
	
	
	
	public void testColumnFloorLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnFloorLengthWithExp() method Entry");
		
		String statement = "SELECT floor(length(airline.Origin + airline.Dest) + 0.4)  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnFloorLengthWithExp() method Exit");
	}
	
	
	
	public void testColumnLengthWithParanthesisExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnLengthWithParanthesisExp() method Entry");
		
		String statement = "SELECT (length(airline.Origin + airline.Dest) + 0.4) paransum  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnLengthWithParanthesisExp() method Exit");
	}
	
	
	
	
	public void testColumnPowerLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnPowerLengthWithExp() method Entry");
		
		String statement = "SELECT pow(length(airline.Origin + airline.Dest), 2) powlen  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnPowerLengthWithExp() method Exit");
	}
	
	
	
	public void testColumnSqrtLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnSqrtLengthWithExp() method Entry");
		
		String statement = "SELECT sqrt(length(airline.Origin + airline.Dest)) sqrtlen  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnSqrtLengthWithExp() method Exit");
	}
	
	
	
	public void testColumnExpLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnExpLengthWithExp() method Entry");
		
		String statement = "SELECT exp(length(airline.Origin + airline.Dest)) explen  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnExpLengthWithExp() method Exit");
	}
	
	
	
	public void testColumnLogLengthWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnLogLengthWithExp() method Entry");
		
		String statement = "SELECT loge(length(airline.Origin + airline.Dest)) loglen  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnLogLengthWithExp() method Exit");
	}
	
	
	
	
	public void testColumnLowerCaseWithUpperCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnLowerCaseWithUpperCaseWithExp() method Entry");
		
		String statement = "SELECT lowercase(uppercase(airline.Origin + airline.Dest + 'low') + 'UPP') lowup  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnLowerCaseWithUpperCaseWithExp() method Exit");
	}
	
	
	
	public void testColumnUpperCaseWithLowerCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnUpperCaseWithLowerCaseWithExp() method Entry");
		
		String statement = "SELECT uppercase(lowercase(airline.Origin + airline.Dest + 'LOW') + 'upp') uplow  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnUpperCaseWithLowerCaseWithExp() method Exit");
	}
	
	
	
	public void testColumnTrimUpperCaseWithLowerCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnTrimUpperCaseWithLowerCaseWithExp() method Entry");
		
		String statement = "SELECT trim('     ' + uppercase(lowercase(airline.Origin + airline.Dest + 'LOW') + 'upp') + ' Spaces      ') uplow  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnTrimUpperCaseWithLowerCaseWithExp() method Exit");
	}
	
	
	
	
	
	public void testColumnBase64_EncUpperCaseWithLowerCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnBase64_EncUpperCaseWithLowerCaseWithExp() method Entry");
		
		String statement = "SELECT base64encode('     ' + uppercase(lowercase(airline.Origin + airline.Dest + 'LOW') + 'upp') + ' Spaces      ') encstring  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnBase64_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}
	
	
	
	
	public void testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");
		
		String statement = "SELECT base64decode(base64encode('     ' + uppercase(lowercase(airline.Origin + airline.Dest + 'LOW') + 'upp') + ' Spaces      ')) decstring  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}
	
	
	
	
	public void testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");
		
		String statement = "SELECT normalizespaces(base64decode(base64encode('     ' + uppercase(lowercase(airline.Origin + airline.Dest + 'LOW') + 'upp') + ' Spaces      '))) normalizedstring  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}
	
	
	
	public void testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");
		
		String statement = "SELECT substring(base64decode(base64encode('     ' + uppercase(lowercase(airline.Origin + airline.Dest + 'LOW') + 'upp') + ' Spaces      ')), 0 , 6) substr  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}
	
	
	
	
	public void testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");
		
		String statement = "SELECT normalizespaces(substring(base64decode(base64encode('     ' + uppercase(lowercase(airline.Origin + airline.Dest + 'LOW') + 'upp') + ' Spaces      ')), 0 , 6)) normsubstr  FROM airline";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());		
		log.info("In testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}
	
	
	
	
	public void testRequiredColumnsLeftJoin(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsLeftJoin() method Entry");
		
		String statement = "SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code "
				+ "FROM carriers left join airline on airline.UniqueCarrier = carriers.Code";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsLeftJoin() method Exit");
	}
	
	
	
	
	public void testRequiredColumnsRightJoin(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsRightJoin() method Entry");
		
		String statement = "SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code "
				+ "FROM airline right join carriers on airline.UniqueCarrier = carriers.Code";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsRightJoin() method Exit");
	}
	
	
	
	
	public void testFlightsAndOr(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOr() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 or airline.DayOfWeek=3 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOr() method Exit");
	}
	
	
	
	
	public void testFlightsAndOrAnd(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAnd() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 or airline.DayOfWeek=3 and airline.DayofMonth=1 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAnd() method Exit");
	}
	
	
	
	public void testFlightsAndOrAndParanthesis(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesis() method Entry");
		String statement = "SELECT * FROM airline WHERE (airline.DayofMonth=8 and airline.MonthOfYear=12) or (airline.DayOfWeek=3 and airline.DayofMonth=1) ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesis() method Exit");
	}
	
	
	
	public void testFlightsAndOrAndParanthesisOr(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOr() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and airline.Origin='LIH' ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesisOr() method Exit");
	}
	
	
	
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlus2(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth+2 = 8 and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and airline.Origin='LIH' ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() method Exit");
	}
	
	
	
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth+2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() method Exit");
	}
	
	
	
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth + airline.DayOfWeek and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() method Exit");
	}
	
	
	
	public void testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth - 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() method Exit");
	}
	
	
	
	
	public void testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth * 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() method Exit");
	}
	
	
	
	
	public void testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 4.0 <= airline.DayofMonth / 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() method Exit");
	}
	
	
	
	public void testRequiredColumnsSubSelect(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsSubSelect() method Entry");
		String statement = "SELECT AirlineYear,MonthOfYear FROM (select * from airline)";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsSubSelect() method Exit");		
	}
	
	
	
	public void testRequiredColumnsFunctionsSubSelect(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsFunctionsSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay) sumdelay FROM (select * from airline) group by UniqueCarrier";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsFunctionsSubSelect() method Exit");		
	}
	
	
	
	public void testRequiredColumnsSubSelectFunctions(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsSubSelectFunctions() method Entry");
		String statement = "SELECT sumarrdelay FROM (select sum(airline.ArrDelay) sumarrdelay from airline  group by airline.UniqueCarrier)";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsSubSelectFunctions() method Exit");		
	}
	
	
	
	public void testRequiredColumnsSubSelectFunctionsSumCount(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsSubSelectFunctionsSumCount() method Entry");
		String statement = "SELECT sumarrdelay,countrec FROM (select sum(airline.ArrDelay) sumarrdelay, count(*) countrec from airline  group by airline.UniqueCarrier)";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsSubSelectFunctionsSumCount() method Exit");		
	}
	
	
	
	public void testRequiredColumnsFunctionsAvgSubSelect(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay) FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsFunctionsAvgSubSelect() method Exit");		
	}
	
	
	
	public void testRequiredColumnsFunctionsAvgSumCountSubSelect(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay),sum(ArrDelay),count(*) cnt FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Exit");		
	}
	
	
	
	
	public void testRequiredColumnsFunctionAvgDelayFunctionsAvgSumCountSubSelect(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Entry");
		String statement = "SELECT avg(avgdelay) delay from(SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay) avgdelay,sum(ArrDelay) sumdelay,count(*) cnt FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear)";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Exit");		
	}
	
	
	
	public void testAllColumnsSubSelectAllColumns(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsSubSelectAllColumns() method Entry");
		String statement = "SELECT * FROM (select * from airline) ";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsSubSelectAllColumns() method Exit");		
	}
	
	
	
	
	
	public void testAllColumnsSubSelectAllColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM (select * from airline where airline.DayofMonth = 12) ";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsSubSelectAllColumnsWithWhere() method Exit");		
	}
	
	
	
	
	public void testAllColumnsWithWhereSubSelectAllColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testAllColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM (select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testAllColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");		
	}
	
	
	
	
	public void testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM "
				+ "(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");		
	}
	
	
	
	
	public void testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT sqrt(abs(ArrDelay)) sqrtabs FROM "
				+ "(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3";
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere() method Exit");		
	}
	
	
	
	
	public void testRequiredColumnsJoinSubSelect(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinSubSelect() method Entry");
		
		String statement = "SELECT * FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear)";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinSubSelect() method Exit");
	}
	
	
	
	public void testRequiredColumnsJoinSubSelectAliasTable(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsJoinSubSelectAliasTable() method Entry");
		
		String statement = "SELECT ijres.DayofMonth,ijres.MonthOfYear,ijres.UniqueCarrier,ijres.Code FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear) ijres";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsJoinSubSelectAliasTable() method Exit");
	}
	
	
	
	public void testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable() method Entry");
		
		String statement = "SELECT ijres.DayofMonth,ijres.MonthOfYear,ijres.UniqueCarrier,ijres.Code FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear) ijres inner "
				+ "join carriers on ijres.UniqueCarrier = carriers.Code";				
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable() method Exit");
	}
	
	
	
	public void testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT AirlineYear,DayOfWeek,DayofMonth FROM( SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM "
				+ "(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3)";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");		
	}
	
	
	
	public void testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("In testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT AirlineYear,MonthOfYear,DayOfWeek,DayofMonth FROM( SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM "
				+ "(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3) where MonthOfYear=12";
		
		
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airlineheadertypes)
				.setHdfs(args[0]).setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		spsql.saveAsTextFile(new URI(args[0]), args[2] + sqloutput + System.currentTimeMillis());
		log.info("In testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");		
	}
}
