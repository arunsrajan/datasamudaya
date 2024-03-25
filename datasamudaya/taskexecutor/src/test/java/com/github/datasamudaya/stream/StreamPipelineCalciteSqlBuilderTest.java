/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.sql.build.StreamPipelineCalciteSqlBuilder;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;

public class StreamPipelineCalciteSqlBuilderTest extends StreamPipelineBaseTestCommon {
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
	Logger log = Logger.getLogger(StreamPipelineCalciteSqlBuilderTest.class);
	List<String> airportsheader = Arrays.asList("iata", "airport", "city", "state", "country", "latitude", "longitude");
	List<SqlTypeName> airportstype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);

	@BeforeClass
	public static void pipelineSetup() throws Exception, Throwable {
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("1");
		pipelineconfig.setBatchsize(DataSamudayaConstants.EMPTY + Runtime.getRuntime().availableProcessors());
		if ("false".equals(pipelineconfig.getLocal())) {
			pipelineconfig.setUseglobaltaskexecutors(true);
			String teid = Utils.getUUID();
			Utils.launchContainersUserSpec("arun", teid, 6, 1000, 1);
			pipelineconfig.setTejobid(teid);
			pipelineconfig.setUser("arun");
		}
	}
	
	@After
	public void resetJobId() {
		if ("false".equals(pipelineconfig.getLocal())) {
			pipelineconfig.setJobid(null);
		}
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumns() throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM airline ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 29);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(46360, total);

		log.info("In testAllColumns() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 29);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(132, total);

		log.info("In testAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 3);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(46360, total);

		log.info("In testRequiredColumns() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";

		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int total = 0;
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 3);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(132, total);

		log.info("In testRequiredColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereGreaterThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth>8 and airline.MonthOfYear>6\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertTrue(((Long) rec[2]) > 8);
				assertTrue((Long) rec[3] > 6);
			}
		}

		log.info("In testRequiredColumnsWithWhereGreaterThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThan() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth<8 and airline.MonthOfYear<6\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertTrue(((Long) rec[2]) < 8);
				assertTrue(((Long) rec[3]) < 6);
			}
		}

		log.info("In testRequiredColumnsWithWhereLessThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereGreaterThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth>=8 and airline.MonthOfYear>=6\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertTrue(((Long) rec[2]) >= 8.0);
				assertTrue(((Long) rec[3]) >= 6.0);
			}
		}

		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertTrue(((Long) rec[2]) <= 8);
				assertTrue(((Long) rec[3]) <= 6);
			}
		}

		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThanEqualsAndCase() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEqualsAndCase() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear, \
				case when airline.DayofMonth < 6 then 'day of month is under 6' else 'day of month is over 6' end, \
				case when airline.MonthOfYear < 6 then 'month of year is under 6' else 'month of year is over 6' end \
				FROM airline \
				WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 6);
				assertTrue(((Long) rec[2]) <= 8);
				assertTrue(((Long) rec[3]) <= 6);
			}
		}

		log.info("In testRequiredColumnsWithWhereLessThanEqualsAndCase() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhen() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhen() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear, \
				case when airline.DayofMonth < 6 then 'day of month is under 6' when airline.DayofMonth = 6 then 'day of month equals 6' else 'day of month is over 6' end, \
				case when airline.MonthOfYear < 6 then 'month of year is under 6' when airline.MonthOfYear = 6 then 'month of year equals 6' else 'month of year is over 6' end \
				FROM airline \
				WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 6);
				assertTrue(((Long) rec[2]) <= 8);
				assertTrue(((Long) rec[3]) <= 6);
			}
		}

		log.info("In testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhen() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhenExpression() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhenExpression() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear, \
				case when airline.DayofMonth < 6 then airline.DayofMonth+1 when airline.DayofMonth = 6 then airline.DayofMonth - 2 else airline.DayofMonth + 3 end, \
				case when airline.MonthOfYear < 6 then 'month of year is under 6' when airline.MonthOfYear = 6 then 'month of year equals 6' else 'month of year is over 6' end \
				FROM airline \
				WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 6);
				assertTrue(((Long) rec[2]) <= 8);
				assertTrue(((Long) rec[3]) <= 6);
			}
		}

		log.info("In testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhenExpression() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLiteralFirst() throws Exception {
		log.info("In testRequiredColumnsWithWhereLiteralFirst() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 4);
				assertEquals(8l, rec[2]);
				assertEquals(12l, rec[3]);
				log.info(Arrays.toString(rec));
			}
		}

		log.info("In testRequiredColumnsWithWhereLiteralFirst() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereColumnEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth=airline.MonthOfYear\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 4);
				assertTrue(rec[2] == rec[3]);
				log.info(Arrays.toString(rec));
			}
		}

		log.info("In testRequiredColumnsWithWhereColumnEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCount() throws Exception {
		log.info("In testRequiredColumnsCount() method Entry");

		String statement = "SELECT count(*) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(46360l, records.get(0).get(0)[0]);

		log.info("In testRequiredColumnsCount() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCountSum() throws Exception {
		log.info("In testAllColumnsCountSum() method Entry");

		String statement = "SELECT count(*),sum(airline.ArrDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(46360l, records.get(0).get(0)[0]);
		assertEquals(-63278l, records.get(0).get(0)[1]);

		log.info("In testAllColumnsCountSum() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCountWithWhere() throws Exception {
		log.info("In testRequiredColumnsCountWithWhere() method Entry");

		String statement = "SELECT count(*) FROM airline WHERE airline.DayofMonth=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(1522l, records.get(0).get(0)[0]);

		log.info("In testRequiredColumnsCountWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsSumWithWhere() throws Exception {
		log.info("In testAllColumnsSumWithWhere() method Entry");

		String statement = "SELECT sum(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(-362l, records.get(0).get(0)[0]);

		log.info("In testAllColumnsSumWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsMinWithWhere() throws Exception {
		log.info("In testAllColumnsMinWithWhere() method Entry");

		String statement = "SELECT min(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(Long.valueOf(-27), records.get(0).get(0)[0]);

		log.info("In testAllColumnsMinWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsMaxWithWhere() throws Exception {
		log.info("In testAllColumnsMaxWithWhere() method Entry");

		String statement = "SELECT max(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(Long.valueOf(44), records.get(0).get(0)[0]);

		log.info("In testAllColumnsMaxWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoin() throws Exception {
		log.info("In testRequiredColumnsJoin() method Entry");

		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertEquals(8l, rec[0]);
				assertEquals(12l, rec[1]);
			}
		}
		assertEquals(132, totalrecords);

		log.info("In testRequiredColumnsJoin() method Exit");
	}

	@Test
	public void testRequiredColumnsJoinCarrierSpecific() throws Exception {
		log.info("In testRequiredColumnsJoinCarrierSpecific() method Entry");

		String statement = """
				SELECT airline.ArrDelay,airline.DepDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Code,carriers.Description \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ'\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(6, rec.length);
				assertEquals(8l, rec[2]);
				assertEquals(8l, rec[3]);
				assertEquals("AQ", rec[4]);
			}
		}

		log.info("In testRequiredColumnsJoinCarrierSpecific() method Exit");
	}

	@Test
	public void testCountAllColumnsWithWhereAndJoin() throws Exception {
		log.info("In testCountAllColumnsWithWhereAndJoin() method Entry");

		String statement = """
				SELECT count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ' and carriers.Code<>'Code'\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int total = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				total += (Long) rec[0];
			}
		}
		assertEquals(131, total);

		log.info("In testCountAllColumnsWithWhereAndJoin() method Exit");
	}

	@Test
	public void testSumAvgAvgCount() throws Exception {
		log.info("In testSumAvgAvgCount() method Entry");

		String statement = """
				select sum(airline.arrdelay) as sumadelay,avg(airline.arrdelay) as adelay
				,avg(airline.depdelay) as ddelay,count(*) as recordcnt 
				from airline group by airline.uniquecarrier
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int total = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				total++;
			}
		}
		log.info("In testSumAvgAvgCount() method Exit");
	}

	@Test
	public void testPrintAllColumnsCountWithWhereAndJoin() throws Exception {
		log.info("In testPrintAllColumnsCountWithWhereAndJoin() method Entry");

		String statement = """
				SELECT * \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ' and carriers.Code<>'Code'\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int total = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				total++;
				assertTrue(rec.length == 31);
				assertTrue(((Long) rec[2]) == 8);
				assertTrue(((Long) rec[1]) == 8);
				assertTrue(rec[29].equals("AQ"));
				log.info(Arrays.toString(rec));
			}
		}
		assertEquals(131, total);

		log.info("In testPrintAllColumnsCountWithWhereAndJoin() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinTwoTables() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTables() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Description,airline.Origin,airports.airport \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int recordcount = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				recordcount++;
				log.info(Arrays.toString(rec));
			}
		}
		assertEquals(46360, recordcount);
		log.info("In testRequiredColumnsJoinTwoTables() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinTwoTablesWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Description,airline.Origin,airports.airport \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int recordcount = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				recordcount++;
				log.info(Arrays.toString(rec));
			}
		}
		assertEquals(132, recordcount);
		log.info("In testRequiredColumnsJoinTwoTablesWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinTwoTablesCount() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesCount() method Entry");
		String statement = """
				SELECT count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int recordcount = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				recordcount += (Long) rec[0];
				log.info(Arrays.toString(rec));
			}
		}
		assertEquals(46360, recordcount);
		log.info("In testRequiredColumnsJoinTwoTablesCount() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinTwoTablesCountWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesCountWhere() method Entry");
		String statement = """
				SELECT count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int recordcount = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				recordcount += (Long) rec[0];
				log.info(Arrays.toString(rec));
			}
		}
		assertEquals(132, recordcount);
		log.info("In testRequiredColumnsJoinTwoTablesCountWhere() method Exit");
	}

	@Test
	public void testRequiredColumnsJoinTwoTablesColumnCountWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnCountWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int recordcount = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 2);
				recordcount += (Long) rec[1];
				log.info(Arrays.toString(rec));
			}
		}
		assertEquals(132, recordcount);
		log.info("In testRequiredColumnsJoinTwoTablesColumnCountWhere() method Exit");
	}

	@Test
	public void testRequiredColumnsJoinTwoTablesColumnSumWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,sum(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		Double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
				log.info(Arrays.toString(rec));
			}
		}
		assertTrue(sum == -362.0);
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhere() method Exit");
	}

	@Test
	public void testRequiredColumnsJoinTwoTablesColumnMinWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnMinWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,min(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		Long arrdelaymin = 0l;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 2);
				arrdelaymin = (Long) rec[1];
				log.info(Arrays.toString(rec));
			}
		}
		assertTrue(arrdelaymin == -27.0);
		log.info("In testRequiredColumnsJoinTwoTablesColumnMinWhere() method Exit");
	}

	@Test
	public void testRequiredColumnsJoinTwoTablesColumnMaxWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnMaxWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,max(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		Long arrdelaymax = 0l;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 2);
				arrdelaymax = (Long) rec[1];
				log.info(Arrays.toString(rec));
			}
		}
		assertTrue(arrdelaymax == 44.0);
		log.info("In testRequiredColumnsJoinTwoTablesColumnMaxWhere() method Exit");
	}

	@Test
	public void testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere() throws Exception {
		log.info("In testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere() method Entry");
		String statement = """
				SELECT airports.iata,airline.UniqueCarrier,sum(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airports.iata,airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		Double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 3);
				sum += (Long) rec[2];
				log.info(Arrays.toString(rec));
			}
		}
		assertTrue(sum == -362.0);
		log.info("In testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,sum(airline.ArrDelay) \
				FROM airline group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		Double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(-63278 == sum);
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,min(airline.ArrDelay),count(*),max(airline.ArrDelay),sum(airline.ArrDelay) \
				FROM airline where airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		Long sum = 0l, min = 0l, max = 0l;
		Long count = 0l;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 5);
				min = (Long) rec[1];
				sum = (Long) rec[4];
				max = (Long) rec[3];
				count = (Long) rec[2];
			}
		}
		assertTrue(min == -27);
		assertTrue(max == 44);
		assertTrue(sum == -362);
		assertTrue(count == 132);
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() throws Exception {
		log.info("In testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Entry");
		String statement = """
				SELECT airports.iata,airline.UniqueCarrier,sum(airline.ArrDelay),min(airline.ArrDelay),max(airline.ArrDelay),count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airports.iata,airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.add(airportssample, "airports", airportsheader, airportstype).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		Double sum = 0.0d, count = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 6);
				sum += (Long) rec[2];
				count += (Long) rec[5];
			}
		}
		assertTrue(sum == -362.0);
		assertTrue(count == 132.0);
		log.info("In testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMultipleAllColumnsOrCondition() throws Exception {
		log.info("In testMultipleAllColumnsOrCondition() method Entry");
		String statement = """
				SELECT * from airline \
				WHERE airline.DayofMonth=8 or airline.MonthOfYear=12\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				assertTrue(((Long) rec[2]) == 8 || ((Long) rec[1]) == 12);
				sum++;
			}
		}
		assertTrue(sum == 5405.0);
		log.info("In testMultipleAllColumnsOrCondition() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testNumberOfFlightsByCarrier() throws Exception {
		log.info("In testNumberOfFlightsByCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier, count(*) FROM airline GROUP BY airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360);
		log.info("In testNumberOfFlightsByCarrier() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testNumberOfFlightsByDayOfWeek() throws Exception {
		log.info("In testNumberOfFlightsByDayOfWeek() method Entry");
		String statement = "SELECT airline.DayOfWeek, count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360);
		log.info("In testNumberOfFlightsByDayOfWeek() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testNumberOfFlightsCancelled() throws Exception {
		log.info("In testNumberOfFlightsCancelled() method Entry");
		String statement = "SELECT count(*) FROM airline WHERE airline.Cancelled = 1";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				sum += (Long) rec[0];
			}
		}
		assertTrue(sum == 388.0);
		log.info("In testNumberOfFlightsCancelled() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testNumberOfFlightsDiverted() throws Exception {
		log.info("In testNumberOfFlightsDiverted() method Entry");
		String statement = "SELECT count(*) FROM airline WHERE airline.Diverted = 1";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				sum += (Long) rec[0];
			}
		}
		assertTrue(sum == 15);
		log.info("In testNumberOfFlightsDiverted() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testTotalDistanceFlownByCarrier() throws Exception {
		log.info("In testTotalDistanceFlownByCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier, sum(airline.Distance) FROM airline GROUP BY airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == 19882075.0);
		log.info("In testTotalDistanceFlownByCarrier() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testDayOfWeekWithMostFlights() throws Exception {
		log.info("In testDayOfWeekWithMostFlights() method Entry");
		String statement = "SELECT airline.DayOfWeek, count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360);
		log.info("In testDayOfWeekWithMostFlights() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMonthOfYearWithMostFlights() throws Exception {
		log.info("In testMonthOfYearWithMostFlights() method Entry");
		String statement = "SELECT airline.MonthOfYear, count(*) FROM airline GROUP BY airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360);
		log.info("In testMonthOfYearWithMostFlights() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAirportsWithDepartures() throws Exception {
		log.info("In testAirportsWithDepartures() method Entry");
		String statement = "SELECT airline.Origin, count(*) FROM airline GROUP BY airline.Origin";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360);
		log.info("In testAirportsWithDepartures() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAirportsWithArrivals() throws Exception {
		log.info("In testAirportsWithArrivals() method Entry");
		String statement = "SELECT airline.Dest, count(*) FROM airline GROUP BY airline.Dest";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360);
		log.info("In testAirportsWithArrivals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testDelayTimeByDayOfWeek() throws Exception {
		log.info("In testDelayTimeByDayOfWeek() method Entry");
		String statement = "SELECT airline.DayOfWeek, sum(airline.ArrDelay),count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		double arrdelay = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				sum += (Long) rec[2];
				arrdelay += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360.0);
		assertTrue(arrdelay == -63278.0);
		log.info("In testDelayTimeByDayOfWeek() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testTotalDelayTimeByMonthOfYear() throws Exception {
		log.info("In testTotalDelayTimeByMonthOfYear() method Entry");
		String statement = "SELECT airline.MonthOfYear, sum(airline.ArrDelay), count(*) FROM airline GROUP BY airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		Double arrdelay = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				sum += (Long) rec[2];
				arrdelay += (Long) rec[1];
			}
		}
		assertTrue(sum == 46360.0);
		assertTrue(arrdelay == -63278.0);
		log.info("In testTotalDelayTimeByMonthOfYear() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAverageDelayByDestinationAirport() throws Exception {
		log.info("In testAverageDelayByDestinationAirport() method Entry");
		String statement = "SELECT airline.Dest, sum(airline.ArrDelay),avg(airline.ArrDelay) AvgDelay FROM airline GROUP BY airline.Dest";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		Double avgarrdelay = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				avgarrdelay += (Double) rec[2];
				sum += (Long) rec[1];
			}
		}
		assertTrue(sum == -63278.0);
		assertTrue(avgarrdelay == -9.663325375452317);
		log.info("In testAverageDelayByDestinationAirport() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsCancelledAndCancellationCode() throws Exception {
		log.info("In testFlightsCancelledAndCancellationCode() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Cancelled,airline.CancellationCode FROM airline WHERE airline.Cancelled = 1";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				sum++;
				assertTrue((Long) rec[1] == 1 && !rec[2].equals("NA"));
			}
		}
		assertTrue(sum == 388);
		log.info("In testFlightsCancelledAndCancellationCode() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsCancelledDueToWeather() throws Exception {
		log.info("In testFlightsCancelledDueToWeather() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Cancelled,airline.CancellationCode FROM airline WHERE airline.Cancelled = 1 AND airline.CancellationCode = 'B'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				sum++;
				assertTrue(((Long) rec[1]) == 1 && rec[2].equals("B"));
			}
		}
		assertTrue(sum == 0);
		log.info("In testFlightsCancelledDueToWeather() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsDivertedDueToWeather() throws Exception {
		log.info("In testFlightsDivertedDueToWeather() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Diverted,airline.WeatherDelay FROM airline WHERE airline.Diverted = 1";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				sum++;
				assertTrue(((Long) rec[1]) == 1 && !rec[2].equals("NA"));
			}
		}
		assertTrue(sum == 15);
		log.info("In testFlightsDivertedDueToWeather() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsDivertedDueToWeatherSortBy() throws Exception {
		log.info("In testFlightsDivertedDueToWeatherSortBy() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 or airline.MonthOfYear=12 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				assertTrue(((Long) rec[2]) == 8 || ((Long) rec[1]) == 12);
			}
		}
		log.info("In testFlightsDivertedDueToWeatherSortBy() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsjoinGroupBy() throws Exception {
		log.info("In testFlightsjoinGroupBy() method Entry");
		String statement = "SELECT airlines.Origin,airports.airport,count(*) FROM airlines inner join airports on airports.iata = airlines.Origin GROUP BY airlines.Origin,airports.airport";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				sum += (Long) rec[2];
			}
		}
		assertTrue(sum == 46360);
		log.info("In testFlightsjoinGroupBy() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsDistinctUniqueCarrier() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrier() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier from airlines";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		var uc = new ArrayList<>();
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				uc.add(rec[0]);
			}
		}
		assertTrue(uc.contains("AQ"));
		log.info("In testFlightsDistinctUniqueCarrier() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsDistinctUniqueCarrierArrDelayDepDelay() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrierArrDelayDepDelay() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier,airlines.ArrDelay,airlines.DepDelay from airlines";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int sum = 0;

		for (List<Object[]> recs : records) {
			sum += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
			}
		}
		log.info(sum);
		assertNotEquals(0, sum);
		log.info("In testFlightsDistinctUniqueCarrierArrDelayDepDelay() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsDistinctUniqueCarrierFlightnumOriginDest() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrierFlightnumOriginDest() method Entry");
		String statement = "select distinct uniquecarrier,flightnum,origin,dest from airlines order by uniquecarrier,flightnum,origin,dest";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int sum = 0;

		for (List<Object[]> recs : records) {
			sum += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
			}
		}
		log.info(sum);
		assertNotEquals(0, sum);
		log.info("In testFlightsDistinctUniqueCarrierFlightnumOriginDest() method Exit");
	}
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsDistinctUniqueCarrierWithWhere() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier from airlines where airlines.UniqueCarrier <> 'UniqueCarrier'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		var uc = new ArrayList<>();
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				uc.add(rec[0]);
			}
		}
		assertTrue(uc.contains("AQ"));
		log.info("In testFlightsDistinctUniqueCarrierWithWhere() method Exit");
	}

	@Test
	public void testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() throws Exception {
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier,airlines.AirlineYear from airlines where airlines.UniqueCarrier <> 'UniqueCarrier' order by airlines.AirlineYear,airlines.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		var uc = new ArrayList<>();
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
				uc.add(rec[0]);
			}
		}
		assertTrue(uc.contains("AQ"));
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Exit");
	}

	@Test
	public void testDistinctUniqueCarrierYearAndMonthWithWhere() throws Exception {
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier,airlines.AirlineYear,airlines.MonthOfYear from airlines where airlines.UniqueCarrier <> 'UniqueCarrier' order by airlines.AirlineYear,airlines.MonthOfYear,airlines.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		var uc = new ArrayList<>();
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(3, rec.length);
				uc.add(rec[0]);
			}
		}
		assertTrue(uc.contains("AQ"));
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvg() throws Exception {
		log.info("In testAllColumnsAvg() method Entry");

		String statement = "SELECT avg(airline.ArrDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(Double.valueOf("-1.3768957938942925"), records.get(0).get(0)[0]);

		log.info("In testAllColumnsAvg() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvgArrDelayPlusArrDelay() throws Exception {
		log.info("In testAllColumnsAvgArrDelayPlusArrDelay() method Entry");

		String statement = "SELECT avg(airline.ArrDelay + airline.DepDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(Double.valueOf("-0.9489740409513241"), records.get(0).get(0)[0]);

		log.info("In testAllColumnsAvgArrDelayPlusArrDelay() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvgArrDelayPerCarrier() throws Exception {
		log.info("In testAllColumnsAvgArrDelayPerCarrier() method Entry");

		String statement = "SELECT airline.UniqueCarrier,avg(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals("AQ", records.get(0).get(0)[0]);
		assertEquals(Double.valueOf("-1.3768957938942925"), records.get(0).get(0)[1]);

		log.info("In testAllColumnsAvgArrDelayPerCarrier() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvgArrDelayPerCarrierWithWhere() throws Exception {
		log.info("In testAllColumnsAvgArrDelayPerCarrierWithWhere() method Entry");

		String statement = "SELECT airline.UniqueCarrier,avg(airline.ArrDelay) FROM airline where airline.DayOfWeek=1 group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals("AQ", records.get(0).get(0)[0]);
		assertEquals(Double.valueOf(-2.2600950118764844), records.get(0).get(0)[1]);

		log.info("In testAllColumnsAvgArrDelayPerCarrierWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsMonthDayAvgArrDelayPerCarrier() throws Exception {
		log.info("In testRequiredColumnsMonthDayAvgArrDelayPerCarrier() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,avg(airline.ArrDelay) avgarrdelay, \
				sum(airline.ArrDelay) as sumarrdelay, count(*) as ct, min(airline.ArrDelay) as minarrdelay, max(airline.ArrDelay) as maxarrdelay\
				 FROM airline group by airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear order by airline.UniqueCarrier, avgarrdelay\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(8, rec.length);
			}
		}

		log.info("In testRequiredColumnsMonthDayAvgArrDelayPerCarrier() method Exit");
	}

	@Test
	public void testCountAvgMinMaxSumArrDelayPerCarrier() throws Exception {
		log.info("In testCountAvgMinMaxSumArrDelayPerCarrier() method Entry");

		String statement = """
				SELECT avg(airline.ArrDelay) avgarrdelay, \
				sum(airline.ArrDelay) as sumarrdelay, count(*) as ct, min(airline.ArrDelay) as minarrdelay, max(airline.ArrDelay) as maxarrdelay\
				 FROM airline group by airline.MonthOfYear order by avgarrdelay\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(5, rec.length);
			}
		}

		log.info("In testCountAvgMinMaxSumArrDelayPerCarrier() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnLength() throws Exception {
		log.info("In testColumnLength() method Entry");

		String statement = "SELECT length(airline.Origin)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnLength() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnWithLength() throws Exception {
		log.info("In testRequiredColumnWithLength() method Entry");

		String statement = "SELECT airline.Origin,length(airline.Origin)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testRequiredColumnWithLength() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnWithMultipleLengths() throws Exception {
		log.info("In testRequiredColumnWithMultipleLengths() method Entry");

		String statement = "SELECT airline.Origin,length(airline.Origin),length(airline.Dest)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(3, rec.length);
			}
		}
		log.info("In testRequiredColumnWithMultipleLengths() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnWithLengthsAndLowercase() throws Exception {
		log.info("In testRequiredColumnWithLengthsAndLowercase() method Entry");

		String statement = "SELECT lowercase(airline.Origin),lowercase(airline.Dest),length(airline.Origin),length(airline.Dest)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(4, rec.length);
			}
		}
		log.info("In testRequiredColumnWithLengthsAndLowercase() method Exit");
	}

	@Test
	public void testRequiredColumnWithLengthsAndUppercase() throws Exception {
		log.info("In testRequiredColumnWithLengthsAndUppercase() method Entry");

		String statement = "SELECT uppercase(airline.Origin),uppercase(airline.Dest),length(airline.Origin),length(airline.Dest)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(4, rec.length);
			}
		}
		log.info("In testRequiredColumnWithLengthsAndUppercase() method Exit");
	}

	@Test
	public void testRequiredColumnTrim() throws Exception {
		log.info("In testRequiredColumnTrim() method Entry");

		String statement = "SELECT trimstr(grpconcat(airline.Origin , ' ')) trmorig ,trimstr(grpconcat(' ' , airline.Dest)) trimdest FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testRequiredColumnTrim() method Exit");
	}

	@Test
	public void testRequiredColumnBase64Encode() throws Exception {
		log.info("In testRequiredColumnBase64Encode() method Entry");

		String statement = "SELECT base64encode(airline.Origin),base64encode(airline.Dest)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testRequiredColumnBase64Encode() method Exit");
	}

	@Test
	public void testRequiredColumnSubStringAlias() throws Exception {
		log.info("In testRequiredColumnSubStringAlias() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin,0,1) as substr  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testRequiredColumnSubStringAlias() method Exit");
	}

	@Test
	public void testRequiredColumnSubString() throws Exception {
		log.info("In testRequiredColumnSubString() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin,0,1),airline.Dest,substring(airline.Dest,0,2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(4, rec.length);
			}
		}
		log.info("In testRequiredColumnSubString() method Exit");
	}

	@Test
	public void testRequiredColumnNormailizeSpaces() throws Exception {
		log.info("In testRequiredColumnNormailizeSpaces() method Entry");

		String statement = "SELECT normalizespaces(airline.Dest),normalizespaces(' This is   good  work') eg FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testRequiredColumnNormailizeSpaces() method Exit");
	}

	@Test
	public void testDate() throws Exception {
		log.info("In testDate() method Entry");

		String statement = "SELECT normalizespaces(' This is   good  work') normspace,currentisodate() isodate FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testDate() method Exit");
	}

	@Test
	public void testDateWithCount() throws Exception {
		log.info("In testDateWithCount() method Entry");

		String statement = "SELECT normalizespaces(' This is   good  work') normspace,currentisodate() isodate,count(*) numrec FROM airline group by airline.AirlineYear";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(3, rec.length);
			}
		}
		log.info("In testDateWithCount() method Exit");
	}

	@Test
	public void testSumWithMultuplication() throws Exception {
		log.info("In testDate() method Entry");

		String statement = "SELECT sum(airline.ArrDelay * 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-126556l, records.get(0).get(0)[0]);
		log.info("In testDate() method Exit");
	}

	@Test
	public void testSumWithAddition() throws Exception {
		log.info("In testSumWithAddition() method Entry");

		String statement = "SELECT sum(airline.ArrDelay + 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(28636l, records.get(0).get(0)[0]);
		log.info("In testSumWithAddition() method Exit");
	}

	@Test
	public void testSumWithSubtraction() throws Exception {
		log.info("In testSumWithSubtraction() method Entry");

		String statement = "SELECT sum(airline.ArrDelay - 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-155192l, records.get(0).get(0)[0]);
		log.info("In testSumWithSubtraction() method Exit");
	}


	@Test
	public void testSum() throws Exception {
		log.info("In testSum() method Entry");

		String statement = "SELECT sum(airline.ArrDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-63278l, records.get(0).get(0)[0]);
		log.info("In testSum() method Exit");
	}

	@Test
	public void testSumWithBase64Encode() throws Exception {
		log.info("In testSumWithBase64Encode() method Entry");

		String statement = "SELECT base64encode(airline.Origin) originalias,sum(airline.ArrDelay - 2) FROM airline group by airline.Origin";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
				sum += (Long) rec[1];
			}
		}
		assertEquals(Double.valueOf(-155192.0), Double.valueOf(sum));
		log.info("In testSumWithBase64Encode() method Exit");
	}

	@Test
	public void testSumWithColumnAndLength() throws Exception {
		log.info("In testSumWithColumnAndLength() method Entry");

		String statement = "SELECT airline.UniqueCarrier,length(airline.UniqueCarrier),sum(airline.ArrDelay - 2) FROM airline group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(3, rec.length);
				sum += (Long) rec[2];
			}
		}
		assertEquals(Double.valueOf(-155192.0), Double.valueOf(sum));
		log.info("In testSumWithColumnAndLength() method Exit");
	}

	@Test
	public void testSelectWithAggFunctionWithGroupBy() throws Exception {
		log.info("In testSelectWithAggFunctionColumnsWithoutGroupBy() method Entry");
		String statement = "SELECT sum(airline.ArrDelay - 2) FROM airline group by airline.MonthOfYear,airline.DayofMonth";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		int totalreccount = 0;
		for (List<Object[]> recs : records) {
			totalreccount += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(365, totalreccount);
		assertEquals(Double.valueOf(-155192.0), Double.valueOf(sum));
		log.info("In testSelectWithAggFunctionColumnsWithoutGroupBy() method Exit");
	}

	@Test
	public void testSumWithDivision() throws Exception {
		log.info("In testSumWithDivision() method Entry");

		String statement = "SELECT sum(airline.ArrDelay / 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(Double.valueOf(-31639.0), records.get(0).get(0)[0]);
		log.info("In testSumWithDivision() method Exit");
	}

	@Test
	public void testSumWithSubtractionAndMultiplication() throws Exception {
		log.info("In testSumWithSubtractionAndMultiplication() method Entry");

		String statement = "SELECT sum((airline.ArrDelay - 2) * 3.5) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-543172.0, records.get(0).get(0)[0]);
		log.info("In testSumWithSubtractionAndMultiplication() method Exit");
	}

	@Test
	public void testSumWithAdditionAndMultiplication() throws Exception {
		log.info("In testSumWithAdditionAndMultiplication() method Entry");

		String statement = "SELECT sum((airline.ArrDelay + 2) * 2.5) FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(71590.0, records.get(0).get(0)[0]);
		log.info("In testSumWithAdditionAndMultiplication() method Exit");
	}

	@Test
	public void testSelectWithWhereIn() throws Exception {
		log.info("In testSelectWithWhereIn() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11,12) ";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}

		statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11) ";
		spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum11 = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum11 += (Long) rec[0];
			}
		}

		statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (12) ";
		spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum12 = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum12 += (Long) rec[0];
			}
		}

		assertEquals(Double.valueOf(sum), Double.valueOf(sum11 + sum12));
		log.info("In testSelectWithWhereIn() method Exit");
	}

	@Test
	public void testSelectCountWithWhereLike() throws Exception {
		log.info("In testSelectCountWithWhereLike() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double count = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				count += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(5995.0), Double.valueOf(count));
		log.info("In testSelectCountWithWhereLike() method Exit");
	}

	@Test
	public void testSelectSumWithWhereLike() throws Exception {
		log.info("In testSelectSumWithWhereLike() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(-6500.0), Double.valueOf(sum));
		log.info("In testSelectSumWithWhereLike() method Exit");
	}

	@Test
	public void testSelectSumWithWhereInAndLikeClause() throws Exception {
		log.info("In testSelectSumWithWhereInAndLikeClause() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11,12) and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(-1199.0), Double.valueOf(sum));
		log.info("In testSelectSumWithWhereInAndLikeClause() method Exit");
	}

	@Test
	public void testSelectCountWithWhereInAndLikeClause() throws Exception {
		log.info("In testSelectCountWithWhereInAndLikeClause() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.MonthOfYear in (11,12) and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double count = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				count += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(1029.0), Double.valueOf(count));
		log.info("In testSelectCountWithWhereInAndLikeClause() method Exit");
	}

	@Test
	public void testSelectCountWithWhereLikeAndBetweenClause() throws Exception {
		log.info("In testSelectCountWithWhereLikeAndBetweenClause() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double count = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				count += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(1029.0), Double.valueOf(count));
		log.info("In testSelectCountWithWhereLikeAndBetweenClause() method Exit");
	}

	@Test
	public void testSelectSumWithWhereLikeAndBetweenClause() throws Exception {
		log.info("In testSelectSumWithWhereLikeAndBetweenClause() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(-1199.0), Double.valueOf(sum));
		log.info("In testSelectSumWithWhereLikeAndBetweenClause() method Exit");
	}

	@Test
	public void testSelectSumWithNestedAbsFunction() throws Exception {
		log.info("In testSelectSumWithNestedAbsFunction() method Entry");
		String statement = "SELECT sum(abs(airline.MonthOfYear) + airline.ArrDelay) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(10467.0), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedAbsFunction() method Exit");
	}

	@Test
	public void testSelectSumWithNestedAbsFunctions() throws Exception {
		log.info("In testSelectSumWithNestedAbsFunctions() method Entry");
		String statement = "SELECT sum(abs(airline.MonthOfYear) + abs(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(28284.0), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedAbsFunctions() method Exit");
	}

	@Test
	public void testSelectSumWithNestedAbsAndLengthFunctions() throws Exception {
		log.info("In testSelectSumWithNestedAbsAndLengthFunctions() method Entry");
		String statement = "SELECT sum(abs(length(airline.Origin)) + abs(length(airline.Dest))) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(6174.0), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedAbsAndLengthFunctions() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnAbs() throws Exception {
		log.info("In testColumnAbs() method Entry");

		String statement = "SELECT abs(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnAbs() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnRound() throws Exception {
		log.info("In testColumnRound() method Entry");

		String statement = "SELECT airline.ArrDelay, round(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testColumnRound() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnCeil() throws Exception {
		log.info("In testColumnCeil() method Entry");

		String statement = "SELECT airline.ArrDelay, ceil(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testColumnCeil() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnFloor() throws Exception {
		log.info("In testColumnFloor() method Entry");

		String statement = "SELECT airline.ArrDelay,floor(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testColumnFloor() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnPower() throws Exception {
		log.info("In testColumnPower() method Entry");

		String statement = "SELECT airline.ArrDelay, pow(airline.ArrDelay, 2) powcal  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testColumnPower() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnSqrt() throws Exception {
		log.info("In testColumnSqrt() method Entry");

		String statement = "SELECT airline.MonthOfYear, sqrt(airline.MonthOfYear)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testColumnSqrt() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnExponential() throws Exception {
		log.info("In testColumnExponential() method Entry");

		String statement = "SELECT exp(airline.MonthOfYear)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnExponential() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnLoge() throws Exception {
		log.info("In testColumnloge() method Entry");

		String statement = "SELECT loge(airline.MonthOfYear) as log  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnloge() method Exit");
	}

	@Test
	public void testSelectSumWithNestedRound() throws Exception {
		log.info("In testSelectSumWithNestedRound() method Entry");
		String statement = "SELECT sum(round(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Long) rec[0];
			}
		}
		assertEquals(Double.valueOf(-1199.0), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedRound() method Exit");
	}

	@Test
	public void testSelectSumWithNestedCeil() throws Exception {
		log.info("In testSelectSumWithNestedCeil() method Entry");
		String statement = "SELECT sum(ceil(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Double) rec[0];
			}
		}
		assertEquals(Double.valueOf(-1199.0), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedCeil() method Exit");
	}

	@Test
	public void testSelectSumWithNestedFloor() throws Exception {
		log.info("In testSelectSumWithNestedFloor() method Entry");
		String statement = "SELECT sum(floor(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Double) rec[0];
			}
		}
		assertEquals(Double.valueOf(-1199.0), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedFloor() method Exit");
	}

	@Test
	public void testSelectSumWithNestedPower() throws Exception {
		log.info("In testSelectSumWithNestedPower() method Entry");
		String statement = "SELECT sum(pow(airline.MonthOfYear, 2)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Double) rec[0];
			}
		}
		assertEquals(Double.valueOf(136515.0), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedPower() method Exit");
	}

	@Test
	public void testSelectSumWithNestedSqrt() throws Exception {
		log.info("In testSelectSumWithNestedSqrt() method Entry");
		String statement = "SELECT sum(sqrt(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Double) rec[0];
			}
		}
		assertEquals(Double.valueOf(3489.7898118120693), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedSqrt() method Exit");
	}

	@Test
	public void testSelectSumWithNestedExponential() throws Exception {
		log.info("In testSelectSumWithNestedExponential() method Entry");
		String statement = "SELECT sum(exp(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Double) rec[0];
			}
		}
		assertEquals(Double.valueOf(1.153141909703247E8), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedExponential() method Exit");
	}

	@Test
	public void testSelectSumWithNestedloge() throws Exception {
		log.info("In testSelectSumWithNestedloge() method Entry");
		String statement = "SELECT sum(loge(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum += (Double) rec[0];
			}
		}
		assertEquals(Double.valueOf(2512.854174498125), Double.valueOf(sum));
		log.info("In testSelectSumWithNestedloge() method Exit");
	}


	public void testSelectGroupConcatGroupBy() throws Exception {
		log.info("In testSelectGroupConcatGroupBy() method Entry");
		String statement = "SELECT airline.DayofMonth, grpconcat(airline.TailNum, '||') FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG' group by airline.DayofMonth,airline.TailNum";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testSelectGroupConcatGroupBy() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnLengthWithExp() throws Exception {
		log.info("In testColumnLengthWithExp() method Entry");

		String statement = "SELECT length(grpconcat(airline.Origin , airline.Dest))  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnAbsLengthWithExp() throws Exception {
		log.info("In testColumnAbsLengthWithExp() method Entry");

		String statement = "SELECT abs(length(grpconcat(airline.Origin , airline.Dest)))  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnAbsLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnRoundLengthWithExp() throws Exception {
		log.info("In testColumnRoundLengthWithExp() method Entry");

		String statement = "SELECT round(length(grpconcat(airline.Origin , airline.Dest)) + 0.4)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnRoundLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnRoundLengthWithExpWithInc() throws Exception {
		log.info("In testColumnRoundLengthWithExpWithInc() method Entry");

		String statement = "SELECT round(length(grpconcat(airline.Origin , airline.Dest)) + 0.6)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnRoundLengthWithExpWithInc() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnCeilLengthWithExpWithInc() throws Exception {
		log.info("In testColumnCeilLengthWithExpWithInc() method Entry");

		String statement = "SELECT ceil(length(grpconcat(airline.Origin , airline.Dest)) + 0.6)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnCeilLengthWithExpWithInc() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnCeilLengthWithExp() throws Exception {
		log.info("In testColumnCeilLengthWithExp() method Entry");

		String statement = "SELECT ceil(length(grpconcat(airline.Origin , airline.Dest)) + 0.4)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnCeilLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnFloorLengthWithExpWithInc() throws Exception {
		log.info("In testColumnFloorLengthWithExpWithInc() method Entry");

		String statement = "SELECT floor(length(grpconcat(airline.Origin , airline.Dest)) + 0.6)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnFloorLengthWithExpWithInc() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnFloorLengthWithExp() throws Exception {
		log.info("In testColumnFloorLengthWithExp() method Entry");

		String statement = "SELECT floor(length(grpconcat(airline.Origin , airline.Dest)) + 0.4)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnFloorLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnLengthWithParanthesisExp() throws Exception {
		log.info("In testColumnLengthWithParanthesisExp() method Entry");

		String statement = "SELECT (length(grpconcat(airline.Origin , airline.Dest)) + 0.4) paransum  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnLengthWithParanthesisExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnPowerLengthWithExp() throws Exception {
		log.info("In testColumnPowerLengthWithExp() method Entry");

		String statement = "SELECT pow(length(grpconcat(airline.Origin , airline.Dest)), 2) powlen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnPowerLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnSqrtLengthWithExp() throws Exception {
		log.info("In testColumnSqrtLengthWithExp() method Entry");

		String statement = "SELECT sqrt(length(grpconcat(airline.Origin , airline.Dest))) sqrtlen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				assertTrue(rec[0] instanceof Double);
			}
		}
		log.info("In testColumnSqrtLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnExpLengthWithExp() throws Exception {
		log.info("In testColumnExpLengthWithExp() method Entry");

		String statement = "SELECT exp(length(grpconcat(airline.Origin , airline.Dest))) explen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnExpLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnLogLengthWithExp() throws Exception {
		log.info("In testColumnLogLengthWithExp() method Entry");

		String statement = "SELECT loge(length(grpconcat(airline.Origin , airline.Dest))) loglen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnLogLengthWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnLowerCaseWithUpperCaseWithExp() throws Exception {
		log.info("In testColumnLowerCaseWithUpperCaseWithExp() method Entry");

		String statement = "SELECT lowercase(uppercase(grpconcat(airline.Origin , airline.Dest) + 'low') + 'UPP') lowup  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnLowerCaseWithUpperCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT uppercase(lowercase(grpconcat(airline.Origin , airline.Dest) + 'LOW') + 'upp') uplow  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnUpperCaseWithLowerCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnTrimUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnTrimUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT trimstr('     ' + uppercase(lowercase(grpconcat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ') uplow  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnTrimUpperCaseWithLowerCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnBase64_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnBase64_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT base64encode('     ' + uppercase(lowercase(grpconcat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ') encstring  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnBase64_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT base64decode(base64encode('     ' + uppercase(lowercase(grpconcat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ')) decstring  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT normalizespaces(base64decode(base64encode('     ' + uppercase(lowercase(grpconcat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      '))) normalizedstring  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT substring(base64decode(base64encode('     ' + uppercase(lowercase(grpconcat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ')), 0 , 6) substr  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT normalizespaces(substring(base64decode(base64encode('     ' + uppercase(lowercase(grpconcat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ')), 0 , 6)) normsubstr  FROM airline";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsLeftJoin() throws Exception {
		log.info("In testRequiredColumnsLeftJoin() method Entry");

		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM carriers left join airline on airline.UniqueCarrier = carriers.Code\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
			}
		}
		assertEquals(47851, totalrecords);

		log.info("In testRequiredColumnsLeftJoin() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsRightJoin() throws Exception {
		log.info("In testRequiredColumnsRightJoin() method Entry");

		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline right join carriers on airline.UniqueCarrier = carriers.Code\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
			}
		}
		assertEquals(47851, totalrecords);

		log.info("In testRequiredColumnsRightJoin() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOr() throws Exception {
		log.info("In testFlightsAndOr() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 or airline.DayOfWeek=3 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 8 && ((Long) rec[1]) == 12
						|| ((Long) rec[3]) == 3);
			}
		}
		log.info("In testFlightsAndOr() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAnd() throws Exception {
		log.info("In testFlightsAndOrAnd() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 or airline.DayOfWeek=3 and airline.DayofMonth=1 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 8 && ((Long) rec[1]) == 12
						|| ((Long) rec[3]) == 3 && ((Long) rec[2]) == 1);
			}
		}
		log.info("In testFlightsAndOrAnd() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesis() throws Exception {
		log.info("In testFlightsAndOrAndParanthesis() method Entry");
		String statement = "SELECT * FROM airline WHERE (airline.DayofMonth=8 and airline.MonthOfYear=12) or (airline.DayOfWeek=3 and airline.DayofMonth=1) ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 8 && ((Long) rec[1]) == 12
						|| ((Long) rec[3]) == 3 && ((Long) rec[2]) == 1);
			}
		}
		log.info("In testFlightsAndOrAndParanthesis() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesisOr() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOr() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and airline.Origin='LIH' ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 8
						&& (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOr() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth+2 = 8 and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and airline.Origin='LIH' ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 6
						&& (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth+2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 6
						&& (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth + airline.DayOfWeek and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) + ((Long) rec[3]) == 8
						&& (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth - 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 10
						&& (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth * 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) == 4
						&& (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 4.0 <= airline.DayofMonth / 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 29);
				sum++;
				assertTrue(((Long) rec[2]) >= 8
						&& (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsSubSelect() throws Exception {
		log.info("In testRequiredColumnsSubSelect() method Entry");
		String statement = "SELECT AirlineYear,MonthOfYear FROM (select * from airline)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 2);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(46360, total);

		log.info("In testRequiredColumnsSubSelect() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsFunctionsSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay) sumdelay FROM (select * from airline) group by UniqueCarrier";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 2);
				log.info(Arrays.toString(record));
			}
		}
		log.info("In testRequiredColumnsFunctionsSubSelect() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsSubSelectFunctions() throws Exception {
		log.info("In testRequiredColumnsSubSelectFunctions() method Entry");
		String statement = "SELECT sumarrdelay FROM (select sum(airline.ArrDelay) sumarrdelay from airline  group by airline.UniqueCarrier)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 1);
				log.info(Arrays.toString(record));
			}
		}
		log.info("In testRequiredColumnsSubSelectFunctions() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsSubSelectFunctionsSumCount() throws Exception {
		log.info("In testRequiredColumnsSubSelectFunctionsSumCount() method Entry");
		String statement = "SELECT sumarrdelay,countrec FROM (select sum(airline.ArrDelay) sumarrdelay, count(*) countrec from airline  group by airline.UniqueCarrier)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 2);
				log.info(Arrays.toString(record));
			}
		}
		log.info("In testRequiredColumnsSubSelectFunctionsSumCount() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsFunctionsAvgSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay) FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				log.info(Arrays.toString(record));
			}
		}
		log.info("In testRequiredColumnsFunctionsAvgSubSelect() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsFunctionsAvgSumCountSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay),sum(ArrDelay),count(*) cnt FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 6);
				log.info(Arrays.toString(record));
			}
		}
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsFunctionAvgDelayFunctionsAvgSumCountSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Entry");
		String statement = "SELECT avg(avgdelay) delay from(SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay) avgdelay,sum(ArrDelay) sumdelay,count(*) cnt FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 1);
				log.info(Arrays.toString(record));
			}
		}
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsSubSelectAllColumns() throws Exception {
		log.info("In testAllColumnsSubSelectAllColumns() method Entry");
		String statement = "SELECT * FROM (select * from airline) ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 29);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(46360, total);

		log.info("In testAllColumnsSubSelectAllColumns() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM (select * from airline where airline.DayofMonth = 12) ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 29);
				assertTrue((Long) record[2] == 12);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(1533, total);

		log.info("In testAllColumnsSubSelectAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM (select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 29);
				assertTrue((Long) record[2] == 12 && (Long) record[3] == 3);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(255, total);

		log.info("In testAllColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3\
				""";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertTrue((Long) record[2] == 12 && (Long) record[3] == 3);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(255, total);

		log.info("In testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT sqrt(abs(ArrDelay)) sqrtabs FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3\
				""";

		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				assertTrue(record.length == 1);
				log.info(Arrays.toString(record));
			}
		}
		log.info("In testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinSubSelect() throws Exception {
		log.info("In testRequiredColumnsJoinSubSelect() method Entry");

		String statement = """
				SELECT * FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear)\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertEquals(8l, rec[0]);
				assertEquals(12l, rec[1]);
			}
		}
		assertEquals(132, totalrecords);

		log.info("In testRequiredColumnsJoinSubSelect() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoinSubSelectAliasTable() throws Exception {
		log.info("In testRequiredColumnsJoinSubSelectAliasTable() method Entry");

		String statement = """
				SELECT ijres.DayofMonth,ijres.MonthOfYear,ijres.UniqueCarrier,ijres.Code FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear) ijres\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertEquals(8l, rec[0]);
				assertEquals(12l, rec[1]);
			}
		}
		assertEquals(132, totalrecords);

		log.info("In testRequiredColumnsJoinSubSelectAliasTable() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable() throws Exception {
		log.info("In testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable() method Entry");

		String statement = """
				SELECT ijres.DayofMonth,ijres.MonthOfYear,ijres.UniqueCarrier,ijres.Code FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear) ijres inner \
				join carriers on ijres.UniqueCarrier = carriers.Code\
				""";
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 4);
				assertEquals(8l, rec[0]);
				assertEquals(12l, rec[1]);
			}
		}
		assertEquals(132, totalrecords);

		log.info("In testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT AirlineYear,DayOfWeek,DayofMonth FROM( SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3)\
				""";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 3);
				assertTrue((Long) record[2] == 12 && (Long) record[1] == 3);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(255, total);

		log.info("In testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT AirlineYear,MonthOfYear,DayOfWeek,DayofMonth FROM( SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3) where MonthOfYear=12\
				""";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineCalciteSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertTrue((Long) record[1] == 12 && (Long) record[2] == 3
						&& (Long) record[3] == 12);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(130, total);

		log.info("In testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");
	}

	@AfterClass
	public static void pipelineConfigReset() {
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("20");
		pipelineconfig.setIsblocksuserdefined("false");
	}

}
