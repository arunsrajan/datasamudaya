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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineSqlBuilderLocalModeTest extends StreamPipelineBaseTestCommon {
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
	Logger log = LogManager.getLogger(StreamPipelineSqlBuilderLocalModeTest.class);
	List<String> airportsheader = Arrays.asList("iata", "airport", "city", "state", "country", "latitude", "longitude");
	List<SqlTypeName> airportstype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);

	@BeforeClass
	public static void pipelineSetup() throws Exception, Throwable {
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("1");
		pipelineconfig.setBatchsize(DataSamudayaConstants.EMPTY + Runtime.getRuntime().availableProcessors());
	}

	@After
	public void resetJobId() {
		if ("false".equals(pipelineconfig.getLocal())) {
			pipelineconfig.setJobid(null);
		}
	}

	@AfterClass
	public static void pipelineConfigReset() throws Exception {
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("20");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAggregateIntersection() throws Exception {
		log.info("In testAggregateIntersection() method Entry");

		String statement = """
				select airlinesamplesql.uniquecarrier,avg(airlinesamplesql.arrdelay) adelay,avg(airlinesamplesql.depdelay) ddelay,sum(airlinesamplesql.arrdelay) sarrdelay, sum(airlinesamplesql.depdelay) sdepdelay, count(*) cnt from airlinesamplesql group by airlinesamplesql.uniquecarrier intersect select airlinesample.uniquecarrier,avg(airlinesample.arrdelay) adelay,avg(airlinesample.depdelay) ddelay,sum(airlinesample.arrdelay) sarrdelay, sum(airlinesample.depdelay) sdepdelay, count(*) cnt from airlinesample group by airlinesample.uniquecarrier
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinesamplesql", airlineheader, airlineheadertypes)
				.add(airlinesample, "airlinesample", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(6l, rec.length);
			}
		}
		assertNotEquals(0, totalrecords);
		log.info("In testAggregateIntersection() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAirportsWithArrivals() throws Exception {
		log.info("In testAirportsWithArrivals() method Entry");
		String statement = "SELECT airline.Dest, count(*) FROM airline GROUP BY airline.Dest";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAirportsWithDepartures() throws Exception {
		log.info("In testAirportsWithDepartures() method Entry");
		String statement = "SELECT airline.Origin, count(*) FROM airline GROUP BY airline.Origin";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumns() throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM airline ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsAvg() throws Exception {
		log.info("In testAllColumnsAvg() method Entry");

		String statement = "SELECT avg(airline.ArrDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(Double.valueOf("-1.3768957938942925"), records.get(0).get(0)[0]);

		log.info("In testAllColumnsAvg() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsAvgArrDelayPerCarrier() throws Exception {
		log.info("In testAllColumnsAvgArrDelayPerCarrier() method Entry");

		String statement = "SELECT airline.UniqueCarrier,avg(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals("AQ", records.get(0).get(0)[0]);
		assertEquals(Double.valueOf("-1.3768957938942925"), records.get(0).get(0)[1]);

		log.info("In testAllColumnsAvgArrDelayPerCarrier() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsAvgArrDelayPerCarrierWithWhere() throws Exception {
		log.info("In testAllColumnsAvgArrDelayPerCarrierWithWhere() method Entry");

		String statement = "SELECT airline.UniqueCarrier,avg(airline.ArrDelay) FROM airline where airline.DayOfWeek=1 group by airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals("AQ", records.get(0).get(0)[0]);
		assertEquals(Double.valueOf(-2.2600950118764844), records.get(0).get(0)[1]);

		log.info("In testAllColumnsAvgArrDelayPerCarrierWithWhere() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsAvgArrDelayPlusArrDelay() throws Exception {
		log.info("In testAllColumnsAvgArrDelayPlusArrDelay() method Entry");

		String statement = "SELECT avg(airline.ArrDelay + airline.DepDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(Double.valueOf("-0.9489740409513241"), records.get(0).get(0)[0]);

		log.info("In testAllColumnsAvgArrDelayPlusArrDelay() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsCount() throws Exception {
		log.info("In testRequiredColumnsCount() method Entry");

		String statement = "SELECT count(*) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(46360l, records.get(0).get(0)[0]);

		log.info("In testRequiredColumnsCount() method Exit");
	}

	@Test
	public void testAllColumnsCountAllCountAllSubquery() throws Exception {
		log.info("In testAllColumnsCountAllCountAllSubquery() method Entry");

		String statement = "SELECT count(*) * 100.0/(SELECT count(*) from airlines) from airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 1);
				log.info(Arrays.toString(rec));
			}
		}
		assertNotEquals(0, records.size());
		log.info("In testAllColumnsCountAllCountAllSubquery() method Exit");
	}

	@Test
	public void testAllColumnsCountAllCountAllSubqueryWhereIn() throws Exception {
		log.info("In testAllColumnsCountAllCountAllSubqueryWhereIn() method Entry");

		String statement = "SELECT count(*) * 100.0/(SELECT count(*) from airlines) from airline where MonthOfyear in (11,12)";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 1);
				log.info(Arrays.toString(rec));
			}
		}
		assertNotEquals(0, records.size());
		log.info("In testAllColumnsCountAllCountAllSubqueryWhereIn() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsCountCountAllSubquery() throws Exception {
		log.info("In testAllColumnsCountCountAllSubquery() method Entry");

		String statement = "SELECT count(*) * 100.0/(SELECT count(*) from airlines) from airline where MonthOfYear = 12";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.add(airlinesamplesql, "airlines", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				assertTrue(rec.length == 1);
				log.info(Arrays.toString(rec));
			}
		}
		assertNotEquals(0, records.size());
		log.info("In testAllColumnsCountCountAllSubquery() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsCountSum() throws Exception {
		log.info("In testAllColumnsCountSum() method Entry");

		String statement = "SELECT count(*),sum(airline.ArrDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(46360l, records.get(0).get(0)[0]);
		assertEquals(-63278l, records.get(0).get(0)[1]);

		log.info("In testAllColumnsCountSum() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsCountWithWhere() throws Exception {
		log.info("In testRequiredColumnsCountWithWhere() method Entry");

		String statement = "SELECT count(*) FROM airline WHERE airline.DayofMonth=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(1522l, records.get(0).get(0)[0]);

		log.info("In testRequiredColumnsCountWithWhere() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsMaxWithWhere() throws Exception {
		log.info("In testAllColumnsMaxWithWhere() method Entry");

		String statement = "SELECT max(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(Long.valueOf(44), records.get(0).get(0)[0]);

		log.info("In testAllColumnsMaxWithWhere() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsMinWithWhere() throws Exception {
		log.info("In testAllColumnsMinWithWhere() method Entry");

		String statement = "SELECT min(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(Long.valueOf(-27), records.get(0).get(0)[0]);

		log.info("In testAllColumnsMinWithWhere() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsSubSelectAllColumns() throws Exception {
		log.info("In testAllColumnsSubSelectAllColumns() method Entry");
		String statement = "SELECT * FROM (select * from airline) ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM (select * from airline where airline.DayofMonth = 12) ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsSumWithWhere() throws Exception {
		log.info("In testAllColumnsSumWithWhere() method Entry");

		String statement = "SELECT sum(airline.ArrDelay) FROM airline WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(-362l, records.get(0).get(0)[0]);

		log.info("In testAllColumnsSumWithWhere() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAllColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM (select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testAverageDelayByDestinationAirport() throws Exception {
		log.info("In testAverageDelayByDestinationAirport() method Entry");
		String statement = "SELECT airline.Dest, sum(airline.ArrDelay),avg(airline.ArrDelay) AvgDelay FROM airline GROUP BY airline.Dest";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertEquals(Double.valueOf(-9.663325375452317), avgarrdelay);
		log.info("In testAverageDelayByDestinationAirport() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnAbs() throws Exception {
		log.info("In testColumnAbs() method Entry");

		String statement = "SELECT abs(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnAbsLengthWithExp() throws Exception {
		log.info("In testColumnAbsLengthWithExp() method Entry");

		String statement = "SELECT abs(length(concat(airline.Origin , airline.Dest)))  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT base64decode(base64encode('     ' + uppercase(lowercase(concat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ')) decstring  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnBase64_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnBase64_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT base64encode('     ' + uppercase(lowercase(concat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ') encstring  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnCeil() throws Exception {
		log.info("In testColumnCeil() method Entry");

		String statement = "SELECT airline.ArrDelay, ceil(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnCeilLengthWithExp() throws Exception {
		log.info("In testColumnCeilLengthWithExp() method Entry");

		String statement = "SELECT ceil(length(concat(airline.Origin , airline.Dest)) + 0.4)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnCeilLengthWithExpWithInc() throws Exception {
		log.info("In testColumnCeilLengthWithExpWithInc() method Entry");

		String statement = "SELECT ceil(length(concat(airline.Origin , airline.Dest)) + 0.6)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnExpLengthWithExp() throws Exception {
		log.info("In testColumnExpLengthWithExp() method Entry");

		String statement = "SELECT exp(length(concat(airline.Origin , airline.Dest))) explen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnExponential() throws Exception {
		log.info("In testColumnExponential() method Entry");

		String statement = "SELECT exp(airline.MonthOfYear)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnFloor() throws Exception {
		log.info("In testColumnFloor() method Entry");

		String statement = "SELECT airline.ArrDelay,floor(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnFloorLengthWithExp() throws Exception {
		log.info("In testColumnFloorLengthWithExp() method Entry");

		String statement = "SELECT floor(length(concat(airline.Origin , airline.Dest)) + 0.4)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnFloorLengthWithExpWithInc() throws Exception {
		log.info("In testColumnFloorLengthWithExpWithInc() method Entry");

		String statement = "SELECT floor(length(concat(airline.Origin , airline.Dest)) + 0.6)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnLength() throws Exception {
		log.info("In testColumnLength() method Entry");

		String statement = "SELECT length(airline.Origin)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnLengthWithExp() throws Exception {
		log.info("In testColumnLengthWithExp() method Entry");

		String statement = "SELECT length(concat(airline.Origin , airline.Dest))  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnLengthWithParanthesisExp() throws Exception {
		log.info("In testColumnLengthWithParanthesisExp() method Entry");

		String statement = "SELECT (length(concat(airline.Origin , airline.Dest)) + 0.4) paransum  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnLoge() throws Exception {
		log.info("In testColumnloge() method Entry");

		String statement = "SELECT loge(airline.MonthOfYear) as log  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnLogLengthWithExp() throws Exception {
		log.info("In testColumnLogLengthWithExp() method Entry");

		String statement = "SELECT loge(length(concat(airline.Origin , airline.Dest))) loglen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnLowerCaseWithUpperCaseWithExp() throws Exception {
		log.info("In testColumnLowerCaseWithUpperCaseWithExp() method Entry");

		String statement = "SELECT lowercase(uppercase(concat(airline.Origin , airline.Dest) + 'low') + 'UPP') lowup  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnNormSpacesBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT normalizespaces(base64decode(base64encode('     ' + uppercase(lowercase(concat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      '))) normalizedstring  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnNormSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT normalizespaces(substring(base64decode(base64encode('     ' + uppercase(lowercase(concat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ')) from 0 for 6)) normsubstr  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnPower() throws Exception {
		log.info("In testColumnPower() method Entry");

		String statement = "SELECT airline.ArrDelay, pow(airline.ArrDelay, 2) powcal  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnPowerLengthWithExp() throws Exception {
		log.info("In testColumnPowerLengthWithExp() method Entry");

		String statement = "SELECT pow(length(concat(airline.Origin , airline.Dest)), 2) powlen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnRound() throws Exception {
		log.info("In testColumnRound() method Entry");

		String statement = "SELECT airline.ArrDelay, round(airline.ArrDelay)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnRoundLengthWithExp() throws Exception {
		log.info("In testColumnRoundLengthWithExp() method Entry");

		String statement = "SELECT round(length(concat(airline.Origin , airline.Dest)) + 0.4)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnRoundLengthWithExpWithInc() throws Exception {
		log.info("In testColumnRoundLengthWithExpWithInc() method Entry");

		String statement = "SELECT round(length(concat(airline.Origin , airline.Dest)) + 0.6)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnSqrt() throws Exception {
		log.info("In testColumnSqrt() method Entry");

		String statement = "SELECT airline.MonthOfYear, sqrt(airline.MonthOfYear)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnSqrtLengthWithExp() throws Exception {
		log.info("In testColumnSqrtLengthWithExp() method Entry");

		String statement = "SELECT sqrt(length(concat(airline.Origin , airline.Dest))) sqrtlen  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnSubStringBase64_Dec_EncUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT substring(base64decode(base64encode('     ' + uppercase(lowercase(concat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ')) from 0 for 6) substr  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnTrimUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnTrimUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT trimstr('     ' + uppercase(lowercase(concat(airline.Origin , airline.Dest) + 'LOW') + 'upp') + ' Spaces      ') uplow  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testColumnUpperCaseWithLowerCaseWithExp() throws Exception {
		log.info("In testColumnUpperCaseWithLowerCaseWithExp() method Entry");

		String statement = "SELECT uppercase(lowercase(concat(airline.Origin , airline.Dest) + 'LOW') + 'upp') uplow  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testCountAllColumnsWithWhereAndJoin() throws Exception {
		log.info("In testCountAllColumnsWithWhereAndJoin() method Entry");

		String statement = """
				SELECT count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ' and carriers.Code<>'Code'\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testCountAvgMinMaxSumArrDelayPerCarrier() throws Exception {
		log.info("In testCountAvgMinMaxSumArrDelayPerCarrier() method Entry");

		String statement = """
				SELECT avg(airline.ArrDelay) avgarrdelay, \
				sum(airline.ArrDelay) as sumarrdelay, count(*) as ct, min(airline.ArrDelay) as minarrdelay, max(airline.ArrDelay) as maxarrdelay\
				 FROM airline group by airline.MonthOfYear order by avgarrdelay\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testDate() throws Exception {
		log.info("In testDate() method Entry");

		String statement = "SELECT normalizespaces(' This is   good  work') normspace,currentisodate() isodate FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testDayOfWeekWithMostFlights() throws Exception {
		log.info("In testDayOfWeekWithMostFlights() method Entry");
		String statement = "SELECT airline.DayOfWeek, count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testDelayTimeByDayOfWeek() throws Exception {
		log.info("In testDelayTimeByDayOfWeek() method Entry");
		String statement = "SELECT airline.DayOfWeek, sum(airline.ArrDelay),count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testDistinctUniqueCarrierYearAndMonthWithWhere() throws Exception {
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier,airlines.AirlineYear,airlines.MonthOfYear from airlines where airlines.UniqueCarrier <> 'UniqueCarrier' order by airlines.AirlineYear,airlines.MonthOfYear,airlines.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOr() throws Exception {
		log.info("In testFlightsAndOr() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 or airline.DayOfWeek=3 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) == 8 && ((Long) rec[1]) == 12 || ((Long) rec[3]) == 3);
			}
		}
		log.info("In testFlightsAndOr() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAnd() throws Exception {
		log.info("In testFlightsAndOrAnd() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 or airline.DayOfWeek=3 and airline.DayofMonth=1 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(
						((Long) rec[2]) == 8 && ((Long) rec[1]) == 12 || ((Long) rec[3]) == 3 && ((Long) rec[2]) == 1);
			}
		}
		log.info("In testFlightsAndOrAnd() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesis() throws Exception {
		log.info("In testFlightsAndOrAndParanthesis() method Entry");
		String statement = "SELECT * FROM airline WHERE (airline.DayofMonth=8 and airline.MonthOfYear=12) or (airline.DayOfWeek=3 and airline.DayofMonth=1) ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(
						((Long) rec[2]) == 8 && ((Long) rec[1]) == 12 || ((Long) rec[3]) == 3 && ((Long) rec[2]) == 1);
			}
		}
		log.info("In testFlightsAndOrAndParanthesis() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesisOr() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOr() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and airline.Origin='LIH' ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) == 8 && (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOr() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 4.0 <= airline.DayofMonth / 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) >= 8 && (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthDivideBy2MultipleColumnRight() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth - 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) == 10 && (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMinus2MultipleColumnRight() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth * 2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) == 4 && (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthMultiply2MultipleColumnRight() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth+2 = 8 and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and airline.Origin='LIH' ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) == 6 && (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth+2 and (12=airline.MonthOfYear or 3=airline.DayOfWeek) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) == 6 && (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlus2ColumnRight() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() throws Exception {
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() method Entry");
		String statement = "SELECT * FROM airline WHERE 8 = airline.DayofMonth + airline.DayOfWeek and (airline.MonthOfYear=12 or airline.DayOfWeek=3) and 'LIH' = airline.Origin ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
				assertTrue(((Long) rec[2]) + ((Long) rec[3]) == 8 && (((Long) rec[1]) == 12 || ((Long) rec[3]) == 3)
						&& "LIH".equals(((String) rec[16])));
			}
		}
		log.info("In testFlightsAndOrAndParanthesisOrDayOfMonthPlusDayOfWeekMultipleColumnRight() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsCancelledAndCancellationCode() throws Exception {
		log.info("In testFlightsCancelledAndCancellationCode() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Cancelled,airline.CancellationCode FROM airline WHERE airline.Cancelled = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsCancelledDueToWeather() throws Exception {
		log.info("In testFlightsCancelledDueToWeather() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Cancelled,airline.CancellationCode FROM airline WHERE airline.Cancelled = 1 AND airline.CancellationCode = 'B'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsDistinctUniqueCarrier() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrier() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier from airlines";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsDistinctUniqueCarrierArrDelayDepDelay() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrierArrDelayDepDelay() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier,airlines.ArrDelay,airlines.DepDelay from airlines";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsDistinctUniqueCarrierFlightnumOriginDest() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrierFlightnumOriginDest() method Entry");
		String statement = "select distinct uniquecarrier,flightnum,origin,dest from airlines order by uniquecarrier,flightnum,origin,dest";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsDistinctUniqueCarrierWithWhere() throws Exception {
		log.info("In testFlightsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier from airlines where airlines.UniqueCarrier <> 'UniqueCarrier'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsDivertedDueToWeather() throws Exception {
		log.info("In testFlightsDivertedDueToWeather() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.Diverted,airline.WeatherDelay FROM airline WHERE airline.Diverted = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsDivertedDueToWeatherSortBy() throws Exception {
		log.info("In testFlightsDivertedDueToWeatherSortBy() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth=8 or airline.MonthOfYear=12 ORDER BY airline.DayOfWeek DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testFlightsjoinGroupBy() throws Exception {
		log.info("In testFlightsjoinGroupBy() method Entry");
		String statement = "SELECT airlines.Origin,airports.airport,count(*) FROM airlines inner join airports on airports.iata = airlines.Origin GROUP BY airlines.Origin,airports.airport";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() throws Exception {
		log.info("In testFlightsRequiredColumnsDistinctUniqueCarrierWithWhere() method Entry");
		String statement = "SELECT distinct airlines.UniqueCarrier,airlines.AirlineYear from airlines where airlines.UniqueCarrier <> 'UniqueCarrier' order by airlines.AirlineYear,airlines.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testJoinWithCount() throws Exception {
		log.info("In testJoinWithCount() method Entry");
		String statement = """
				select airline.origin,airports.airport,count(*) FROM airline
				inner join airports on airports.iata = airline.origin
				GROUP BY airline.origin,airports.airport
				""";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
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
		assertEquals(11, total);

		log.info("In testJoinWithCount() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMonthOfYearWithMostFlights() throws Exception {
		log.info("In testMonthOfYearWithMostFlights() method Entry");
		String statement = "SELECT airline.MonthOfYear, count(*) FROM airline GROUP BY airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMultipleAllColumnsOrCondition() throws Exception {
		log.info("In testMultipleAllColumnsOrCondition() method Entry");
		String statement = """
				SELECT * from airline \
				WHERE airline.DayofMonth=8 or airline.MonthOfYear=12\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() throws Exception {
		log.info("In testMultipleRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Entry");
		String statement = """
				SELECT airports.iata,airline.UniqueCarrier,sum(airline.ArrDelay),min(airline.ArrDelay),max(airline.ArrDelay),count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airports.iata,airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testNonAggSqrtAggAvgFunctionWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT sqrt(abs(ArrDelay)) sqrtabs FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3\
				""";

		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testNumberOfFlightsByCarrier() throws Exception {
		log.info("In testNumberOfFlightsByCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier, count(*) FROM airline GROUP BY airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testNumberOfFlightsByDayOfWeek() throws Exception {
		log.info("In testNumberOfFlightsByDayOfWeek() method Entry");
		String statement = "SELECT airline.DayOfWeek, count(*) FROM airline GROUP BY airline.DayOfWeek";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testNumberOfFlightsCancelled() throws Exception {
		log.info("In testNumberOfFlightsCancelled() method Entry");
		String statement = "SELECT count(*) FROM airline WHERE airline.Cancelled = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testNumberOfFlightsDiverted() throws Exception {
		log.info("In testNumberOfFlightsDiverted() method Entry");
		String statement = "SELECT count(*) FROM airline WHERE airline.Diverted = 1";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testPrintAllColumnsCountWithWhereAndJoin() throws Exception {
		log.info("In testPrintAllColumnsCountWithWhereAndJoin() method Entry");

		String statement = """
				SELECT * \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=8 and carriers.Code='AQ' and carriers.Code<>'Code'\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testPrintAvgDelayWithDelayPercentageByUniqueCarrier() throws Exception {
		log.info("In testPrintAvgDelayWithDelayPercentageByUniqueCarrier() method Entry");

		String statement = """
				select uniquecarrier,avg(arrdelay),
				sum(arrdelay)*100.0/(select sum(arrdelay+depdelay)
				from airline)
				from airline_1 group by uniquecarrier order by uniquecarrier
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesqlucs, "airline", airlineheader, airlineheadertypes)
				.add(airlinesamplesqlucs, "airline_1", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int total = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				total++;
				assertTrue(rec.length == 3);
				log.info(Arrays.toString(rec));
			}
		}
		assertEquals(2, total);

		log.info("In testPrintAvgDelayWithDelayPercentageByUniqueCarrier() method Exit");
	}

	@Test
	public void testRequiredColumnBase64Encode() throws Exception {
		log.info("In testRequiredColumnBase64Encode() method Entry");

		String statement = "SELECT base64encode(airline.Origin),base64encode(airline.Dest)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testRequiredColumnNormailizeSpaces() throws Exception {
		log.info("In testRequiredColumnNormailizeSpaces() method Entry");

		String statement = "SELECT normalizespaces(airline.Dest),normalizespaces(' This is   good  work') eg FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsAcosFunction() throws Exception {
		log.info("In testRequiredColumnsAcosFunction() method Entry");
		String statement = "SELECT UniqueCarrier,acos(MonthOfYear/DayOfMonth) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsAcosFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsAsciiFunction() throws Exception {
		log.info("In testRequiredColumnsAsciiFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,ascii(origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsAsciiFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsAsinFunction() throws Exception {
		log.info("In testRequiredColumnsAsinFunction() method Entry");
		String statement = "SELECT UniqueCarrier,asin(MonthOfYear/DayOfMonth) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsAsinFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsAtanFunction() throws Exception {
		log.info("In testRequiredColumnsAtanFunction() method Entry");
		String statement = "SELECT UniqueCarrier,atan(MonthOfYear/DayOfMonth) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsAtanFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCharacFunction() throws Exception {
		log.info("In testRequiredColumnsCharacFunction() method Entry");
		String statement = "SELECT UniqueCarrier,DayofMonth,charac(DayofMonth+65) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCharacFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCharLengthFunction() throws Exception {
		log.info("In testRequiredColumnsCharLengthFunction() method Entry");
		String statement = "SELECT UniqueCarrier,char_length(concat(origin,dest)), character_length(concat(origin,dest)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCharLengthFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCosecFunction() throws Exception {
		log.info("In testRequiredColumnsCosecFunction() method Entry");
		String statement = "SELECT UniqueCarrier,cosec(radians(120)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCosecFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCosineFunction() throws Exception {
		log.info("In testRequiredColumnsCosineFunction() method Entry");
		String statement = "SELECT UniqueCarrier,cos(radians(120)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCosineFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCotangentFunction() throws Exception {
		log.info("In testRequiredColumnsCotangentFunction() method Entry");
		String statement = "SELECT UniqueCarrier,cot(radians(120)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCotangentFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCubeRootFunction() throws Exception {
		log.info("In testRequiredColumnsCubeRootFunction() method Entry");
		String statement = "SELECT UniqueCarrier,cbrt(MonthOfYear) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCubeRootFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCubeRootPIFunction() throws Exception {
		log.info("In testRequiredColumnsCubeRootPIFunction() method Entry");
		String statement = "SELECT UniqueCarrier,cbrt(pii()) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCubeRootPIFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCurdateNowCurtimeFunction() throws Exception {
		log.info("In testRequiredColumnsCurdateNowCurtimeFunction() method Entry");
		String statement = "SELECT UniqueCarrier,curdate(),now(),curtime() FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				log.info(Arrays.toString(record));
				assertTrue(record.length == 4);
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCurdateNowCurtimeFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsCurrentTimeMillisFunction() throws Exception {
		log.info("In testRequiredColumnsCurrentTimeMillisFunction() method Entry");
		String statement = "SELECT UniqueCarrier,current_timemillis() FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsCurrentTimeMillisFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsDegreesFunction() throws Exception {
		log.info("In testRequiredColumnsDegreesFunction() method Entry");
		String statement = "SELECT UniqueCarrier,degrees(2.0943951023931953) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsDegreesFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsDistinctIntersection() throws Exception {
		log.info("In testRequiredColumnsDistinctIntersection() method Entry");

		String statement = """
				select distinct uniquecarrier,flightnum,origin,dest from airlinesamplesql intersect select distinct uniquecarrier,flightnum,origin,dest from airlinesample
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinesamplesql", airlineheader, airlineheadertypes)
				.add(airlinesample, "airlinesample", airlineheader, airlineheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(4l, rec.length);
			}
		}
		assertEquals(201, totalrecords);
		log.info("In testRequiredColumnsDistinctIntersection() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsFunctionAvgDelayFunctionsAvgSumCountSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Entry");
		String statement = "SELECT avg(avgdelay) delay from(SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay) avgdelay,sum(ArrDelay) sumdelay,count(*) cnt FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsFunctionsAvgSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay) FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsFunctionsAvgSumCountSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsAvgSumCountSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,DayofMonth,MonthOfYear,avg(ArrDelay),sum(ArrDelay),count(*) cnt FROM (select airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,airline.ArrDelay from airline) group by UniqueCarrier,DayofMonth,MonthOfYear";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsFunctionsSubSelect() throws Exception {
		log.info("In testRequiredColumnsFunctionsSubSelect() method Entry");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay) sumdelay FROM (select * from airline) group by UniqueCarrier";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexof(origin,substring(origin,1)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfStartposFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfStartposFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexofstartpos(origin,substring(origin,1),0) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfStartposFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfAnyFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfAnyFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexofany(origin,substring(origin,1)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfAnyFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfAnyButFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfAnyButFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexofanybut(origin,'123') FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(0, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfAnyButFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfAnyButNumbersFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfAnyButNumbersFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexofanybut(concat('123',origin),'123') FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(3, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfAnyButNumbersFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfDifferenceFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfDifferenceFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexofdiff(origin, origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(-1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfDifferenceFunction() method Exit");
	}
	
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfIgnoreCaseFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfIgnoreCaseFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexofignorecase(origin, substring(lowercase(origin), 1)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfIgnoreCaseFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIndexOfIgnoreCaseStartposFunction() throws Exception {
		log.info("In testRequiredColumnsIndexOfIgnoreCaseStartposFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,indexofignorecasestartpos(origin, substring(lowercase(origin), 1), 1) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsIndexOfIgnoreCaseStartposFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLastIndexOfFunction() throws Exception {
		log.info("In testRequiredColumnsLastIndexOfFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,lastindexof(origin, substring(origin,1)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLastIndexOfFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLastIndexOfStartposFunction() throws Exception {
		log.info("In testRequiredColumnsLastIndexOfStartposFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,lastindexofstartpos(origin, substring(origin,1), 1) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(1, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLastIndexOfStartposFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLeftPadFunction() throws Exception {
		log.info("In testRequiredColumnsLeftPadFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,leftpad(origin, 6) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals("   "+record[1], record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLeftPadFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLeftPadStringFunction() throws Exception {
		log.info("In testRequiredColumnsLeftPadStringFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,leftpadstring(origin, origin, 6) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(record[1]+""+record[1], record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLeftPadStringFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRemoveAllFunction() throws Exception {
		log.info("In testRequiredColumnsRemoveAllFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,remove(origin, origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(DataSamudayaConstants.EMPTY, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRemoveAllFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRemoveEndFunction() throws Exception {
		log.info("In testRequiredColumnsRemoveEndFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,removeend(origin, origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(DataSamudayaConstants.EMPTY, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRemoveEndFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRemoveEndIgnoreCaseFunction() throws Exception {
		log.info("In testRequiredColumnsRemoveEndIgnoreCaseFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,removeendignorecase(origin, lowercase(origin)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(DataSamudayaConstants.EMPTY, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRemoveEndIgnoreCaseFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRemoveAllIgnoreCaseFunction() throws Exception {
		log.info("In testRequiredColumnsRemoveAllIgnoreCaseFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,removeignorecase(origin, lowercase(origin)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(DataSamudayaConstants.EMPTY, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRemoveAllIgnoreCaseFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRemoveStartFunction() throws Exception {
		log.info("In testRequiredColumnsRemoveStartFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,removestart(origin, origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(DataSamudayaConstants.EMPTY, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRemoveStartFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRemoveStartIgnoreCaseFunction() throws Exception {
		log.info("In testRequiredColumnsRemoveStartIgnoreCaseFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,removestartignorecase(origin, lowercase(origin)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(DataSamudayaConstants.EMPTY, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRemoveStartIgnoreCaseFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRepeatFunction() throws Exception {
		log.info("In testRequiredColumnsRepeatFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,repeat(origin, 3) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(record[1]+DataSamudayaConstants.EMPTY+record[1]+record[1], record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRepeatFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRepeatSeparatorFunction() throws Exception {
		log.info("In testRequiredColumnsRepeatSeparatorFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,repeatseparator(origin,dest, 3) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(record[1]+DataSamudayaConstants.EMPTY+record[2]+record[1]+record[2]+record[1], record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRepeatSeparatorFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsChopFunction() throws Exception {
		log.info("In testRequiredColumnsChopFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,chop(origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				String origin = (String) record[1];
				assertEquals(origin.substring(0, origin.length() - 1), record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsChopFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsGetDigitsFunction() throws Exception {
		log.info("In testRequiredColumnsGetDigitsFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,getdigits(concat(origin,'123')) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals("123", record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsGetDigitsFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRightPadFunction() throws Exception {
		log.info("In testRequiredColumnsRightPadFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,rightpad(origin, 6) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(record[1]+"   ", record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRightPadFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRightPadStringFunction() throws Exception {
		log.info("In testRequiredColumnsRightPadStringFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,rightpadstring(origin, '123', 6) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(record[1]+"123", record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRightPadStringFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWrapFunction() throws Exception {
		log.info("In testRequiredColumnsWrapFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,wrap(origin, '123') FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				String rotatedtext = (String) record[1];				
				assertEquals("123"+rotatedtext+"123", record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsWrapFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWrapIfMissingFunction() throws Exception {
		log.info("In testRequiredColumnsWrapIfMissingFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,wrapifmissing(concat(origin, '123'), '123') FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				String wraptext = (String) record[1];				
				assertEquals("123"+wraptext+"123", record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsWrapIfMissingFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUnwrapFunction() throws Exception {
		log.info("In testRequiredColumnsUnwrapFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,unwrap(concat(concat('123', origin), '123'), '123') FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertEquals(record[1], record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsUnwrapFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUncapitalizeFunction() throws Exception {
		log.info("In testRequiredColumnsUncapitalizeFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,uncapitalize(origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				String text = (String) record[1];
				assertEquals((""+text.charAt(0)).toLowerCase()+text.substring(1), record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsUncapitalizeFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRotateFunction() throws Exception {
		log.info("In testRequiredColumnsRotateFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,rotate(origin, 1) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				String rotatedtext = (String) record[1];
				rotatedtext = rotatedtext.charAt(rotatedtext.length()-1) + rotatedtext.substring(0, rotatedtext.length()-1);
				assertEquals(rotatedtext, record[3]);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRotateFunction() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsInitcapFunction() throws Exception {
		log.info("In testRequiredColumnsInitcapFunction() method Entry");
		String statement = "SELECT UniqueCarrier,initcap(lower(concat(origin,dest))) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsInitcapFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable() throws Exception {
		log.info("In testRequiredColumnsInnerJoinSubSelectInnerJoinAliasTable() method Entry");

		String statement = """
				SELECT ijres.DayofMonth,ijres.MonthOfYear,ijres.UniqueCarrier,ijres.Code FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear) ijres inner \
				join carriers1 on ijres.UniqueCarrier = carriers1.Code\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.add(carriers, "carriers1", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsInsertFunction() throws Exception {
		log.info("In testRequiredColumnsInsertFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,insertstr(origin,dest,0,length(dest)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsInsertFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsInsertLengthLessFunction() throws Exception {
		log.info("In testRequiredColumnsInsertLengthLessFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,insertstr(origin,dest,0,length(dest)-2) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsInsertLengthLessFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIntersection() throws Exception {
		log.info("In testRequiredColumnsIntersection() method Entry");

		String statement = """
				select airlinesamplesql.uniquecarrier, airlinesamplesql.origin from airlinesamplesql intersect select airlinesample.uniquecarrier, airlinesample.dest from airlinesample
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinesamplesql", airlineheader, airlineheadertypes)
				.add(airlinesample, "airlinesample", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2l, rec.length);
			}
		}
		assertNotEquals(0, totalrecords);
		log.info("In testRequiredColumnsIntersection() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIntersectionIntersection() throws Exception {
		log.info("In testRequiredColumnsIntersectionIntersection() method Entry");

		String statement = """
				select airlinesamplesql1.uniquecarrier, airlinesamplesql1.origin from airlinesamplesql1 intersect select airlinesample.uniquecarrier, airlinesample.dest from airlinesample intersect select airlinesamplesql2.uniquecarrier, airlinesamplesql2.dest from airlinesamplesql2
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinesamplesql1", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "airlinesamplesql2", airlineheader, airlineheadertypes)
				.add(airlinesample, "airlinesample", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2l, rec.length);
			}
		}
		assertNotEquals(0l, totalrecords);
		log.info("In testRequiredColumnsIntersectionIntersection() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsIntersectionIntersectionIntersection() throws Exception {
		log.info("In testRequiredColumnsIntersectionIntersectionIntersection() method Entry");

		String statement = """
				select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin intersect select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight1 where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin intersect select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight2 where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin intersect select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight3 where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "flight", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "flight1", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "flight2", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "flight3", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(4l, rec.length);
			}
		}
		assertNotEquals(0l, totalrecords);
		log.info("In testRequiredColumnsIntersectionIntersectionIntersection() method Exit");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUnionUnionUnion() throws Exception {
		log.info("In testRequiredColumnsUnionUnionUnion() method Entry");

		String statement = """
				select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin union select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight1 where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin union select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight2 where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin union select distinct uniquecarrier,monthofyear,dayofmonth,origin from flight3 where airlineyear in (2007) group by uniquecarrier,monthofyear,dayofmonth,origin
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "flight", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "flight1", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "flight2", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "flight3", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(4l, rec.length);
			}
		}
		assertNotEquals(0l, totalrecords);
		log.info("In testRequiredColumnsUnionUnionUnion() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoin() throws Exception {
		log.info("In testRequiredColumnsJoin() method Entry");

		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinSubSelect() throws Exception {
		log.info("In testRequiredColumnsJoinSubSelect() method Entry");

		String statement = """
				SELECT * FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear)\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinSubSelectAliasTable() throws Exception {
		log.info("In testRequiredColumnsJoinSubSelectAliasTable() method Entry");

		String statement = """
				SELECT ijres.DayofMonth,ijres.MonthOfYear,ijres.UniqueCarrier,ijres.Code FROM (SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear) ijres\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinTwoTables() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTables() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Description,airline.Origin,airports.airport \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airline1987, "airline", airlineheader, airlineheadertypes)
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
		assertEquals(500, recordcount);
		log.info("In testRequiredColumnsJoinTwoTables() method Exit");
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testRequiredColumnsJoinTwoTablesColumnMaxWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnMaxWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,max(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testRequiredColumnsJoinTwoTablesColumnMinWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnMinWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,min(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumCountMinMaxWhereNoFilter() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,min(airline.ArrDelay),count(*),max(airline.ArrDelay),sum(airline.ArrDelay) \
				FROM airline where airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testRequiredColumnsJoinTwoTablesColumnSumWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,sum(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesColumnSumWhereNoFilter() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,sum(airline.ArrDelay) \
				FROM airline group by airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinTwoTablesCount() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesCount() method Entry");
		String statement = """
				SELECT count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airline1987, "airline", airlineheader, airlineheadertypes)
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
		assertEquals(500, recordcount);
		log.info("In testRequiredColumnsJoinTwoTablesCount() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinTwoTablesCountWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesCountWhere() method Entry");
		String statement = """
				SELECT count(*) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsJoinTwoTablesWhere() throws Exception {
		log.info("In testRequiredColumnsJoinTwoTablesWhere() method Entry");
		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Description,airline.Origin,airports.airport \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLcaseFunction() throws Exception {
		log.info("In testRequiredColumnsLcaseFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,lcase(origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLcaseFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLeftcharsFunction() throws Exception {
		log.info("In testRequiredColumnsLeftcharsFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,leftchars(concat(origin,dest),4) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLeftcharsFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLeftJoin() throws Exception {
		log.info("In testRequiredColumnsLeftJoin() method Entry");

		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM carriers left join airline on airline.UniqueCarrier = carriers.Code\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLocateFunction() throws Exception {
		log.info("In testRequiredColumnsLocateFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,locate(dest,concat(origin,dest), 0) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLocateFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLowerFunction() throws Exception {
		log.info("In testRequiredColumnsLowerFunction() method Entry");
		String statement = "SELECT UniqueCarrier,lower(concat(origin,dest)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLowerFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsLtrimFunction() throws Exception {
		log.info("In testRequiredColumnsLtrimFunction() method Entry");
		String statement = "SELECT UniqueCarrier,ltrim(concat(concat('   ',dest),'   ')) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsLtrimFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsMonthDayAvgArrDelayPerCarrier() throws Exception {
		log.info("In testRequiredColumnsMonthDayAvgArrDelayPerCarrier() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear,avg(airline.ArrDelay) avgarrdelay, \
				sum(airline.ArrDelay) as sumarrdelay, count(*) as ct, min(airline.ArrDelay) as minarrdelay, max(airline.ArrDelay) as maxarrdelay\
				 FROM airline group by airline.UniqueCarrier,airline.DayofMonth,airline.MonthOfYear order by airline.UniqueCarrier, avgarrdelay\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsOverlayPosFunction() throws Exception {
		log.info("In testRequiredColumnsOverlayPosFunction() method Entry");
		String statement = "SELECT UniqueCarrier,concat(origin,dest),dest,overlay(concat(origin,dest) PLACING dest from 2) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsOverlayPosFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsOverlayPosLengthFunction() throws Exception {
		log.info("In testRequiredColumnsOverlayPosLengthFunction() method Entry");
		String statement = "SELECT UniqueCarrier,concat(origin,dest),dest,overlay(concat(origin,dest) PLACING dest from 0 for 2) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsOverlayPosLengthFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsPositionSubInStrFromStartIndexFunction() throws Exception {
		log.info("In testRequiredColumnsPositionSubInStrFromStartIndexFunction() method Entry");
		String statement = "SELECT UniqueCarrier,position('hnl' in lowercase(concat(origin,dest)) from 2) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsPositionSubInStrFromStartIndexFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsPositionSubInStrFunction() throws Exception {
		log.info("In testRequiredColumnsPositionSubInStrFunction() method Entry");
		String statement = "SELECT UniqueCarrier,position('hnl' in lowercase(concat(origin,dest))) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsPositionSubInStrFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRadiansFunction() throws Exception {
		log.info("In testRequiredColumnsRadiansFunction() method Entry");
		String statement = "SELECT UniqueCarrier,radians(120) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRadiansFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRandFunction() throws Exception {
		log.info("In testRequiredColumnsRandFunction() method Entry");
		String statement = "SELECT UniqueCarrier,rand(current_timemillis()) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRandFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRandIntegerFunction() throws Exception {
		log.info("In testRequiredColumnsRandIntegerFunction() method Entry");
		String statement = "SELECT UniqueCarrier,rand_integer(current_timemillis(),100) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRandIntegerFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT AirlineYear,DayOfWeek,DayofMonth FROM( SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3)\
				""";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsReverseFunction() throws Exception {
		log.info("In testRequiredColumnsReverseFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,reverse(concat(origin,dest)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsReverseFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRightcharsFunction() throws Exception {
		log.info("In testRequiredColumnsRightcharsFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,dest,rightchars(concat(origin,dest),4) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRightcharsFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRightJoin() throws Exception {
		log.info("In testRequiredColumnsRightJoin() method Entry");

		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code \
				FROM airline right join carriers on airline.UniqueCarrier = carriers.Code\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsRtrimFunction() throws Exception {
		log.info("In testRequiredColumnsRtrimFunction() method Entry");
		String statement = "SELECT UniqueCarrier,rtrim(concat(concat('   ',dest),'   ')) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsRtrimFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsSecantFunction() throws Exception {
		log.info("In testRequiredColumnsSecantFunction() method Entry");
		String statement = "SELECT UniqueCarrier,sec(radians(120)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsSecantFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsSineFunction() throws Exception {
		log.info("In testRequiredColumnsSineFunction() method Entry");
		String statement = "SELECT UniqueCarrier,sin(radians(120)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsSineFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsSubSelect() throws Exception {
		log.info("In testRequiredColumnsSubSelect() method Entry");
		String statement = "SELECT AirlineYear,MonthOfYear FROM (select * from airline)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsSubSelectFunctions() throws Exception {
		log.info("In testRequiredColumnsSubSelectFunctions() method Entry");
		String statement = "SELECT sumarrdelay FROM (select sum(airline.ArrDelay) sumarrdelay from airline  group by airline.UniqueCarrier)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsSubSelectFunctionsSumCount() throws Exception {
		log.info("In testRequiredColumnsSubSelectFunctionsSumCount() method Entry");
		String statement = "SELECT sumarrdelay,countrec FROM (select sum(airline.ArrDelay) sumarrdelay, count(*) countrec from airline  group by airline.UniqueCarrier)";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsTanFunction() throws Exception {
		log.info("In testRequiredColumnsTanFunction() method Entry");
		String statement = "SELECT UniqueCarrier,tan(radians(120)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsTanFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsTrimLeadingTrailingBothFunction() throws Exception {
		log.info("In testRequiredColumnsTrimLeadingTrailingBothFunction() method Entry");
		String statement = "SELECT UniqueCarrier,lowercase(concat(origin,dest)),trim(LEADING 'hnl' from lowercase(concat(origin,dest))), trim(TRAILING 'hnl' from lowercase(concat(origin,dest))), trim(BOTH 'hnl' from lowercase(concat(concat(origin,dest),dest))) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 5);
				log.info(Arrays.toString(record));
			}
		}
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsTrimLeadingTrailingBothFunction() method Exit");
	}

	@Test
	public void testRequiredColumnSubStringAlias() throws Exception {
		log.info("In testRequiredColumnSubStringAlias() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin from 0 for 1) as substr  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testRequiredColumnSubStringPos() throws Exception {
		log.info("In testRequiredColumnSubStringPos() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin from 1),airline.Dest,substring(airline.Dest from 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		log.info("In testRequiredColumnSubStringPos() method Exit");
	}

	@Test
	public void testRequiredColumnSubStringPosLength() throws Exception {
		log.info("In testRequiredColumnSubString() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin from 0 for 1),airline.Dest,substring(airline.Dest from 0 for 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUcaseFunction() throws Exception {
		log.info("In testRequiredColumnsUcaseFunction() method Entry");
		String statement = "SELECT UniqueCarrier,origin,ucase(origin) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsUcaseFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUnion() throws Exception {
		log.info("In testRequiredColumnsUnion() method Entry");

		String statement = """
				select airlinesamplesql.uniquecarrier, airlinesamplesql.origin from airlinesamplesql union select airlinesample.uniquecarrier, airlinesample.dest from airlinesample
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinesamplesql", airlineheader, airlineheadertypes)
				.add(airlinesample, "airlinesample", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2l, rec.length);
			}
		}
		assertEquals(12, totalrecords);
		log.info("In testRequiredColumnsUnion() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUnionUnion() throws Exception {
		log.info("In testRequiredColumnsUnionUnion() method Entry");

		String statement = """
				select airlinesamplesql1.uniquecarrier, airlinesamplesql1.origin from airlinesamplesql1 union select airlinesample.uniquecarrier, airlinesample.dest from airlinesample union select airlinesamplesql2.uniquecarrier, airlinesamplesql2.dest from airlinesamplesql2
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinesamplesql1", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "airlinesamplesql2", airlineheader, airlineheadertypes)
				.add(airlinesample, "airlinesample", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2l, rec.length);
			}
		}
		assertNotEquals(0, totalrecords);
		log.info("In testRequiredColumnsUnionUnion() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUnionUnionSum() throws Exception {
		log.info("In testRequiredColumnsUnionUnionSum() method Entry");

		String statement = """
				select sum(flight.delay) from (select uniquecarrier,min(arrdelay) delay from airlinessamplesql1 group by uniquecarrier union select uniquecarrier,max(arrdelay) delay from airlinesamplesql2 group by uniquecarrier) flight
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinessamplesql1", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "airlinesamplesql2", airlineheader, airlineheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1l, rec.length);
			}
		}
		assertNotEquals(0, totalrecords);
		log.info("In testRequiredColumnsUnionUnionSum() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUnionUnionSumGroupBy() throws Exception {
		log.info("In testRequiredColumnsUnionUnionSumGroupBy() method Entry");

		String statement = """
				select uniquecarrier, sum(flight.delay) from (select uniquecarrier,min(arrdelay) delay from airlinessamplesql1 group by uniquecarrier union select uniquecarrier,max(arrdelay) delay from airlinesamplesql2 group by uniquecarrier) flight group by uniquecarrier
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airlinessamplesql1", airlineheader, airlineheadertypes)
				.add(airlinesamplesql, "airlinesamplesql2", airlineheader, airlineheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		long totalrecords = 0;
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			totalrecords += recs.size();
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2l, rec.length);
			}
		}
		assertNotEquals(0, totalrecords);
		log.info("In testRequiredColumnsUnionUnionSumGroupBy() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsUpperFunction() throws Exception {
		log.info("In testRequiredColumnsUpperFunction() method Entry");
		String statement = "SELECT UniqueCarrier,upper(concat(origin,dest)) FROM airline";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		assertNotEquals(0, total);
		log.info("In testRequiredColumnsUpperFunction() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12";

		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereColumnEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth=airline.MonthOfYear\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereGreaterThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth>8 and airline.MonthOfYear>6\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereGreaterThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth>=8 and airline.MonthOfYear>=6\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereLessThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThan() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth<8 and airline.MonthOfYear<6\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereLessThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereLiteralFirst() throws Exception {
		log.info("In testRequiredColumnsWithWhereLiteralFirst() method Entry");

		String statement = """
				SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear \
				FROM airline \
				WHERE 8=airline.DayofMonth and 12=airline.MonthOfYear\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT AirlineYear,MonthOfYear,DayOfWeek,DayofMonth FROM( SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3) where MonthOfYear=12\
				""";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 4);
				assertTrue((Long) record[1] == 12 && (Long) record[2] == 3 && (Long) record[3] == 12);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(130, total);

		log.info("In testRequiredColumnsWithWhereRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhereSubSelectAllColumnsWithWhere() method Entry");
		String statement = """
				SELECT AirlineYear,MonthOfYear,DayofMonth,DayOfWeek FROM \
				(select * from airline where airline.DayofMonth = 12) where DayOfWeek = 3\
				""";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testRequiredColumnTrim() throws Exception {
		log.info("In testRequiredColumnTrim() method Entry");

		String statement = "SELECT trimstr(concat(airline.Origin , ' ')) trmorig ,trimstr(concat(' ' , airline.Dest)) trimdest FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnWithLength() throws Exception {
		log.info("In testRequiredColumnWithLength() method Entry");

		String statement = "SELECT airline.Origin,length(airline.Origin)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnWithLengthsAndLowercase() throws Exception {
		log.info("In testRequiredColumnWithLengthsAndLowercase() method Entry");

		String statement = "SELECT lowercase(airline.Origin),lowercase(airline.Dest),length(airline.Origin),length(airline.Dest)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testRequiredColumnWithMultipleLengths() throws Exception {
		log.info("In testRequiredColumnWithMultipleLengths() method Entry");

		String statement = "SELECT airline.Origin,length(airline.Origin),length(airline.Dest)  FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere() throws Exception {
		log.info("In testRequiredMultipleColumnsJoinTwoTablesColumnMaxWhere() method Entry");
		String statement = """
				SELECT airports.iata,airline.UniqueCarrier,sum(airline.ArrDelay) \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code \
				 inner join airports on airports.iata = airline.Origin \
				WHERE airline.DayofMonth=8 and airline.MonthOfYear=12 group by airports.iata,airline.UniqueCarrier\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testSelectConcatGroupBy() throws Exception {
		log.info("In testSelectConcatGroupBy() method Entry");
		String statement = "SELECT airline.DayofMonth, concat(airline.TailNum, airline.Origin) FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG' group by airline.DayofMonth,airline.TailNum,airline.Origin";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		log.info("In testSelectConcatGroupBy() method Exit");
	}

	@Test
	public void testSelectCountWithWhereInAndLikeClause() throws Exception {
		log.info("In testSelectCountWithWhereInAndLikeClause() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.MonthOfYear in (11,12) and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectCountWithWhereLike() throws Exception {
		log.info("In testSelectCountWithWhereLike() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectCountWithWhereLikeAndBetweenClause() throws Exception {
		log.info("In testSelectCountWithWhereLikeAndBetweenClause() method Entry");
		String statement = "SELECT count(*) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	public void testSelectGroupConcatGroupBy() throws Exception {
		log.info("In testSelectGroupConcatGroupBy() method Entry");
		String statement = "SELECT airline.DayofMonth, group_concat(airline.TailNum, ',') FROM airline where airline.MonthOfYear between 10 and 13 and airline.Origin like 'HNL' and  airline.Dest like 'OGG' group by airline.DayofMonth";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testSelectSumWithNestedAbsAndLengthFunctions() throws Exception {
		log.info("In testSelectSumWithNestedAbsAndLengthFunctions() method Entry");
		String statement = "SELECT sum(abs(length(airline.Origin)) + abs(length(airline.Dest))) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testSelectSumWithNestedAbsFunction() throws Exception {
		log.info("In testSelectSumWithNestedAbsFunction() method Entry");
		String statement = "SELECT sum(abs(airline.MonthOfYear) + airline.ArrDelay) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithNestedCeil() throws Exception {
		log.info("In testSelectSumWithNestedCeil() method Entry");
		String statement = "SELECT sum(ceil(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithNestedExponential() throws Exception {
		log.info("In testSelectSumWithNestedExponential() method Entry");
		String statement = "SELECT sum(exp(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithNestedFloor() throws Exception {
		log.info("In testSelectSumWithNestedFloor() method Entry");
		String statement = "SELECT sum(floor(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithNestedloge() throws Exception {
		log.info("In testSelectSumWithNestedloge() method Entry");
		String statement = "SELECT sum(loge(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@Test
	public void testSelectSumWithNestedPower() throws Exception {
		log.info("In testSelectSumWithNestedPower() method Entry");
		String statement = "SELECT sum(pow(airline.MonthOfYear, 2)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithNestedRound() throws Exception {
		log.info("In testSelectSumWithNestedRound() method Entry");
		String statement = "SELECT sum(round(airline.ArrDelay)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithNestedSqrt() throws Exception {
		log.info("In testSelectSumWithNestedSqrt() method Entry");
		String statement = "SELECT sum(sqrt(airline.MonthOfYear)) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithWhereInAndLikeClause() throws Exception {
		log.info("In testSelectSumWithWhereInAndLikeClause() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11,12) and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithWhereLike() throws Exception {
		log.info("In testSelectSumWithWhereLike() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectSumWithWhereLikeAndBetweenClause() throws Exception {
		log.info("In testSelectSumWithWhereLikeAndBetweenClause() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear between 11 and 12 and airline.Origin like 'HNL' and  airline.Dest like 'OGG'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectWithAggFunctionWithGroupBy() throws Exception {
		log.info("In testSelectWithAggFunctionColumnsWithoutGroupBy() method Entry");
		String statement = "SELECT sum(airline.ArrDelay - 2) FROM airline group by airline.MonthOfYear,airline.DayofMonth";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSelectWithWhereIn() throws Exception {
		log.info("In testSelectWithWhereIn() method Entry");
		String statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11,12)";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

		pipelineconfig.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (11)";
		spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records1 = (List<List<Object[]>>) spsql.collect(true, null);
		double sum11 = 0.0d;
		for (List<Object[]> recs : records1) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				sum11 += (Long) rec[0];
			}
		}

		pipelineconfig.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		statement = "SELECT sum(airline.ArrDelay) FROM airline where airline.MonthOfYear in (12)";
		spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records2 = (List<List<Object[]>>) spsql.collect(true, null);
		double sum12 = 0.0d;
		for (List<Object[]> recs : records2) {
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
	public void testSum() throws Exception {
		log.info("In testSum() method Entry");

		String statement = "SELECT sum(airline.ArrDelay) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-63278l, records.get(0).get(0)[0]);
		log.info("In testSum() method Exit");
	}

	@Test
	public void testSumAvgAvgCount() throws Exception {
		log.info("In testSumAvgAvgCount() method Entry");

		String statement = """
				select sum(airline.arrdelay) as sumadelay,avg(airline.arrdelay) as adelay
				,avg(airline.depdelay) as ddelay,count(*) as recordcnt
				from airline group by airline.uniquecarrier
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSumWithAddition() throws Exception {
		log.info("In testSumWithAddition() method Entry");

		String statement = "SELECT sum(airline.ArrDelay + 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(28636l, records.get(0).get(0)[0]);
		log.info("In testSumWithAddition() method Exit");
	}

	@Test
	public void testSumWithAdditionAndMultiplication() throws Exception {
		log.info("In testSumWithAdditionAndMultiplication() method Entry");

		String statement = "SELECT sum((airline.ArrDelay + 2) * 2.5) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(71590.0, records.get(0).get(0)[0]);
		log.info("In testSumWithAdditionAndMultiplication() method Exit");
	}

	@Test
	public void testSumWithBase64Encode() throws Exception {
		log.info("In testSumWithBase64Encode() method Entry");

		String statement = "SELECT base64encode(airline.Origin) originalias,sum(airline.ArrDelay - 2) FROM airline group by airline.Origin";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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
	public void testSumWithDivision() throws Exception {
		log.info("In testSumWithDivision() method Entry");

		String statement = "SELECT sum(airline.ArrDelay / 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(Double.valueOf(-31639.0), records.get(0).get(0)[0]);
		log.info("In testSumWithDivision() method Exit");
	}

	@Test
	public void testSumWithMultuplication() throws Exception {
		log.info("In testDate() method Entry");

		String statement = "SELECT sum(airline.ArrDelay * 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-126556l, records.get(0).get(0)[0]);
		log.info("In testDate() method Exit");
	}

	@Test
	public void testSumWithSubtraction() throws Exception {
		log.info("In testSumWithSubtraction() method Entry");

		String statement = "SELECT sum(airline.ArrDelay - 2) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-155192l, records.get(0).get(0)[0]);
		log.info("In testSumWithSubtraction() method Exit");
	}

	@Test
	public void testSumWithSubtractionAndMultiplication() throws Exception {
		log.info("In testSumWithSubtractionAndMultiplication() method Entry");

		String statement = "SELECT sum((airline.ArrDelay - 2) * 3.5) FROM airline";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(airlinesamplesql, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.CSV).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(-543172.0, records.get(0).get(0)[0]);
		log.info("In testSumWithSubtractionAndMultiplication() method Exit");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testTotalDelayTimeByMonthOfYear() throws Exception {
		log.info("In testTotalDelayTimeByMonthOfYear() method Entry");
		String statement = "SELECT airline.MonthOfYear, sum(airline.ArrDelay), count(*) FROM airline GROUP BY airline.MonthOfYear";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testTotalDistanceFlownByCarrier() throws Exception {
		log.info("In testTotalDistanceFlownByCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier, sum(airline.Distance) FROM airline GROUP BY airline.UniqueCarrier";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
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

}
