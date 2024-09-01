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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;

public class StreamPipelineSqlBuilderJsonTest extends StreamPipelineBaseTestCommon {
	List<String> githubeventsheader = Arrays.asList("id", "type", "actor", "repo", "payload",
			"public", "created_at", "org");
	List<SqlTypeName> githubeventsheadertypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	Logger log = Logger.getLogger(StreamPipelineSqlBuilderJsonTest.class);

	@BeforeClass
	public static void pipelineSetup() {
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("1");
		pipelineconfig.setBatchsize(DataSamudayaConstants.EMPTY + Runtime.getRuntime().availableProcessors());
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumns() throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM gevents";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 8);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(96, total);

		log.info("In testAllColumns() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM gevents WHERE gevents.type='CreateEvent' and gevents.public='true'";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 8);
				assertTrue("CreateEvent".equals(((String) record[1])) && ((Boolean) record[5]) == true);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(11, total);

		log.info("In testAllColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT gevents.id,gevents.type,gevents.public FROM gevents ";

		int total = 0;
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 3);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(96, total);

		log.info("In testRequiredColumns() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT gevents.id,gevents.type,gevents.public FROM gevents WHERE gevents.type='CreateEvent' and gevents.public='true'";

		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int total = 0;
		for (List<Object[]> recs : records) {
			for (Object[] record : recs) {
				total++;
				assertTrue(record.length == 3);
				log.info(Arrays.toString(record));
			}
		}
		assertEquals(11, total);

		log.info("In testRequiredColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereGreaterThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Entry");

		String statement = """
				SELECT gevents.id,gevents.type,gevents.public \
				FROM gevents \
				WHERE gevents.id>2614896676\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				count++;
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				assertTrue(((Long) rec[0]) > 2614896676l);
			}
		}
		assertEquals(76, count);
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThan() method Entry");

		String statement = """
				SELECT gevents.id,gevents.type,gevents.public \
				FROM gevents \
				WHERE gevents.id<2614896676\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				count++;
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				assertTrue(((Long) rec[0]) < 2614896676l);
			}
		}
		assertEquals(19, count);

		log.info("In testRequiredColumnsWithWhereLessThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereGreaterThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Entry");

		String statement = """
				SELECT gevents.id,gevents.type,gevents.public \
				FROM gevents \
				WHERE gevents.id>=2614896676\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				count++;
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				assertTrue(((Long) rec[0]) >= 2614896676l);
			}
		}
		assertEquals(77, count);
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Entry");

		String statement = """
				SELECT gevents.id,gevents.type,gevents.public \
				FROM gevents \
				WHERE gevents.id<=2614896676\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				count++;
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				assertTrue(((Long) rec[0]) <= 2614896676l);
			}
		}
		assertEquals(20, count);
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLiteralFirst() throws Exception {
		log.info("In testRequiredColumnsWithWhereLiteralFirst() method Entry");

		String statement = """
				SELECT gevents.id,gevents.type,gevents.public \
				FROM gevents \
				WHERE 'CreateEvent'=gevents.type\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				count++;
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
			}
		}
		assertEquals(11, count);
		log.info("In testRequiredColumnsWithWhereLiteralFirst() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereColumnEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Entry");

		String statement = """
				SELECT gevents.id,gevents.type,gevents.public \
				FROM gevents \
				WHERE gevents.type=gevents.type\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				count++;
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
			}
		}
		assertEquals(96, count);
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCount() throws Exception {
		log.info("In testRequiredColumnsCount() method Entry");

		String statement = "SELECT count(*) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(96l, records.get(0).get(0)[0]);

		log.info("In testRequiredColumnsCount() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCountWithWhere() throws Exception {
		log.info("In testRequiredColumnsCountWithWhere() method Entry");

		String statement = "SELECT count(*) FROM gevents WHERE gevents.type='CreateEvent'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(11l, records.get(0).get(0)[0]);

		log.info("In testRequiredColumnsCountWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsSumWithWhere() throws Exception {
		log.info("In testAllColumnsSumWithWhere() method Entry");

		String statement = "SELECT sum(gevents.id) FROM gevents WHERE 'CreateEvent'=gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(28763864287l, records.get(0).get(0)[0]);

		log.info("In testAllColumnsSumWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsMinWithWhere() throws Exception {
		log.info("In testAllColumnsMinWithWhere() method Entry");

		String statement = "SELECT min(gevents.id) FROM gevents WHERE 'PushEvent'=gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(2614896653l, records.get(0).get(0)[0]);

		log.info("In testAllColumnsMinWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsMaxWithWhere() throws Exception {
		log.info("In testAllColumnsMaxWithWhere() method Entry");

		String statement = "SELECT max(gevents.id) FROM gevents WHERE 'IssueCommentEvent'=gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(2614896841l, records.get(0).get(0)[0]);

		log.info("In testAllColumnsMaxWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMultipleAllColumnsAndOrCondition() throws Exception {
		log.info("In testMultipleAllColumnsAndOrCondition() method Entry");
		String statement = """
				SELECT * FROM gevents \
				WHERE gevents.id>2614896670 and gevents.type='CreateEvent'\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 8);
				assertTrue(((Long) rec[0]) > 2614896670l && "CreateEvent".equals(((String) rec[1])));
				count++;
			}
		}
		assertEquals(9, count);
		log.info("In testMultipleAllColumnsAndOrCondition() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMultipleAllColumnsOrAndCondition() throws Exception {
		log.info("In testMultipleAllColumnsOrAndCondition() method Entry");
		String statement = """
				SELECT * FROM gevents \
				WHERE (gevents.id>2614896670 or gevents.type='CreateEvent')\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 8);
				assertTrue(((Long) rec[0]) > 2614896670l || "CreateEvent".equals(((String) rec[1])));
				count++;
			}
		}
		assertEquals(83, count);
		log.info("In testMultipleAllColumnsOrAndCondition() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testNumberOfRecordsEventType() throws Exception {
		log.info("In testNumberOfRecordsEventType() method Entry");
		String statement = "SELECT gevents.type, count(*) FROM gevents GROUP BY gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertEquals(96, sum);
		log.info("In testNumberOfRecordsEventType() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testNumberOfPushEventsWithWhere() throws Exception {
		log.info("In testNumberOfPushEventsWithWhere() method Entry");
		String statement = "SELECT count(*) FROM gevents WHERE gevents.type='PushEvent'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				sum += (Long) rec[0];
			}
		}
		assertEquals(37, sum);
		log.info("In testNumberOfPushEventsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testSumByEventType() throws Exception {
		log.info("In testSumByEventType() method Entry");
		String statement = "SELECT gevents.type, sum(gevents.id) FROM gevents GROUP BY gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		long sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 2);
				sum += (Long) rec[1];
			}
		}
		assertEquals(251030088052l, sum);
		log.info("In testSumByEventType() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testSumTotalIdRecordCount() throws Exception {
		log.info("In testSumTotalIdRecordCount() method Entry");
		String statement = "SELECT gevents.type, sum(gevents.id), count(*) FROM gevents GROUP BY gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		long count = 0;
		long sum = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				count += (Long) rec[2];
				sum += (Long) rec[1];
			}
		}
		assertEquals(96, count);
		assertEquals(251030088052l, sum);
		log.info("In testSumTotalIdRecordCount() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testSumIdAvgIdEventType() throws Exception {
		log.info("In testSumIdAvgIdEventType() method Entry");
		String statement = "SELECT gevents.type, sum(gevents.id),avg(gevents.id) avgid FROM gevents GROUP BY gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		long sum = 0;
		double avgeventtype = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 3);
				avgeventtype += (double) rec[2];
				sum += (Long) rec[1];
			}
		}
		assertEquals(251030088052l, sum);
		assertTrue(1.8304277284450794E10d == avgeventtype);
		log.info("In testSumIdAvgIdEventType() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testEventTypeOrderBy() throws Exception {
		log.info("In testEventTypeOrderBy() method Entry");
		String statement = "SELECT * FROM gevents WHERE gevents.type='PushEvent' or gevents.type='CreateEvent' ORDER BY gevents.type DESC";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int totalrecords = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertTrue(rec.length == 8);
				totalrecords++;
			}
		}

		assertEquals(48, totalrecords);

		log.info("In testEventTypeOrderBy() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testDistinctEventsType() throws Exception {
		log.info("In testDistinctEventsType() method Entry");
		String statement = "SELECT distinct gevents.type FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int totalrecords = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				totalrecords++;
			}
		}
		assertEquals(7, totalrecords);
		log.info("In testDistinctEventsType() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testDistinctEventTypesWithWhere() throws Exception {
		log.info("In testDistinctEventTypesWithWhere() method Entry");
		String statement = "SELECT distinct gevents.type FROM gevents where gevents.type <> 'PushEvent'";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int total = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
				total++;
				assertNotEquals("PushEvent", rec[0]);
			}
		}
		assertEquals(6, total);
		log.info("In testDistinctEventTypesWithWhere() method Exit");
	}

	@Test
	public void testDistinctIdTypeWithWhereAndOrder() throws Exception {
		log.info("In testDistinctIdTypeWithWhereAndOrder() method Entry");
		String statement = "SELECT distinct gevents.id,gevents.type FROM gevents where gevents.type <> 'PushEvent' order by gevents.id,gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
				count++;
			}
		}
		assertEquals(59, count);
		log.info("In testDistinctIdTypeWithWhereAndOrder() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvg() throws Exception {
		log.info("In testAllColumnsAvg() method Entry");

		String statement = "SELECT avg(gevents.id) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		assertEquals(2.6148967505416665E9, records.get(0).get(0)[0]);

		log.info("In testAllColumnsAvg() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvgIdPerType() throws Exception {
		log.info("In testAllColumnsAvgIdPerType() method Entry");

		String statement = "SELECT gevents.type,avg(gevents.id) FROM gevents group by gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double avgid = 0.0d;
		int count = 0;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
				avgid += (double) rec[1];
				count++;
			}
		}
		assertEquals(7, count);
		assertTrue(2.614896754921542E9 == (avgid / 7.0d));

		log.info("In testAllColumnsAvgIdPerType() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvgIdPerTypeWithWhere() throws Exception {
		log.info("In testAllColumnsAvgIdPerTypeWithWhere() method Entry");

		String statement = "SELECT gevents.type,avg(gevents.id) FROM gevents where gevents.type='PushEvent' or gevents.type='CreateEvent' group by gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		int count = 0;
		double avgid = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
				avgid += (double) rec[1];
				count++;
			}
		}
		assertEquals(2, count);
		assertTrue(2.614896752925061E9 == (avgid / 2.0d));

		log.info("In testAllColumnsAvgIdPerTypeWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsAvgIdPerTypeSumAvgCountMinMax() throws Exception {
		log.info("In testAllColumnsAvgIdPerTypeSumAvgCountMinMax() method Entry");

		String statement = """
				SELECT gevents.type,avg(gevents.id) avgid, \
				sum(gevents.id) as sumid, count(*) as ct, min(gevents.id) as minid, max(gevents.id) as maxid\
				 FROM gevents group by gevents.type order by gevents.type, avgid\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(6, rec.length);
			}
		}

		log.info("In testAllColumnsAvgIdPerTypeSumAvgCountMinMax() method Exit");
	}

	@Test
	public void testAllAvgIdPerTypeSumAvgCountMinMax() throws Exception {
		log.info("In testAllAvgIdPerTypeSumAvgCountMinMax() method Entry");

		String statement = """
				SELECT avg(gevents.id) avgid, \
				sum(gevents.id) as sumid, count(*) as ct, min(gevents.id) as minid, max(gevents.id) as maxid\
				 FROM gevents group by gevents.type order by avgid\
				""";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);

		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(5, rec.length);
			}
		}

		log.info("In testAllAvgIdPerTypeSumAvgCountMinMax() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testColumnLength() throws Exception {
		log.info("In testColumnLength() method Entry");

		String statement = "SELECT length(gevents.type) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT gevents.type,length(gevents.type)  FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT gevents.type,length(gevents.type),length(gevents.created_at)  FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT lowercase(gevents.type),lowercase(gevents.actor),length(gevents.type),length(gevents.actor)  FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT uppercase(gevents.type),uppercase(gevents.actor),length(gevents.type),length(gevents.actor)  FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT trim(gevents.type + ' ') trmtype ,trim(' ' + gevents.type) trimtypefront FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT base64encode(gevents.type) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(1, rec.length);
			}
		}
		log.info("In testRequiredColumnBase64Encode() method Exit");
	}

	@Test
	public void testRequiredColumnSubStringAlias() throws Exception {
		log.info("In testRequiredColumnSubStringAlias() method Entry");

		String statement = "SELECT gevents.type,substring(gevents.type,0,1) as substr  FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT gevents.type,substring(gevents.type,0,1) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testRequiredColumnSubString() method Exit");
	}

	@Test
	public void testRequiredColumnNormailizeSpaces() throws Exception {
		log.info("In testRequiredColumnNormailizeSpaces() method Entry");

		String statement = "SELECT normalizespaces(gevents.type),normalizespaces(' This is   good  work') eg FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT normalizespaces(' This is   good  work') normspace,currentisodate() isodate FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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

		String statement = "SELECT normalizespaces(gevents.type + ' This is   good  work') normspace,currentisodate() isodate,count(*) numrec FROM gevents group by gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
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
		log.info("In testSumWithMultuplication() method Entry");

		String statement = "SELECT sum(gevents.id * 2) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(502060176104l, records.get(0).get(0)[0]);
		log.info("In testSumWithMultuplication() method Exit");
	}

	@Test
	public void testSumWithAddition() throws Exception {
		log.info("In testSumWithAddition() method Entry");

		String statement = "SELECT sum(gevents.id + 2) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(251030088244l, records.get(0).get(0)[0]);
		log.info("In testSumWithAddition() method Exit");
	}

	@Test
	public void testSumWithSubtraction() throws Exception {
		log.info("In testSumWithSubtraction() method Entry");

		String statement = "SELECT sum(gevents.id - 2) FROM gevents";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		assertEquals(251030087860l, records.get(0).get(0)[0]);
		log.info("In testSumWithSubtraction() method Exit");
	}

	@Test
	public void testSumWithBase64Encode() throws Exception {
		log.info("In testSumWithBase64Encode() method Entry");
		String statement = "SELECT base64encode(gevents.type) originalias,sum(gevents.id - 2) FROM gevents group by gevents.type";
		StreamPipelineSql spsql = StreamPipelineSqlBuilder.newBuilder()
				.add(githubevents, "gevents", githubeventsheader, githubeventsheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setPipelineConfig(pipelineconfig)
				.setFileformat(DataSamudayaConstants.JSON).setSql(statement).build();
		List<List<Object[]>> records = (List<List<Object[]>>) spsql.collect(true, null);
		double sum = 0.0d;
		for (List<Object[]> recs : records) {
			for (Object[] rec : recs) {
				log.info(Arrays.toString(rec));
				assertEquals(2, rec.length);
			}
		}
		log.info("In testSumWithBase64Encode() method Exit");
	}

	@AfterClass
	public static void pipelineConfigReset() {
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("20");
	}

}
