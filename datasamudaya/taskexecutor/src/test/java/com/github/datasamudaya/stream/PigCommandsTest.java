package com.github.datasamudaya.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.parser.QueryParserDriver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.pig.PigUtils;

public class PigCommandsTest extends StreamPipelineBaseTestCommon {

	String[] airlinehead = { "AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay" };
	String[] carrierhead = { "Code", "Description" };
	List<String> pigQueriesToExecute = new ArrayList<>();
	static String containeralloc;
	static boolean isuseglobaltaskexecutors;
	static String islocal;
	static String user;
	static QueryParserDriver queryParserDriver;
	static Map<String, Object> pigAliasExecutedObjectMap = new ConcurrentHashMap<>();

	@BeforeClass
	public static void setUpConfig() throws Exception {
		containeralloc = pipelineconfig.getContaineralloc();
		isuseglobaltaskexecutors = pipelineconfig.getUseglobaltaskexecutors();
		islocal = pipelineconfig.getLocal();
		user = pipelineconfig.getUser();
		queryParserDriver = PigUtils.getQueryParserDriver("pig");
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
		pipelineconfig.setUseglobaltaskexecutors(true);
		pipelineconfig.setLocal("false");
		pipelineconfig.setUser("arun");
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
		Utils.launchContainers("arun", tejobid);
	}

	@Test
	public void testPigLoad() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);\n");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		List<List> results = (List<List>) PigUtils.executeCollect(
				lp, "data", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List recordspart : results) {
			totalrecords += recordspart.size();
		}
		assertEquals(46361, totalrecords);
	}

	@Test
	public void testPigLoadForEach() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Map<String, Object>> recordspart : results) {
			totalrecords += recordspart.size();
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("MonthOfYear"));
			}
		}
		assertEquals(46361, totalrecords);
	}

	@Test
	public void testPigLoadForEachDumpAll() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add(
				"data2 = FOREACH data GENERATE UniqueCarrier, SUM(ArrDelay) as sumdelay,COUNT(*) as cnt,AVG(ArrDelay) as avgarrdelay,SUM(DepDelay) as sumdepdelay,AVG(DepDelay) as avgdepdelay;");
		pigQueriesToExecute.add("carriers = LOAD '/carriers' AS (Code: chararray,Description: chararray);");
		pigQueriesToExecute.add("data3 = JOIN data2 BY UniqueCarrier, carriers BY Code;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data3", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Map<String, Object>> recordspart : results) {
			totalrecords += recordspart.size();
			for (Map<String, Object> map : recordspart) {
				assertEquals(53, map.size());
			}
		}
	}

	@Test
	public void testPigLoadForEachOrder() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		
		pigQueriesToExecute.add("data2 = ORDER data1 BY UniqueCarrier ASC, MonthOfYear ASC, DayofMonth ASC;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data2", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Map<String, Object>> recordspart : results) {
			totalrecords += recordspart.size();
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("MonthOfYear"));
			}
		}
		assertEquals(46361, totalrecords);
	}

	@Test
	public void testPigLoadForEachOrderFilter() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		pigQueriesToExecute.add("data2 = ORDER data1 BY UniqueCarrier ASC, MonthOfYear ASC, DayofMonth ASC;");
		pigQueriesToExecute.add("filtered_data = FILTER data2 BY MonthOfYear > 11 AND DayofMonth >= 6;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "filtered_data",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Map<String, Object>> recordspart : results) {
			totalrecords += recordspart.size();
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("MonthOfYear"));
			}
		}
		assertEquals(3389, totalrecords);
	}

	@Test
	public void testPigLoadForEachOrderFilterDistinct() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		
		pigQueriesToExecute.add("data2 = ORDER data1 BY UniqueCarrier ASC, MonthOfYear ASC, DayofMonth ASC;");
		
		pigQueriesToExecute.add("filtered_data = FILTER data2 BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("distinct_data = Distinct filtered_data;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "distinct_data",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Map<String, Object>> recordspart : results) {
			totalrecords += recordspart.size();
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("MonthOfYear"));
			}
		}
		assertEquals(26, totalrecords);
	}

	@Test
	public void testPigLoadCount() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE COUNT(*) as cnt;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("cnt"));
				assertEquals(46361, map.get("cnt"));
			}
		}
	}

	@Test
	public void testPigLoadCountWithGrpBy() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, COUNT(*) as cnt;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("cnt"));
				assertTrue(map.containsKey("UniqueCarrier"));
				if (((int) map.get("cnt")) == 1) {
					assertEquals("UniqueCarrier", map.get("UniqueCarrier"));
				} else if (((int) map.get("cnt")) == 46360) {
					assertEquals("AQ", map.get("UniqueCarrier"));
				}
			}
		}
	}

	@Test
	public void testPigLoadSum() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE SUM(ArrDelay) as sumarrdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("sumarrdelay"));
				assertEquals(-63278l, map.get("sumarrdelay"));
			}
		}
	}

	@Test
	public void testPigLoadSumDelays() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE SUM(ArrDelay+DepDelay) as sumdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("sumdelay"));
				assertEquals(-43110l, map.get("sumdelay"));
			}
		}
	}

	@Test
	public void testPigLoadSumWithGrpBy() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, SUM(ArrDelay) as sumarrdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("sumarrdelay"));
				assertTrue(map.containsKey("UniqueCarrier"));
				if (((long) map.get("sumarrdelay")) == 0) {
					assertEquals("UniqueCarrier", map.get("UniqueCarrier"));
				} else if (((long) map.get("sumarrdelay")) == -63278) {
					assertEquals("AQ", map.get("UniqueCarrier"));
				}
			}
		}
	}

	@Test
	public void testPigLoadAvg() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE AVG(ArrDelay) as avgarrdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("avgarrdelay"));
				assertEquals(-1.3768957938942925, map.get("avgarrdelay"));
			}
		}
	}

	@Test
	public void testPigLoadAvgWithGrpBy() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, AVG(ArrDelay) as avgarrdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("avgarrdelay"));
				assertTrue(map.containsKey("UniqueCarrier"));
				if (((double) map.get("avgarrdelay")) == 0) {
					assertEquals("UniqueCarrier", map.get("UniqueCarrier"));
				} else if (((double) map.get("avgarrdelay")) == -1.364897219645823) {
					assertEquals("AQ", map.get("UniqueCarrier"));
				}
			}
		}
	}

	@Test
	public void testPigLoadAbs() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE ArrDelay, abs(ArrDelay) as absarrdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("ArrDelay"));
				assertTrue(map.containsKey("absarrdelay"));
				assertEquals(map.get("absarrdelay"), Math.abs((long) map.get("ArrDelay")));
			}
		}
	}

	@Test
	public void testPigLoadLength() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, length(UniqueCarrier) as lenuc;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("lenuc"));
				assertEquals(Long.valueOf(((String) map.get("UniqueCarrier")).length()), map.get("lenuc"));
			}
		}
	}

	@Test
	public void testPigLoadNormalizespaces() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add(
				"data1 = FOREACH data GENERATE UniqueCarrier, normalizespaces('Test      Test     Test   Testd') as nsp;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("nsp"));
				assertEquals("Test Test Test Testd", map.get("nsp"));
			}
		}
	}

	@Test
	public void testPigLoadNormalizespaces_2() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute
				.add("data1 = FOREACH data GENERATE normalizespaces('Test      Test     Test   Testd') as nsp;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("nsp"));
				assertEquals("Test Test Test Testd", map.get("nsp"));
			}
		}
	}

	@Test
	public void testPigLoadExpressionsAddition() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute
				.add("data1 = FOREACH data GENERATE MonthOfYear, DayofMonth, MonthOfYear+DayofMonth as summoydom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("summoydom"));
				assertEquals(((long) map.get("MonthOfYear")) + ((long) map.get("DayofMonth")), map.get("summoydom"));
			}
		}
	}

	@Test
	public void testPigLoadExpressionsSubtract() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute
				.add("data1 = FOREACH data GENERATE MonthOfYear, DayofMonth, MonthOfYear-DayofMonth as submoydom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("submoydom"));
				assertEquals(((long) map.get("MonthOfYear")) - ((long) map.get("DayofMonth")), map.get("submoydom"));
			}
		}
	}

	@Test
	public void testPigLoadExpressionsMultiply() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute
				.add("data1 = FOREACH data GENERATE MonthOfYear, DayofMonth, MonthOfYear*DayofMonth as mulmoydom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("mulmoydom"));
				assertEquals(((long) map.get("MonthOfYear")) * ((long) map.get("DayofMonth")), map.get("mulmoydom"));
			}
		}
	}

	@Test
	public void testPigLoadExpressionsDivide() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute
				.add("data1 = FOREACH data GENERATE AirlineYear, MonthOfYear, AirlineYear/MonthOfYear as divyearmon;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("AirlineYear"));
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("divyearmon"));
				assertEquals(
						((long) map.get("AirlineYear"))
								/ (Double.valueOf(map.get("MonthOfYear") + DataSamudayaConstants.EMPTY)),
						map.get("divyearmon"));
			}
		}
	}

	@Test
	public void testPigLoadExpressionsMultipleOpertors() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add(
				"data1 = FOREACH data GENERATE AirlineYear, MonthOfYear, DayofMonth, AirlineYear/MonthOfYear*DayofMonth as divmulyearmon;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(4, map.size());
				assertTrue(map.containsKey("AirlineYear"));
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("divmulyearmon"));
				assertEquals(((long) map.get("AirlineYear"))
						/ (Double.valueOf(map.get("MonthOfYear") + DataSamudayaConstants.EMPTY))
						* ((long) map.get("DayofMonth")), map.get("divmulyearmon"));
			}
		}
	}

	@Test
	public void testPigLoadUppercase() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");

		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, uppercase(UniqueCarrier) as ucuc;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);

		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("ucuc"));
				assertEquals(((String) map.get("UniqueCarrier")).toUpperCase(), map.get("ucuc"));
			}
		}
	}

	@Test
	public void testPigLoadLowercase() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");

		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, lowercase(UniqueCarrier) as lcuc;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);

		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("lcuc"));
				assertEquals(((String) map.get("UniqueCarrier")).toLowerCase(), map.get("lcuc"));
			}
		}
	}

	@Test
	public void testPigLoadBase64encode() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute
				.add("data1 = FOREACH data GENERATE UniqueCarrier, base64encode(UniqueCarrier) as base64uc;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);

		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("base64uc"));
				assertEquals(map.get("base64uc"),
						Base64.getEncoder().encodeToString(((String) map.get("UniqueCarrier")).getBytes()));
			}
		}
	}

	@Test
	public void testPigLoadBase64decode() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute
				.add("data1 = FOREACH data GENERATE UniqueCarrier, base64encode(UniqueCarrier) as base64uc;");
		
		pigQueriesToExecute
				.add("data2 = FOREACH data1 GENERATE UniqueCarrier, base64decode(base64uc) as base64decode;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data2", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("base64decode"));
				assertEquals(map.get("UniqueCarrier"), map.get("base64decode"));
			}
		}
	}

	@Test
	public void testPigLoadPower() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, pow(DayofMonth, 2) as powdom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("powdom"));
				assertEquals(Math.pow(((long) map.get("DayofMonth")), 2), map.get("powdom"));
			}
		}
	}

	@Test
	public void testPigLoadCeil() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");

		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, ceil(DayofMonth) as ceildom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);

		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("ceildom"));
				assertEquals(Math.ceil(((long) map.get("DayofMonth"))), map.get("ceildom"));
			}
		}
	}

	@Test
	public void testPigLoadFloor() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");

		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, floor(DayofMonth) as floordom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("floordom"));
				assertEquals(Math.ceil(((long) map.get("DayofMonth"))), map.get("floordom"));
			}
		}
	}

	@Test
	public void testPigLoadSqrt() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, sqrt(DayofMonth) as sqrtdom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("sqrtdom"));
				assertEquals(Math.sqrt(((long) map.get("DayofMonth"))), map.get("sqrtdom"));
			}
		}
	}

	@Test
	public void testPigLoadExp() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");

		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, exp(DayofMonth) as expdom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("expdom"));
				assertEquals(Math.exp(((long) map.get("DayofMonth"))), map.get("expdom"));
			}
		}
	}

	@Test
	public void testPigLoadLoge() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, loge(DayofMonth) as logdom;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);

		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Map<String, Object>> recordspart : results) {
			for (Map<String, Object> map : recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("logdom"));
				assertEquals(Math.log(((long) map.get("DayofMonth"))), map.get("logdom"));
			}
		}
	}

	@AfterClass
	public static void resetConfig() {
		pipelineconfig.setContaineralloc(containeralloc);
		pipelineconfig.setUseglobaltaskexecutors(isuseglobaltaskexecutors);
		pipelineconfig.setLocal(islocal);
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
	}

}
