package com.github.datasamudaya.stream;

import static java.util.Objects.nonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
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
		pipelineconfig.setLocal("true");
		pipelineconfig.setUser("arun");
		pipelineconfig.setTejobid(tejobid);
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
		if(pipelineconfig.getLocal().equals("false")) {
			Utils.launchContainers("arun", tejobid);
		}
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(3, obj.length);
			}
		}
		assertEquals(46360, totalrecords);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data2", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(3, obj.length);			
			}
		}
		assertEquals(46361, totalrecords);
	}
	
	@Test
	public void testPigLoadFilter() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(29, obj.length);
				assertTrue((Long)obj[1]>11 && (Long)obj[2]>=6);
			}
		}
		assertEquals(3389, totalrecords);
	}
	
	@Test
	public void testPigLoadFilterStore() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("STORE filtered_data into '/examplestest1';");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		assertTrue(nonNull(lp));
		PigUtils.executeStore(lp, DataSamudayaConstants.EMPTY, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
	}
	
	
	@Test
	public void testPigLoadFilterFilter() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("filtered_data1 = FILTER filtered_data BY DayOfWeek > 1;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data1",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertTrue((Long)obj[1]>11 && (Long)obj[2]>=6 && (Long)obj[3]>1);
				assertEquals(29, obj.length);
			}
		}
		assertTrue(totalrecords<3389);
	}
	
	@Test
	public void testPigLoadFilterExecCollectFilterExecCollectForEach() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("filtered_data1 = FILTER filtered_data BY DayOfWeek > 1;");
		pigQueriesToExecute.add("data1 = FOREACH filtered_data GENERATE SUM(ArrDelay) as sumarrdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(29, obj.length);
			}
		}
		assertEquals(3389, totalrecords);
		
		results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data1",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(29, obj.length);
			}
		}
		assertTrue(totalrecords<3389);
		
		results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(1, obj.length);
				assertTrue(nonNull(obj[0]));
			}
		}
		assertEquals(1, totalrecords);
	}
	
	
	@Test
	public void testPigLoadFilterExecCollectFilterExecCollectForEachForEach() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("filtered_data1 = FILTER filtered_data BY DayOfWeek > 1;");
		pigQueriesToExecute.add("data1 = FOREACH filtered_data GENERATE SUM(ArrDelay) as sumarrdelay;");
		pigQueriesToExecute.add("data2 = FOREACH filtered_data1 GENERATE SUM(ArrDelay) as sumarrdelay;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertTrue((Long)obj[1]>11 && (Long)obj[2]>=6);
				assertEquals(29, obj.length);
			}
		}
		assertEquals(3389, totalrecords);
		
		results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data1",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertTrue((Long)obj[1]>11 && (Long)obj[2]>=6 && (Long)obj[3]>1);
				assertEquals(29, obj.length);
			}
		}
		assertTrue(totalrecords<3389);
		
		results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(1, obj.length);
			}
		}
		assertEquals(1, totalrecords);
		
		results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data2",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(1, obj.length);
			}
		}
		assertEquals(1, totalrecords);
	}

	@Test
	public void testPigLoadFilterForEachFilterForEach() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("data1 = FOREACH filtered_data GENERATE SUM(ArrDelay) as sumarrdelay;");
		pigQueriesToExecute.add("filtered_data1 = FILTER data1 BY sumarrdelay < 1000;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data1",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(1, obj.length);
				assertTrue(((long)obj[0]<1000));
			}
		}
		assertEquals(1, totalrecords);
	}
	
	
	@Test
	public void testPigLoadFilterForEachStore() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("data1 = FOREACH filtered_data GENERATE UniqueCarrier, SUM(ArrDelay) as sumarrdelay;");
		pigQueriesToExecute.add("STORE data1 into '/examplestest';");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		assertTrue(nonNull(lp));
		PigUtils.executeStore(lp, DataSamudayaConstants.EMPTY, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
	}
	
	
	@Test
	public void testPigLoadFilterForEachColumnsChangeStore() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("data1 = FOREACH filtered_data GENERATE SUM(ArrDelay) as sumarrdelay, UniqueCarrier;");
		pigQueriesToExecute.add("STORE data1 into '/examplestest';");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		assertTrue(nonNull(lp));
		PigUtils.executeStore(lp, DataSamudayaConstants.EMPTY, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
	}
	
	
	@Test
	public void testPigLoadFilterForEachMultipleStores() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("data1 = FOREACH filtered_data GENERATE UniqueCarrier, SUM(ArrDelay) as sumarrdelay;");
		pigQueriesToExecute.add("STORE data1 into '/examplestest';");
		pigQueriesToExecute.add("STORE filtered_data into '/examplestest';");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		assertTrue(nonNull(lp));
		PigUtils.executeStore(lp, DataSamudayaConstants.EMPTY, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
	}
	
	@Test
	public void testPigLoadFilterForEachFilterForEachGroup() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("filtered_data = FILTER data BY MonthOfYear > 11 AND DayofMonth >= 6;");
		pigQueriesToExecute.add("data1 = FOREACH filtered_data GENERATE UniqueCarrier,SUM(ArrDelay) as sumarrdelay;");
		pigQueriesToExecute.add("filtered_data1 = FILTER data1 BY sumarrdelay < 1000;");
		LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data1",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(2, obj.length);
				assertTrue(((Long)obj[1])<1000);
			}
		}
		assertEquals(1, totalrecords);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "filtered_data",
				pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for (List<Object[]> recordspart : results) {
			totalrecords += recordspart.size();
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(3, obj.length);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(1, obj.length);
				assertEquals(46361, obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(2, obj.length);
				if ((int) obj[1] == 1) {
					assertEquals("UniqueCarrier", obj[0]);
				} else if ((int) obj[1] == 46360) {
					assertEquals("AQ", obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				assertEquals(1, obj.length);
				assertEquals(-63278, obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				assertEquals(1, obj.length);
				assertEquals(-43110, obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(2, obj.length);
				if ((int) obj[1] == 0) {
					assertEquals("UniqueCarrier", obj[0]);
				} else if ((int) obj[1] == -63278) {
					assertEquals("AQ", obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(1, obj.length);
				assertEquals(-1.3768957938942925, obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(2, obj.length);
				if ((double) obj[1] == 0) {
					assertEquals("UniqueCarrier", obj[0]);
				} else if ((double) obj[1] == -1.364897219645823) {
					assertEquals("AQ", obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(2, obj.length);
				assertEquals(obj[1], Math.abs((int) obj[0]));
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(2, obj.length);			
				assertEquals(Long.valueOf(((String) obj[0]).length()), obj[1]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(2, obj.length);
				assertEquals("Test Test Test Testd", obj[1]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(1, obj.length);
				assertEquals("Test Test Test Testd", obj[0]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(3, obj.length);				
				assertEquals(((long) obj[0]) + ((long) obj[1]), obj[2]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(3, obj.length);				
				assertEquals(((long) obj[0]) - ((long) obj[1]), obj[2]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(3, obj.length);				
				assertEquals(((long) obj[0]) * ((long) obj[1]),obj[2]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(3, obj.length);
				assertEquals(
						((int) obj[0])
								/ (Double.valueOf(obj[1]+DataSamudayaConstants.EMPTY)),
								obj[2]);
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
		List<List<Object[]>> results = (List<List<Object[]>>) PigUtils.executeCollect(
				lp, "data1", pipelineconfig.getUser(),
				jobid, tejobid, pipelineconfig);
		for (List<Object[]> recordspart : results) {
			for (Object[] obj : recordspart) {
				log.info(Arrays.toString(obj));
				assertEquals(4, obj.length);				
				assertEquals(((int) obj[0])
						/ (Double.valueOf(obj[1] + DataSamudayaConstants.EMPTY))
						* ((int) obj[2]), obj[3]);
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
				assertEquals(Math.ceil(((int) map.get("DayofMonth"))), map.get("floordom"));
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
	
	@Test
	public void testPigLoadMultipleAssignment() throws Exception {
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
				+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add(
				"data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, loge(DayofMonth) as logdom;");
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

	@AfterClass
	public static void resetConfig() {
		pipelineconfig.setContaineralloc(containeralloc);
		pipelineconfig.setUseglobaltaskexecutors(isuseglobaltaskexecutors);
		pipelineconfig.setLocal(islocal);
		pipelineconfig.setUser(user);
		pipelineconfig.setTejobid(tejobid);
	}

}
