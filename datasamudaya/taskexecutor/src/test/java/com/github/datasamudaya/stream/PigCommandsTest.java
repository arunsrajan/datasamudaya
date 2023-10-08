package com.github.datasamudaya.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pig.parser.QueryParserDriver;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.pig.PigQueryExecutor;
import com.github.datasamudaya.stream.pig.PigUtils;

public class PigCommandsTest extends StreamPipelineBaseTestCommon{
	
	String[] airlinehead = {"AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	String[] carrierhead = {"Code", "Description"};
	List<String> pigQueriesToExecute = new ArrayList<>();
	static String containeralloc;
	static boolean isuseglobaltaskexecutors;
	static String islocal;
	static String user;
	static QueryParserDriver queryParserDriver;
	static Map<String, Object> pigAliasExecutedObjectMap  = new ConcurrentHashMap<>();
	
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
		Utils.launchContainers("arun", tejobid);
	}
	
	@Test
	public void testCachedStreamCache() throws Exception {
		log.info("In testCachedStreamCache() method Entry");
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);		
		StreamPipeline<Map<String, Object>> csp = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlinehead).map(csv->{
					String[] header = airlinehead; 
					Map<String, Object> map = new HashMap<>();
					for(String column:header) {
						map.put(column, csv.get(column));
					}
					return map;
				});
		log.info(csp.getPigtasks());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filtered = csp.filter(map->map.get("AirlineYear").equals("2007") &&  map.get("MonthOfYear").equals("12")).cache();
		log.info(filtered.getPigtasks());
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		List<List<Map<String, Object>>> records = filtered.map(map->map).collect(true, null);
		for(List<Map<String, Object>> partitions:records) {
			for(Map<String, Object> map: partitions) {
				log.info(map);
				assertEquals("2007", map.get("AirlineYear"));
				assertEquals("12", map.get("MonthOfYear"));
			}
		}
		log.info("In testCachedStreamCache() method Exit");		
	}
	
	@Test
	public void testCachedStreamCaches() throws Exception {
		log.info("In testCachedStreamCaches() method Entry");
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);		
		StreamPipeline<Map<String, Object>> csp = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlinehead).map(csv->{
					String[] header = airlinehead; 
					Map<String, Object> map = new HashMap<>();
					for(String column:header) {
						map.put(column, csv.get(column));
					}
					return map;
				});
		log.info(csp.getPigtasks());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredmonthofyear = csp.filter(map->map.get("AirlineYear").equals("2007") &&  map.get("MonthOfYear").equals("12")).cache();
		log.info(filteredmonthofyear.getPigtasks());
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		List<List> records = filteredmonthofyear.filter(map->map.get("DayofMonth").equals("4")).collect(true, null);
		for(List<Map<String, Object>> partitions:records) {
			for(Map<String, Object> map: partitions) {
				log.info(map);
				assertEquals("2007", map.get("AirlineYear"));
				assertEquals("4", map.get("DayofMonth"));
				assertEquals("12", map.get("MonthOfYear"));
			}
		}
		log.info("In testCachedStreamCaches() method Exit");		
	}
	
	
	@Test
	public void testCachedStreamMultipleFilterCaches() throws Exception {
		log.info("In testCachedStreamMultipleFilterCaches() method Entry");
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);		
		StreamPipeline<Map<String, Object>> csp = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlinehead).map(csv->{
					String[] header = airlinehead; 
					Map<String, Object> map = new HashMap<>();
					for(String column:header) {
						map.put(column, csv.get(column));
					}
					return map;
				});
		log.info(csp.getPigtasks());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredmonthofyear = csp.filter(map->map.get("AirlineYear").equals("2007") &&  map.get("MonthOfYear").equals("12")).cache();
		log.info(filteredmonthofyear.getPigtasks());
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredfilteredmonthofyear = filteredmonthofyear.filter(map->map.get("DayofMonth").equals("4")).cache();
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		List<List> records = filteredfilteredmonthofyear.map(val->val).collect(true, null);
		for(List<Map<String, Object>> partitions:records) {
			for(Map<String, Object> map: partitions) {
				log.info(map);
				assertEquals("2007", map.get("AirlineYear"));
				assertEquals("4", map.get("DayofMonth"));
				assertEquals("12", map.get("MonthOfYear"));
			}
		}
		log.info("In testCachedStreamMultipleFilterCaches() method Exit");		
	}
	
	@Test
	public void testCachedStreamMultipleFilterSaveToFile() throws Exception {
		log.info("In testCachedStreamMultipleFilterSaveToFile() method Entry");
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);		
		StreamPipeline<Map<String, Object>> csp = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlinehead).map(csv->{
					String[] header = airlinehead; 
					Map<String, Object> map = new HashMap<>();
					for(String column:header) {
						map.put(column, csv.get(column));
					}
					return map;
				});
		log.info(csp.getPigtasks());
		
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredmonthofyear = csp.filter(map->map.get("AirlineYear").equals("2007") &&  map.get("MonthOfYear").equals("12")).cache();
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		filteredmonthofyear.map(val->val).saveAsTextFilePig(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), DataSamudayaConstants.FORWARD_SLASH + "examplespig"+
				DataSamudayaConstants.FORWARD_SLASH + "pig-" + System.currentTimeMillis());
		log.info(filteredmonthofyear.getPigtasks());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredfilteredmonthofyear = filteredmonthofyear.filter(map->map.get("DayofMonth").equals("4")).cache();
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		filteredfilteredmonthofyear.map(val->val).saveAsTextFilePig(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), DataSamudayaConstants.FORWARD_SLASH + "examplespig"+
				DataSamudayaConstants.FORWARD_SLASH + "pig-" + System.currentTimeMillis());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		filteredfilteredmonthofyear.map(val->val).saveAsTextFilePig(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), DataSamudayaConstants.FORWARD_SLASH + "examplespig"+
				DataSamudayaConstants.FORWARD_SLASH + "pig-" + System.currentTimeMillis());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		filteredmonthofyear.map(val->val).saveAsTextFilePig(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), DataSamudayaConstants.FORWARD_SLASH + "examplespig"+
				DataSamudayaConstants.FORWARD_SLASH + "pig-" + System.currentTimeMillis());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredfilteredmonthofyeardom = filteredmonthofyear.filter(map->map.get("DayofMonth").equals("5")).cache();
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		filteredfilteredmonthofyeardom.map(val->val).saveAsTextFilePig(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), DataSamudayaConstants.FORWARD_SLASH + "examplespig"+
				DataSamudayaConstants.FORWARD_SLASH + "pig-" + System.currentTimeMillis());
		
		log.info("In testCachedStreamMultipleFilterSaveToFile() method Exit");		
	}
	
	@Test
	public void testCachedStreamCachesJoin() throws Exception {
		log.info("In testCachedStreamCachesJoin() method Entry");
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);		
		StreamPipeline<Map<String, Object>> csp = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig, airlinehead).map(csv->{
					String[] header = airlinehead; 
					Map<String, Object> map = new HashMap<>();
					for(String column:header) {
						map.put(column, csv.get(column));
					}
					return map;
				});
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> carriersstream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig, carrierhead).map(csv->{
					String[] head = carrierhead; 
					Map<String, Object> map = new HashMap<>();
					for(String column:head) {
						map.put(column, csv.get(column));
					}
					return map;
				});
		log.info(csp.getPigtasks());
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredmonthofyear = csp.filter(map->map.get("AirlineYear").equals("2007") &&  map.get("MonthOfYear").equals("12")).cache();
		log.info(filteredmonthofyear.getPigtasks());
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		StreamPipeline<Map<String, Object>> filteredmonthofyeardom = filteredmonthofyear.filter(map->map.get("DayofMonth").equals("4")).cache();
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pipelineconfig.setJobid(jobid);
		List<List> records = filteredmonthofyeardom.map(rec->rec).join(carriersstream.map(rec->rec), (map1,map2)->{
			return map1.get("UniqueCarrier").equals(map2.get("Code"));
		}).collect(true, null);
		for(List<Tuple2<Map<String,Object>,Map<String,Object>>> partitions:records) {
			for(Tuple2<Map<String,Object>,Map<String,Object>> values: partitions) {
				log.info(values);
				assertEquals("2007", values.v1.get("AirlineYear"));
				assertEquals("4", values.v1.get("DayofMonth"));
				assertEquals("12", values.v1.get("MonthOfYear"));
				assertEquals("AQ", values.v2.get("Code"));
				assertEquals("AQ", values.v1.get("UniqueCarrier"));
			}
		}
		log.info("In testCachedStreamCachesJoin() method Exit");		
	}
	
	@Test
	public void testPigLoad() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List> results = (List<List>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for(List recordspart:results) {
			totalrecords += recordspart.size();
		}
		assertEquals(46361, totalrecords);
	}
	
	@Test
	public void testPigLoadForEach() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for(List<Map<String, Object>> recordspart:results) {
			totalrecords += recordspart.size();
			for(Map<String, Object> map: recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("MonthOfYear"));
			}
		}
		assertEquals(46361, totalrecords);
	}
	
	@Test
	public void testPigLoadForEachOrder() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data2 = ORDER data1 BY UniqueCarrier ASC, MonthOfYear ASC, DayofMonth ASC;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data2"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for(List<Map<String, Object>> recordspart:results) {
			totalrecords += recordspart.size();
			for(Map<String, Object> map: recordspart) {
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
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data2 = ORDER data1 BY UniqueCarrier ASC, MonthOfYear ASC, DayofMonth ASC;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("filtered_data = FILTER data2 BY MonthOfYear > 11 AND DayofMonth >= 6;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("filtered_data"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for(List<Map<String, Object>> recordspart:results) {
			totalrecords += recordspart.size();
			for(Map<String, Object> map: recordspart) {
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
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, DayofMonth, MonthOfYear;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data2 = ORDER data1 BY UniqueCarrier ASC, MonthOfYear ASC, DayofMonth ASC;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("filtered_data = FILTER data2 BY MonthOfYear > 11 AND DayofMonth >= 6;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("distinct_data = Distinct filtered_data;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("distinct_data"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		int totalrecords = 0;
		for(List<Map<String, Object>> recordspart:results) {
			totalrecords += recordspart.size();
			for(Map<String, Object> map: recordspart) {
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
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE COUNT(*) as cnt;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("cnt"));
				assertEquals(46361, map.get("cnt"));
			}
		}		
	}
	
	@Test
	public void testPigLoadCountWithGrpBy() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, COUNT(*) as cnt;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("cnt"));
				assertTrue(map.containsKey("UniqueCarrier"));
				if(((int)map.get("cnt"))==1) {
					assertEquals("UniqueCarrier", map.get("UniqueCarrier"));
				} else if(((int)map.get("cnt")) == 46360) {
					assertEquals("AQ", map.get("UniqueCarrier"));
				}
			}
		}		
	}
	
	@Test
	public void testPigLoadSum() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE SUM(ArrDelay) as sumarrdelay;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("sumarrdelay"));
				assertEquals(-63278,map.get("sumarrdelay"));
			}
		}		
	}
	
	@Test
	public void testPigLoadSumDelays() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE SUM(ArrDelay+DepDelay) as sumdelay;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("sumdelay"));
				assertEquals(-43110,map.get("sumdelay"));
			}
		}		
	}
	
	@Test
	public void testPigLoadSumWithGrpBy() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, SUM(ArrDelay) as sumarrdelay;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("sumarrdelay"));
				assertTrue(map.containsKey("UniqueCarrier"));
				if(((int)map.get("sumarrdelay"))==0) {
					assertEquals("UniqueCarrier", map.get("UniqueCarrier"));
				} else if(((int)map.get("sumarrdelay")) == -63278) {
					assertEquals("AQ", map.get("UniqueCarrier"));
				}
			}
		}		
	}
	
	@Test
	public void testPigLoadAvg() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE AVG(ArrDelay) as avgarrdelay;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("avgarrdelay"));
				assertEquals(-1.364897219645823,map.get("avgarrdelay"));
			}
		}		
	}
	
	@Test
	public void testPigLoadAvgWithGrpBy() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, AVG(ArrDelay) as avgarrdelay;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("avgarrdelay"));
				assertTrue(map.containsKey("UniqueCarrier"));
				if(((double)map.get("avgarrdelay"))==0) {
					assertEquals("UniqueCarrier", map.get("UniqueCarrier"));
				} else if(((double)map.get("avgarrdelay")) == -1.364897219645823) {
					assertEquals("AQ", map.get("UniqueCarrier"));
				}
			}
		}		
	}
	
	@Test
	public void testPigLoadAbs() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE ArrDelay, abs(ArrDelay) as absarrdelay;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("ArrDelay"));
				assertTrue(map.containsKey("absarrdelay"));
				assertEquals(map.get("absarrdelay"), Math.abs((int) map.get("ArrDelay")));
			}
		}		
	}
	
	@Test
	public void testPigLoadLength() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, length(UniqueCarrier) as lenuc;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("lenuc"));
				assertEquals(Long.valueOf(((String)map.get("UniqueCarrier")).length()), map.get("lenuc"));
			}
		}		
	}
	
	@Test
	public void testPigLoadNormalizespaces() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, normalizespaces('Test      Test     Test   Testd') as nsp;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("nsp"));
				assertEquals("Test Test Test Testd", map.get("nsp"));
			}
		}		
	}
	
	@Test
	public void testPigLoadNormalizespaces_2() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE normalizespaces('Test      Test     Test   Testd') as nsp;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(1, map.size());
				assertTrue(map.containsKey("nsp"));
				assertEquals("Test Test Test Testd", map.get("nsp"));
			}
		}		
	}
	
	
	@Test
	public void testPigLoadExpressionsAddition() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE MonthOfYear, DayofMonth, MonthOfYear+DayofMonth as summoydom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("summoydom"));
				assertEquals(((int)map.get("MonthOfYear"))+((int)map.get("DayofMonth")), map.get("summoydom"));
			}
		}		
	}
	
	@Test
	public void testPigLoadExpressionsSubtract() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE MonthOfYear, DayofMonth, MonthOfYear-DayofMonth as submoydom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("submoydom"));
				assertEquals(((int)map.get("MonthOfYear"))-((int)map.get("DayofMonth")), map.get("submoydom"));
			}
		}		
	}
	
	@Test
	public void testPigLoadExpressionsMultiply() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE MonthOfYear, DayofMonth, MonthOfYear*DayofMonth as mulmoydom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("mulmoydom"));
				assertEquals(((int)map.get("MonthOfYear"))*((int)map.get("DayofMonth")), map.get("mulmoydom"));
			}
		}		
	}
	
	@Test
	public void testPigLoadExpressionsDivide() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE AirlineYear, MonthOfYear, AirlineYear/MonthOfYear as divyearmon;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(3, map.size());
				assertTrue(map.containsKey("AirlineYear"));
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("divyearmon"));
				assertEquals(((int)map.get("AirlineYear"))/(Double.valueOf(map.get("MonthOfYear")+DataSamudayaConstants.EMPTY)), map.get("divyearmon"));
			}
		}		
	}
	
	@Test
	public void testPigLoadExpressionsMultipleOpertors() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE AirlineYear, MonthOfYear, DayofMonth, AirlineYear/MonthOfYear*DayofMonth as divmulyearmon;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(4, map.size());
				assertTrue(map.containsKey("AirlineYear"));
				assertTrue(map.containsKey("MonthOfYear"));
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("divmulyearmon"));
				assertEquals(((int)map.get("AirlineYear"))/(Double.valueOf(map.get("MonthOfYear")+DataSamudayaConstants.EMPTY)) * ((int)map.get("DayofMonth")), map.get("divmulyearmon"));
			}
		}		
	}
	
	@Test
	public void testPigLoadUppercase() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, uppercase(UniqueCarrier) as ucuc;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("ucuc"));
				assertEquals(((String)map.get("UniqueCarrier")).toUpperCase(), map.get("ucuc"));
			}
		}		
	}
	
	
	@Test
	public void testPigLoadLowercase() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, lowercase(UniqueCarrier) as lcuc;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("lcuc"));
				assertEquals(((String)map.get("UniqueCarrier")).toLowerCase(), map.get("lcuc"));
			}
		}		
	}
	
	
	
	@Test
	public void testPigLoadBase64encode() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, base64encode(UniqueCarrier) as base64uc;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("base64uc"));
				assertEquals(map.get("base64uc"), Base64.getEncoder().encodeToString(((String)map.get("UniqueCarrier")).getBytes()));
			}
		}		
	}
	
	@Test
	public void testPigLoadBase64decode() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE UniqueCarrier, base64encode(UniqueCarrier) as base64uc;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data2 = FOREACH data1 GENERATE UniqueCarrier, base64decode(base64uc) as base64decode;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);

		
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data2"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("UniqueCarrier"));
				assertTrue(map.containsKey("base64decode"));
				assertEquals(map.get("UniqueCarrier"), map.get("base64decode"));
			}
		}		
	}
	
	@Test
	public void testPigLoadPower() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, pow(DayofMonth, 2) as powdom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("powdom"));
				assertEquals(Math.pow(((int)map.get("DayofMonth")), 2),map.get("powdom"));
			}
		}		
	}
	
	
	@Test
	public void testPigLoadCeil() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, ceil(DayofMonth) as ceildom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("ceildom"));
				assertEquals(Math.ceil(((int)map.get("DayofMonth"))),map.get("ceildom"));
			}
		}		
	}
	
	@Test
	public void testPigLoadFloor() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, floor(DayofMonth) as floordom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("floordom"));
				assertEquals(Math.ceil(((int)map.get("DayofMonth"))),map.get("floordom"));
			}
		}		
	}
	
	@Test
	public void testPigLoadSqrt() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, sqrt(DayofMonth) as sqrtdom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("sqrtdom"));
				assertEquals(Math.sqrt(((int)map.get("DayofMonth"))),map.get("sqrtdom"));
			}
		}		
	}
	
	@Test
	public void testPigLoadExp() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, exp(DayofMonth) as expdom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("expdom"));
				assertEquals(Math.exp(((int)map.get("DayofMonth"))),map.get("expdom"));
			}
		}		
	}
	
	@Test
	public void testPigLoadLoge() throws Exception {
		String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.clear();
		pigQueriesToExecute.add("data = LOAD '/airlinesample' AS (AirlineYear: int ,MonthOfYear: int ,DayofMonth: int ,DayOfWeek: int ,DepTime: int ,CRSDepTime: int ,ArrTime: int ,CRSArrTime: int ,UniqueCarrier: chararray ,FlightNum: int ,TailNum: chararray ,ActualElapsedTime: int ,CRSElapsedTime: int ,AirTime: int ,ArrDelay: int ,DepDelay: int ,Origin: chararray ,Dest: chararray ,Distance: int ,TaxiIn: int ,TaxiOut: int ,Cancelled: int ,CancellationCode: chararray ,Diverted: int ,CarrierDelay: int ,WeatherDelay: int ,NASDelay: int ,SecurityDelay: int ,LateAircraftDelay: int);");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		pigQueriesToExecute.add("data1 = FOREACH data GENERATE DayofMonth, loge(DayofMonth) as logdom;");
		PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		
		jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
		List<List<Map<String, Object>>> results = (List<List<Map<String, Object>>>) PigUtils.executeCollect((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get("data1"), pipelineconfig.getUser(), jobid, tejobid, pipelineconfig);
		for(List<Map<String, Object>> recordspart:results) {
			for(Map<String, Object> map: recordspart) {
				assertEquals(2, map.size());
				assertTrue(map.containsKey("DayofMonth"));
				assertTrue(map.containsKey("logdom"));
				assertEquals(Math.log(((int)map.get("DayofMonth"))),map.get("logdom"));
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
