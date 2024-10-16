package com.github.datasamudaya.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.stream.pig.PigUtils;

public class PigCommandsLocalModeTest extends PigCommandsTest {

	String[] airlinehead = {"AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	String[] carrierhead = {"Code", "Description"};
	List<String> pigQueriesToExecute = new ArrayList<>();
	static String containeralloc;
	static boolean isuseglobaltaskexecutors;
	static String islocal;
	static String user;
	static Map<String, Object> pigAliasExecutedObjectMap = new ConcurrentHashMap<>();

	@BeforeClass
	public static void setUpConfig() throws Exception {
		containeralloc = pipelineconfig.getContaineralloc();
		isuseglobaltaskexecutors = pipelineconfig.getUseglobaltaskexecutors();
		islocal = pipelineconfig.getLocal();
		user = pipelineconfig.getUser();
		pipelineconfig.setIsremotescheduler(false);
		pipelineconfig.setUseglobaltaskexecutors(false);
		queryParserDriver = PigUtils.getQueryParserDriver("pig");
		pipelineconfig.setLocal("true");
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);		
	}

	@AfterClass
	public static void resetConfig() {
		pipelineconfig.setContaineralloc(containeralloc);
		pipelineconfig.setUseglobaltaskexecutors(isuseglobaltaskexecutors);
		pipelineconfig.setLocal(islocal);
		pipelineconfig.setUser(user);
	}

}
