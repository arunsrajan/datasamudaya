package com.github.datasamudaya.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pig.parser.QueryParserDriver;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.pig.PigUtils;

public class PigCommandsTest extends PigCommandsLocalModeTest {

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
		if ("false".equals(pipelineconfig.getLocal())) {
			pipelineconfig.setIsremotescheduler(true);
			Utils.launchContainersExecutorSpecWithDriverSpec("arun", tejobid, 3, 3000, 1, 3, 3000, true);
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
