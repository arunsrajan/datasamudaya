/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.stream.sql.examples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.stream.Pipeline;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;

public class SqlUniqueCarrierSumCountDepDelaySAInMemory implements Serializable, Pipeline {
	private static final long serialVersionUID = -7001849661976107123L;
	private Logger log = Logger.getLogger(SqlUniqueCarrierSumCountDepDelaySAInMemory.class);
	List<String> airlineheader = Arrays.asList("AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay");
	List<SqlTypeName> airsqltype = Arrays.asList(SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT, SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.VARCHAR,SqlTypeName.BIGINT,SqlTypeName.VARCHAR,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.VARCHAR,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT,SqlTypeName.BIGINT);
	List<String> carrierheader = Arrays.asList("Code", "Description");
	List<SqlTypeName> carriersqltype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setMaxmem(args[2]);
		pipelineconfig.setMinmem("512");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		testSql(args, pipelineconfig);
	}

	@SuppressWarnings({"unchecked"})
	public void testSql(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("SqlUniqueCarrierSumCountDepDelaySAInMemory.testSql Before---------------------------------------");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.DepDelay),count(*) "
				+ "FROM airline where airline.DepDelay<>'NA' and airline.DepDelay<>'DepDelay' group by airline.UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airsqltype)
				.setHdfs(args[0])
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		Long sum = 0l;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(DepDelay)");
			}
		}
		log.info("Sum = " + sum);
		log.info("SqlUniqueCarrierSumCountDepDelaySAInMemory.testSql After---------------------------------------");
	}
}
