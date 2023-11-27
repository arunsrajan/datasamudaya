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
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.Pipeline;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;

public class SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal implements Serializable, Pipeline {
	private static final long serialVersionUID = -7001849661976107123L;
	private final Logger log = Logger.getLogger(SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal.class);
	List<String> airlineheader = Arrays.asList("AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay");
	List<SqlTypeName> airsqltype = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT);
	List<String> carrierheader = Arrays.asList("Code", "Description");
	List<SqlTypeName> carriersqltype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setLocal("true");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setBlocksize(args[2]);
		pipelineconfig.setBatchsize(args[3]);
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		testSql(args, pipelineconfig);
	}

	public void testSql(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal.testSql Before---------------------------------------");
		String statement = """
				SELECT airline.AirlineYear,airline.MonthOfYear,airline.DayofMonth,airline.UniqueCarrier,sum(airline.ArrDelay),count(*) \
				FROM airline where airline.ArrDelay<>'NA' and airline.ArrDelay<>'ArrDelay' group by airline.AirlineYear,airline.MonthOfYear,airline.DayofMonth,airline.UniqueCarrier\
				""";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(args[1], "airline", airlineheader, airsqltype)
				.setHdfs(args[0])
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		mdpsql.saveAsTextFile(new URI(args[0]), args[4] + "/SqlCountSum-" + System.currentTimeMillis());
		log.info("SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal.testSql After---------------------------------------");
	}
}
