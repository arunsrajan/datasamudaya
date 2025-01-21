package com.github.datasamudaya.stream.sql.dataframe;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.Pipeline;
import com.github.datasamudaya.stream.sql.dataframe.build.AggregateFunctionBuilder;
import com.github.datasamudaya.stream.sql.dataframe.build.Column;
import com.github.datasamudaya.stream.sql.dataframe.build.DataFrame;
import com.github.datasamudaya.stream.sql.dataframe.build.DataFrameContext;
import com.github.datasamudaya.stream.sql.dataframe.build.FunctionBuilder;

public class DataFrameAggregateAbs implements Pipeline{
	private static final long serialVersionUID = -7389037644589597711L;
	private final Logger log = LogManager.getLogger(DataFrameAggregateAbs.class);
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
	@Override
	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY);

		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem(args[2]);
		pipelineconfig.setMinmem("536870912");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
		DataFrame df = DataFrameContext.newDataFrameContext(pipelineconfig)
				.addTable(args[0], airlineheader.toArray(new String[0]), "airlines", airlineheadertypes)
				.setDb("db").setFileFormat("csv").setHdfs(args[1]).build();
		df.scantable("airlines");
		df.select("UniqueCarrier", "ArrDelay", "DepDelay");
		FunctionBuilder nonaggfuncbuilder = FunctionBuilder.builder().addField(null, new String[] { "UniqueCarrier" })
				.addFunction("abs", "absarrdelay", new Object[] { new Column("ArrDelay") })
				.addFunction("abs", "absdepdelay", new Object[] { new Column("DepDelay") });
		df.selectWithFunc(nonaggfuncbuilder)
				.aggregate(AggregateFunctionBuilder.builder()
						.sum("sumarrdelay", new Object[] { new Column("absarrdelay") })
						.sum("sumdepdelay", new Object[] { new Column("absdepdelay") }), "UniqueCarrier");
		List<List<Object[]>> output = (List<List<Object[]>>) df.execute();
		for (List<Object[]> valuel : output) {
			for (Object[] values : valuel) {
				log.info(Arrays.toString(values));
			}
		}
	}
}
