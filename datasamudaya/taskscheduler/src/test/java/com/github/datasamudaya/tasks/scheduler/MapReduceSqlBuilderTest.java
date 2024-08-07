package com.github.datasamudaya.tasks.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.tasks.scheduler.sql.MapReduceApplicationSqlBuilder;

@SuppressWarnings({"unchecked", "rawtypes"})
public class MapReduceSqlBuilderTest extends MassiveDataMRJobBase {
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
	static Logger log = LoggerFactory.getLogger(MapReduceSqlBuilderTest.class);

	@Test
	public void testAllFunction() throws Exception {
		log.info("In testAllFunction() method Entry");
		String statement = "SELECT sum(airline.ArrDelay),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(4, values.length);
				}
			});
		});
		log.info("In testAllFunction() method Exit");
	}

	@Test
	public void testAllFunctionsWithCarrier() throws Exception {
		log.info("In testAllFunctionsWithCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(5, values.length);
				}
			});
		});
		log.info("In testAllFunctionsWithCarrier() method Exit");
	}

	@Test
	public void testAllFunctionsWithCarrierAbsValue() throws Exception {
		log.info("In testAllFunctionsWithCarrierAbsValue() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(abs(airline.ArrDelay)),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(5, values.length);					
				}
			});
		});
		log.info("In testAllFunctionsWithCarrierAbsValue() method Exit");
	}

	@Test
	public void testAllFunctionsWithCarrierParameterAdd() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterAdd() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay + 2.0),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(5, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("sum(airline.ArrDelay + 2.0)"));
					assertTrue(values.containsKey("max(airline.ArrDelay)"));
					assertTrue(values.containsKey("min(airline.ArrDelay)"));
					assertTrue(values.containsKey("count()"));
				}
			});
		});
		log.info("In testAllFunctionsWithCarrierParameterAdd() method Exit");
	}

	@Test
	public void testAllFunctionsWithCarrierParameterSub() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterSub() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay - 2.0),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(5, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("sum(airline.ArrDelay - 2.0)"));
					assertTrue(values.containsKey("max(airline.ArrDelay)"));
					assertTrue(values.containsKey("min(airline.ArrDelay)"));
					assertTrue(values.containsKey("count()"));
				}
			});
		});
		log.info("In testAllFunctionsWithCarrierParameterSub() method Exit");
	}


	@Test
	public void testAllFunctionsWithCarrierParameterMul() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterMul() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay * 2.0),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(5, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("sum(airline.ArrDelay * 2.0)"));
					assertTrue(values.containsKey("max(airline.ArrDelay)"));
					assertTrue(values.containsKey("min(airline.ArrDelay)"));
					assertTrue(values.containsKey("count()"));
				}
			});
		});
		log.info("In testAllFunctionsWithCarrierParameterMul() method Exit");
	}

	@Test
	public void testAllFunctionsWithCarrierParameterDiv() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterDiv() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay / 2.0) as sdiv,count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(5, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("sdiv"));
					assertTrue(values.containsKey("max(airline.ArrDelay)"));
					assertTrue(values.containsKey("min(airline.ArrDelay)"));
					assertTrue(values.containsKey("count()"));
				}
			});
		});
		log.info("In testAllFunctionsWithCarrierParameterDiv() method Exit");
	}


	@Test
	public void testAllFunctionsWithCarrierParameterDelay() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterDelay() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(abs(airline.ArrDelay) + abs(airline.DepDelay)) as sdelay,count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(5, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("sdelay"));
					assertTrue(values.containsKey("max(airline.ArrDelay)"));
					assertTrue(values.containsKey("min(airline.ArrDelay)"));
					assertTrue(values.containsKey("count()"));
				}
			});
		});
		log.info("In testAllFunctionsWithCarrierParameterDelay() method Exit");
	}


	@Test
	public void testAllFunctionsWithCarrierWithWhere() throws Exception {
		log.info("In testAllFunctionsWithCarrierWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay) FROM airline where airline.MonthOfYear=12  group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(2, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("sum(airline.ArrDelay)"));
				}
			});
		});
		log.info("In testAllFunctionsWithCarrierWithWhere() method Exit");
	}

	@Test
	public void testAllColumns() throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(29l, values.length);
				}
			});
		});
		log.info("In testAllColumns() method Exit");
	}

	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(2, values.length);
				}
			});
		});
		log.info("In testRequiredColumns() method Exit");
	}
	
	@Test
	public void testNonAggFunctionRound() throws Exception {
		log.info("In testNonAggFunctionRound() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,round(airline.ArrDelay + 3.14) FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(3, values.length);					
				}
			});
		});
		log.info("In testNonAggFunctionRound() method Exit");
	}
	
	@Test
	public void testColumnLength() throws Exception {
		log.info("In testColumnLength() method Entry");

		String statement = "SELECT length(airline.Origin)  FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(1, values.length);
			}
			});
		});
		log.info("In testColumnLength() method Exit");
	}

	
	@Test
	public void testRequiredColumnWithLength() throws Exception {
		log.info("In testRequiredColumnWithLength() method Entry");

		String statement = "SELECT airline.Origin,length(airline.Origin)  FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(2, values.length);
			}
			});
		});
		log.info("In testRequiredColumnWithLength() method Exit");
	}

	
	@Test
	public void testRequiredColumnWithMultipleLengths() throws Exception {
		log.info("In testRequiredColumnWithMultipleLengths() method Entry");

		String statement = "SELECT airline.Origin,length(airline.Origin),length(airline.Dest)  FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(3, values.length);
			}
			});
		});
		log.info("In testRequiredColumnWithMultipleLengths() method Exit");
	}

	
	@Test
	public void testRequiredColumnWithLengthsAndLowercase() throws Exception {
		log.info("In testRequiredColumnWithLengthsAndLowercase() method Entry");

		String statement = "SELECT lowercase(airline.Origin),lowercase(airline.Dest),length(airline.Origin),length(airline.Dest)  FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(4, values.length);
			}
			});
		});
		log.info("In testRequiredColumnWithLengthsAndLowercase() method Exit");
	}

	@Test
	public void testRequiredColumnWithLengthsAndUppercase() throws Exception {
		log.info("In testRequiredColumnWithLengthsAndUppercase() method Entry");

		String statement = "SELECT uppercase(airline.Origin),uppercase(airline.Dest),length(airline.Origin),length(airline.Dest)  FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(4, values.length);
			}
			});
		});
		log.info("In testRequiredColumnWithLengthsAndUppercase() method Exit");
	}

	@Test
	public void testRequiredColumnTrim() throws Exception {
		log.info("In testRequiredColumnTrim() method Entry");

		String statement = "SELECT trimstr(concat(airline.Origin , ' ')) trmorig ,trimstr(concat(' ' , airline.Dest)) trimdest FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(2, values.length);
			}
			});
		});
		log.info("In testRequiredColumnTrim() method Exit");
	}

	@Test
	public void testRequiredColumnBase64Encode() throws Exception {
		log.info("In testRequiredColumnBase64Encode() method Entry");

		String statement = "SELECT base64encode(airline.Origin),base64encode(airline.Dest)  FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(2, values.length);
			}
			});
		});
		log.info("In testRequiredColumnBase64Encode() method Exit");
	}

	@Test
	public void testRequiredColumnSubStringAlias() throws Exception {
		log.info("In testRequiredColumnSubStringAlias() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin from 0 for 1) as substr  FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(2, values.length);
			}
			});
		});
		log.info("In testRequiredColumnSubStringAlias() method Exit");
	}

	@Test
	public void testRequiredColumnSubStringPos() throws Exception {
		log.info("In testRequiredColumnSubStringPos() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin from 1),airline.Dest,substring(airline.Dest from 2) FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(4, values.length);
			}
			});
		});
		log.info("In testRequiredColumnSubStringPos() method Exit");
	}

	
	@Test
	public void testRequiredColumnSubStringPosLength() throws Exception {
		log.info("In testRequiredColumnSubString() method Entry");

		String statement = "SELECT airline.Origin,substring(airline.Origin from 0 for 1),airline.Dest,substring(airline.Dest from 0 for 2) FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(4, values.length);
			}
			});
		});
		log.info("In testRequiredColumnSubString() method Exit");
	}

	@Test
	public void testRequiredColumnNormailizeSpaces() throws Exception {
		log.info("In testRequiredColumnNormailizeSpaces() method Entry");

		String statement = "SELECT normalizespaces(airline.Dest),normalizespaces(' This is   good  work') eg FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder()
				.add(airlinesample, "airline", airlineheader, airlineheadertypes).setHdfs(hdfsfilepath)
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
				log.info("{}",Arrays.toString(values));
				assertEquals(2, values.length);
			}
			});
		});
		log.info("In testRequiredColumnNormailizeSpaces() method Exit");
	}

	@Test
	public void testRequiredColumnsWithWhereInOrder() throws Exception {
		log.info("In testRequiredColumnsWithWhereInOrder() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear,airline.DayofMonth FROM airline where airline.MonthOfYear=12 and airline.DayofMonth=19 order by airline.MonthOfYear,airline.DayofMonth desc";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("DayofMonth"));
					assertEquals(12.0, values.get("MonthOfYear"));
					assertEquals(19.0, values.get("DayofMonth"));
				}
			});
		});
		log.info("In testRequiredColumnsWithWhereInOrder() method Exit");
	}

	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(29, values.size());
					assertEquals(12.0, values.get("MonthOfYear"));
				}
			});
		});
		log.info("In testAllColumnsWithWhere() method Exit");
	}

	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(2, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("MonthOfYear"));
					assertEquals(12.0, values.get("MonthOfYear"));
				}
			});
		});
		log.info("In testRequiredColumnsWithWhere() method Exit");
	}

	@Test
	public void testAllColumnsJoin() throws Exception {
		log.info("In testAllColumnsJoin() method Entry");
		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Description \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear\
				""";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(4, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("DayofMonth"));
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("Description"));
					assertEquals(12.0, values.get("MonthOfYear"));
					assertEquals(8.0, values.get("DayofMonth"));
				}
			});
		});
		log.info("In testAllColumnsJoin() method Exit");
	}


	@Test
	public void testFunctionsJoin() throws Exception {
		log.info("In testFunctionsJoin() method Entry");
		String statement = """
				SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,sum(airline.ArrDelay) sumdelay \
				FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12 = airline.MonthOfYear\
				""";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(4, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("DayofMonth"));
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("sumdelay"));
					assertEquals(12.0, values.get("MonthOfYear"));
					assertEquals(8.0, values.get("DayofMonth"));
				}
			});
		});
		log.info("In testFunctionsJoin() method Exit");
	}

	@Test
	public void testRequiredColumnsInOrder() throws Exception {
		log.info("In testRequiredColumnsInOrder() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear,airline.DayofMonth FROM airline order by airline.MonthOfYear,airline.DayofMonth desc";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("DayofMonth"));
					assertTrue(values.containsKey("MonthOfYear"));
				}
			});
		});
		log.info("In testRequiredColumnsInOrder() method Exit");
	}


	@Test
	public void testNonAggFunctionAbsWithWhere() throws Exception {
		log.info("In testNonAggFunctionAbsWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,abs(airline.ArrDelay) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(2, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("abs(airline.ArrDelay)"));
					assertTrue(((Double) values.get("abs(airline.ArrDelay)")) >= 0);
				}
			});
		});
		log.info("In testNonAggFunctionAbsWithWhere() method Exit");
	}


	@Test
	public void testNonAggFunctionAbsAddWithWhere() throws Exception {
		log.info("In testNonAggFunctionAbsAddWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,abs(airline.ArrDelay + 10) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(2, values.size());
					assertTrue(values.containsKey("UniqueCarrier"));
					assertTrue(values.containsKey("abs(airline.ArrDelay + 10)"));
					assertTrue(((Double) values.get("abs(airline.ArrDelay + 10)")) >= 0);
				}
			});
		});
		log.info("In testNonAggFunctionAbsAddWithWhere() method Exit");
	}


	@Test
	public void testNonAggFunctionLengthWithWhere() throws Exception {
		log.info("In testNonAggFunctionLengthWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,length(airline.UniqueCarrier) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(2, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("length(airline.UniqueCarrier)"));
					assertTrue(((Long) values.get("length(airline.UniqueCarrier)")) > 0);
				}
			});
		});
		log.info("In testNonAggFunctionLengthWithWhere() method Exit");
	}


	@Test
	public void testNonAggFunctionRoundWithWhere() throws Exception {
		log.info("In testNonAggFunctionRoundWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,round(airline.ArrDelay + 3.14) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("round(airline.ArrDelay + 3.14)"));
				}
			});
		});
		log.info("In testNonAggFunctionRoundWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionCeilWithWhere() throws Exception {
		log.info("In testNonAggFunctionCeilWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,ceil(airline.ArrDelay + 3.14) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("ceil(airline.ArrDelay + 3.14)"));
				}
			});
		});
		log.info("In testNonAggFunctionCeilWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionFloorWithWhere() throws Exception {
		log.info("In testNonAggFunctionFloorWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,floor(airline.ArrDelay + 3.14) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("floor(airline.ArrDelay + 3.14)"));
				}
			});
		});
		log.info("In testNonAggFunctionFloorWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionPowWithWhere() throws Exception {
		log.info("In testNonAggFunctionPowWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,pow(airline.ArrDelay + 3.14, 0.5) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("pow(airline.ArrDelay + 3.14, 0.5)"));
				}
			});
		});
		log.info("In testNonAggFunctionPowWithWhere() method Exit");
	}


	@Test
	public void testNonAggFunctionSqrtWithWhere() throws Exception {
		log.info("In testNonAggFunctionSqrtWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,sqrt(abs(airline.ArrDelay + 3.14)) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("sqrt(abs(airline.ArrDelay + 3.14))"));
				}
			});
		});
		log.info("In testNonAggFunctionSqrtWithWhere() method Exit");
	}


	@Test
	public void testNonAggFunctionExpWithWhere() throws Exception {
		log.info("In testNonAggFunctionExpWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,exp(abs(airline.ArrDelay)) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("exp(abs(airline.ArrDelay))"));
				}
			});
		});
		log.info("In testNonAggFunctionExpWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionLogeWithWhere() throws Exception {
		log.info("In testNonAggFunctionLogeWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,loge(abs(airline.ArrDelay)) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("loge(abs(airline.ArrDelay))"));
				}
			});
		});
		log.info("In testNonAggFunctionLogeWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionLowercaseWithWhere() throws Exception {
		log.info("In testNonAggFunctionLowercaseWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,lowercase(airline.UniqueCarrier) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("lowercase(airline.UniqueCarrier)"));
					assertEquals("aq", values.get("lowercase(airline.UniqueCarrier)"));
				}
			});
		});
		log.info("In testNonAggFunctionLowercaseWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionUppercaseWithWhere() throws Exception {
		log.info("In testNonAggFunctionUppercaseWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,uppercase(lowercase(airline.UniqueCarrier)) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("uppercase(lowercase(airline.UniqueCarrier))"));
					assertEquals("AQ", values.get("uppercase(lowercase(airline.UniqueCarrier))"));
				}
			});
		});
		log.info("In testNonAggFunctionUppercaseWithWhere() method Exit");
	}


	@Test
	public void testNonAggFunctionBase64encodeWithWhere() throws Exception {
		log.info("In testNonAggFunctionBase64encodeWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,base64encode(airline.UniqueCarrier) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("base64encode(airline.UniqueCarrier)"));
				}
			});
		});
		log.info("In testNonAggFunctionBase64encodeWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionBase64decodeWithWhere() throws Exception {
		log.info("In testNonAggFunctionBase64decodeWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,base64decode(base64encode(airline.UniqueCarrier)) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("base64decode(base64encode(airline.UniqueCarrier))"));
					assertEquals("AQ", values.get("base64decode(base64encode(airline.UniqueCarrier))"));
				}
			});
		});
		log.info("In testNonAggFunctionBase64decodeWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionNormalizespaceWithWhere() throws Exception {
		log.info("In testNonAggFunctionNormalizespaceWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,normalizespaces('   Spaces    Nomalizer   ' + base64encode(airline.UniqueCarrier) + '     ') FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("normalizespaces('   Spaces    Nomalizer   ' + base64encode(airline.UniqueCarrier) + '     ')"));
				}
			});
		});
		log.info("In testNonAggFunctionNormalizespaceWithWhere() method Exit");
	}

	@Test
	public void testNonAggFunctionTrimWithWhere() throws Exception {
		log.info("In testNonAggFunctionTrimWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,trim('   Spaces    Nomalizer   ' + base64encode(airline.UniqueCarrier) + '     ') FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("trim('   Spaces    Nomalizer   ' + base64encode(airline.UniqueCarrier) + '     ')"));
				}
			});
		});
		log.info("In testNonAggFunctionTrimWithWhere() method Exit");
	}


	@Test
	public void testNonAggFunctionSubstringWithWhere() throws Exception {
		log.info("In testNonAggFunctionSubstringWithWhere() method Entry");
		String statement = "SELECT airline.MonthOfYear,airline.ArrDelay,substring('Spaces Nomalizer ' + base64encode(airline.UniqueCarrier), 2, 10) FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath).setJobConfiguration(jc)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Map<String, Object>> rec = (List<Map<String, Object>>) context.get(key);
				for (Map<String, Object> values : rec) {
					log.info("{}", values);
					assertEquals(3, values.size());
					assertTrue(values.containsKey("MonthOfYear"));
					assertTrue(values.containsKey("ArrDelay"));
					assertTrue(values.containsKey("substring('Spaces Nomalizer ' + base64encode(airline.UniqueCarrier), 2, 10)"));
				}
			});
		});
		log.info("In testNonAggFunctionSubstringWithWhere() method Exit");
	}


}
