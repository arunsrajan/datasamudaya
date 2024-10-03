package com.github.datasamudaya.tasks.scheduler;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

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
		log.info("In testAllFunctionsWithCarrierParameterAdd() method Exit");
	}

	@Test
	public void testAllFunctionsWithCarrierParameterSub() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterSub() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay - 2.0),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
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
		log.info("In testAllFunctionsWithCarrierParameterSub() method Exit");
	}


	@Test
	public void testAllFunctionsWithCarrierParameterMul() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterMul() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay * 2.0),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
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
		log.info("In testAllFunctionsWithCarrierParameterMul() method Exit");
	}

	@Test
	public void testAllFunctionsWithCarrierParameterDiv() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterDiv() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay / 2.0) as sdiv,count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
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
		log.info("In testAllFunctionsWithCarrierParameterDiv() method Exit");
	}


	@Test
	public void testAllFunctionsWithCarrierParameterDelay() throws Exception {
		log.info("In testAllFunctionsWithCarrierParameterDelay() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(abs(airline.ArrDelay) + abs(airline.DepDelay)) as sdelay,count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
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
		log.info("In testAllFunctionsWithCarrierParameterDelay() method Exit");
	}


	@Test
	public void testAllFunctionsWithCarrierWithWhere() throws Exception {
		log.info("In testAllFunctionsWithCarrierWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay) FROM airline where airline.MonthOfYear=12  group by airline.UniqueCarrier";
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
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear,airline.DayofMonth FROM airline where airline.MonthOfYear=12 and airline.DayofMonth>25 order by airline.MonthOfYear,airline.DayofMonth asc";
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
					assertEquals(12l, values[1]);
					assert((long)values[2]>25);
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
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(29, values.length);
					assertEquals(12l, values[1]);
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
				.setDb(DataSamudayaConstants.SQLMETASTORE_DB)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context -> {
			context.keys().stream().forEach(key -> {
				List<Object[]> rec = (List<Object[]>) context.get(key);
				for (Object[] values : rec) {
					log.info("{}", Arrays.toString(values));
					assertEquals(2, values.length);
					assertEquals(12l, values[1]);
				}
			});
		});
		log.info("In testRequiredColumnsWithWhere() method Exit");
	}

	@Test
	public void testRequiredColumnsInOrder() throws Exception {
		log.info("In testRequiredColumnsInOrder() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear,airline.DayofMonth FROM airline order by airline.MonthOfYear,airline.DayofMonth desc";
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
		log.info("In testRequiredColumnsInOrder() method Exit");
	}


}
