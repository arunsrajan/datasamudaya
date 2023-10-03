package com.github.datasamudaya.tasks.scheduler;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplication;
import com.github.datasamudaya.tasks.scheduler.sql.MapReduceApplicationSqlBuilder;

public class MapReduceSqlBuilderTest extends MassiveDataMRJobBase {
	List<String> airlineheader = Arrays.asList("AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay");
	List<SqlTypeName> airlineheadertypes = Arrays.asList(SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE, SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.VARCHAR,SqlTypeName.DOUBLE,SqlTypeName.VARCHAR,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.VARCHAR,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE,SqlTypeName.DOUBLE);
	static Logger log = LoggerFactory.getLogger(MapReduceSqlBuilderTest.class);
	
	@Test
	public void testAllFunction() throws Exception {
		log.info("In testAllFunction() method Entry");
		String statement = "SELECT sum(airline.ArrDelay),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testAllFunction() method Exit");		
	}
	
	@Test
	public void testAllFunctionsWithCarrier() throws Exception {
		log.info("In testAllFunctionsWithCarrier() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay),count(*),max(airline.ArrDelay),min(airline.ArrDelay) FROM airline group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testAllFunctionsWithCarrier() method Exit");		
	}
	
	@Test
	public void testAllFunctionsWithCarrierWithWhere() throws Exception {
		log.info("In testAllFunctionsWithCarrierWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,sum(airline.ArrDelay) FROM airline where airline.MonthOfYear=12  group by airline.UniqueCarrier";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testAllFunctionsWithCarrierWithWhere() method Exit");		
	}
	
	@Test
	public void testAllColumns() throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testAllColumns() method Exit");		
	}
	
	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear FROM airline";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testRequiredColumns() method Exit");		
	}
	
	@Test
	public void testRequiredColumnsWithWhereInOrder() throws Exception {
		log.info("In testRequiredColumnsWithWhereInOrder() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear,airline.DayofMonth FROM airline where airline.MonthOfYear=12 and airline.DayofMonth=19 order by airline.MonthOfYear,airline.DayofMonth desc";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add("/airline1989", "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testRequiredColumnsWithWhereInOrder() method Exit");		
	}
	
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testAllColumnsWithWhere() method Exit");		
	}
	
	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear FROM airline where airline.MonthOfYear=12";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testRequiredColumnsWithWhere() method Exit");		
	}
	
	@Test
	public void testAllColumnsJoin() throws Exception {
		log.info("In testAllColumnsJoin() method Entry");
		String statement = "SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Description "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12= airline.MonthOfYear";				
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testAllColumnsJoin() method Exit");		
	}
	
	
	@Test
	public void testFunctionsJoin() throws Exception {
		log.info("In testFunctionsJoin() method Entry");
		String statement = "SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,sum(airline.ArrDelay) sumdelay "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE 8 = airline.DayofMonth and 12 = airline.MonthOfYear";				
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.add(carriers, "carriers", carrierheader, carrierheadertypes).setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testFunctionsJoin() method Exit");		
	}
	
	@Test
	public void testRequiredColumnsInOrder() throws Exception {
		log.info("In testRequiredColumnsInOrder() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.MonthOfYear,airline.DayofMonth FROM airline order by airline.MonthOfYear,airline.DayofMonth desc";
		MapReduceApplication mra = (MapReduceApplication) MapReduceApplicationSqlBuilder.newBuilder().add(airlinesample, "airline", airlineheader, airlineheadertypes)
				.setHdfs(hdfsfilepath)
				.setSql(statement).build();
		List<Context> records = (List) mra.call();
		records.stream().forEach(context->{
			context.keys().stream().forEach(key->{
				log.info("{} {}", key, context.get(key));
			});
		});
		log.info("In testRequiredColumnsInOrder() method Exit");		
	}
}
