package com.github.datasamudaya.stream.sql;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.stream.utils.Optimizer;
import com.github.datasamudaya.stream.utils.SimpleSchema;
import com.github.datasamudaya.stream.utils.SimpleTable;

public class RequiredColumnsExtractorTest {
	private static Logger log = LoggerFactory.getLogger(RequiredColumnsExtractorTest.class);
	List<String> airlineheader = Arrays.asList("AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
			"SecurityDelay", "LateAircraftDelay");
	List<SqlTypeName> airlineheadertypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT);
	List<String> carrierheader = Arrays.asList("Code", "Description");
	List<SqlTypeName> carrierheadertypes = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	List<String> airportsheader = Arrays.asList("iata", "airport", "city", "state", "country", "latitude",
			"longitude");
	List<SqlTypeName> airportstype = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	
	@Test
	public void testAllColumns() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("SELECT * FROM airline"));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));
		assertEquals(29, tablereqcolumnsmap.get("airline").size());
	}
	
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("SELECT * FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12"));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));
		assertEquals(29, tablereqcolumnsmap.get("airline").size());
	}
	
	@Test
	public void testRequiredColumns() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline"));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(3, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("15"));
	}
	
	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline WHERE airline.DayofMonth=8 and airline.MonthOfYear=12"));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(5, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("15"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	
	@Test
	public void testRequiredColumnsWithWhereGreaterThan() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear
						FROM airline
						WHERE airline.DayofMonth>8 and airline.MonthOfYear>6
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(4, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	@Test
	public void testRequiredColumnsWithWhereLessThan() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear
						FROM airline
						WHERE airline.DayofMonth<8 and airline.MonthOfYear<6
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(4, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	@Test
	public void testRequiredColumnsWithWhereGreaterThanEquals() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear
						FROM airline
						WHERE airline.DayofMonth>=8 and airline.MonthOfYear>=6
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(4, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	@Test
	public void testRequiredColumnsWithWhereLessThanEquals() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear
						FROM airline
						WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(4, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	@Test
	public void testRequiredColumnsWithWhereLessThanEqualsAndCase() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,
						case when airline.DayofMonth < 6 then 'day of month is under 6' else 'day of month is over 6' end,
						case when airline.MonthOfYear < 6 then 'month of year is under 6' else 'month of year is over 6' end
						FROM airline
						WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(4, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	@Test
	public void testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhen() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear,
						case when airline.DayofMonth < 6 then 'day of month is under 6' else 'day of month is over 6' end,
						case when airline.MonthOfYear < 6 then 'month of year is under 6' else 'month of year is over 6' end
						FROM airline
						WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(4, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	@Test
	public void testRequiredColumnsWithWhereLessThanEqualsAndCaseMultipleWhenExpression() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear, \
						case when airline.DayofMonth < 6 then airline.DayofMonth+1 when airline.DayofMonth = 6 then airline.DayofMonth - 2 else airline.DayofMonth + 3 end, \
						case when airline.MonthOfYear < 6 then 'month of year is under 6' when airline.MonthOfYear = 6 then 'month of year equals 6' else 'month of year is over 6' end \
						FROM airline \
						WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6\
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(4, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("8"));
		assertEquals(Boolean.TRUE, columns.contains("14"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		assertEquals(Boolean.TRUE, columns.contains("2"));
	}
	
	@Test
	public void testRequiredColumnsUnion() throws Exception {
		Map<String, Set<String>> tablereqcolumnsmap = new RequiredColumnsExtractor()
				.getRequiredColumnsByTable(buildSqlRelNode("""
						select sum(abs(length(concat(Origin , Dest)))) FROM airline where MonthOfYear between 11 and 12 and Origin like 'HNL' and  Dest like 'OGG' union select sum(abs(length(concat(Origin , Dest)))) FROM airlinesample where MonthOfYear between 11 and 12 and Origin like 'HNL' and  Dest like 'OGG'
						"""));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airline"));
		assertEquals(Boolean.TRUE, tablereqcolumnsmap.containsKey("airlinesample"));		
		Set<String> columns = tablereqcolumnsmap.get("airline");
		assertEquals(3, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("16"));
		assertEquals(Boolean.TRUE, columns.contains("17"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
		columns = tablereqcolumnsmap.get("airlinesample");
		assertEquals(3, columns.size());
		assertEquals(Boolean.TRUE, columns.contains("16"));
		assertEquals(Boolean.TRUE, columns.contains("17"));
		assertEquals(Boolean.TRUE, columns.contains("1"));
	}
	
	private static SimpleTable getSimpleTable(String tablename, String[] fields, SqlTypeName[] types) {
		SimpleTable.Builder builder = SimpleTable.newBuilder(tablename);
		int typecount = 0;
		for (String field : fields) {
			builder = builder.addField(field, types[typecount]);
			typecount++;
		}
		return builder.withRowCount(60000L).build();
	}
	private RelNode buildSqlRelNode(String sql) throws Exception {
		SimpleSchema.Builder builder = SimpleSchema.newBuilder("airschema");
		builder.addTable(getSimpleTable("airline", airlineheader.toArray(new String[0]),
				airlineheadertypes.toArray(new SqlTypeName[0])));
		builder.addTable(getSimpleTable("airlinesample", airlineheader.toArray(new String[0]),
				airlineheadertypes.toArray(new SqlTypeName[0])));
		builder.addTable(getSimpleTable("carriers", carrierheader.toArray(new String[0]),
				carrierheadertypes.toArray(new SqlTypeName[0])));
		builder.addTable(getSimpleTable("airports", airportsheader.toArray(new String[0]),
				airportstype.toArray(new SqlTypeName[0])));
		SimpleSchema schema = builder.build();
		Optimizer optimizer = Optimizer.create(schema);
		SqlNode sqlTree = optimizer.parse(sql);
		log.info("{}",sqlTree);	
		RelNode relTree = optimizer.convert(sqlTree);		
		RuleSet rules = RuleSets.ofList(
				CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.FILTER_MERGE,
				CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE,
				CoreRules.PROJECT_FILTER_VALUES_MERGE, EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_FILTER_RULE,
				EnumerableRules.ENUMERABLE_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_SORT_RULE,
				EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
				EnumerableRules.ENUMERABLE_UNION_RULE, EnumerableRules.ENUMERABLE_INTERSECT_RULE
				);

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		traverseRelNode(optimizerRelTree, 0);
		
		return optimizerRelTree;
	}
	private void traverseRelNode(RelNode relNode, int depth) {
		// Print information about the current node
		printNodeInfo(relNode, depth);

		// Traverse child nodes
		List<RelNode> childNodes = relNode.getInputs();
		for (RelNode child : childNodes) {
			traverseRelNode(child, depth + 1);
		}
	}

	private void printNodeInfo(RelNode relNode, int depth) {
		// Print information about the current node
		log.info("{}", getIndent(depth) + "Node ID: " + relNode.getId());
		log.info("{}", getIndent(depth) + "Node Description: " + relNode.toString());
		// Add more information as needed based on your requirements
	}

	private String getIndent(int depth) {
		// Create an indentation string based on the depth
		StringBuilder indent = new StringBuilder();
		for (int i = 0; i < depth; i++) {
			indent.append("  ");
		}
		return indent.toString();
	}
}
