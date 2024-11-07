package com.github.datasamudaya.common;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.utils.sql.Optimizer;
import com.github.datasamudaya.common.utils.sql.RexNodeComparator;
import com.github.datasamudaya.common.utils.sql.SimpleSchema;
import com.github.datasamudaya.common.utils.sql.SimpleTable;

public class RexNodeComparatorTest {
	static List<String> airlineheader = Arrays.asList("AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
			"SecurityDelay", "LateAircraftDelay");
	static List<SqlTypeName> airlineheadertypes = Arrays.asList(SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.VARCHAR,
			SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT, SqlTypeName.BIGINT,
			SqlTypeName.BIGINT);
	RuleSet rules = RuleSets.ofList(
			CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.FILTER_MERGE,
			CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE,
			CoreRules.AGGREGATE_PROJECT_MERGE,
			CoreRules.AGGREGATE_JOIN_TRANSPOSE,
			CoreRules.AGGREGATE_PROJECT_MERGE,
			CoreRules.PROJECT_AGGREGATE_MERGE,
			CoreRules.PROJECT_MERGE,
			CoreRules.FILTER_INTO_JOIN,
			CoreRules.FILTER_PROJECT_TRANSPOSE,
			EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
			EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_FILTER_RULE,
			EnumerableRules.ENUMERABLE_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_SORT_RULE,
			EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
			EnumerableRules.ENUMERABLE_UNION_RULE, EnumerableRules.ENUMERABLE_INTERSECT_RULE
			);
	static Optimizer optimizer = null;
	private static SimpleTable getSimpleTable(String tablename, String[] fields, SqlTypeName[] types) {
		SimpleTable.Builder builder = SimpleTable.newBuilder(tablename);
		int typecount = 0;
		for (String field : fields) {
			builder = builder.addField(field, types[typecount]);
			typecount++;
		}
		return builder.withRowCount(60000L).build();
	}
	
	@BeforeClass
	public static void init() {
		SimpleSchema.Builder builder = SimpleSchema.newBuilder("airschema");
		builder.addTable(getSimpleTable("airline", airlineheader.toArray(new String[0]),
				airlineheadertypes.toArray(new SqlTypeName[0])));
		SimpleSchema schema = builder.build();
		optimizer = Optimizer.create(schema);
	}
	
	@Test
	public void testWhereConditionSameOrder() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.uniquecarrier = 'AA'
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				airline.uniquecarrier = 'AA'
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	
	@Test
	public void testWhereConditionDiffOrder() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.uniquecarrier = 'AA'
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				'AA' = airline.uniquecarrier
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionSameOrderLiteralComparison() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.uniquecarrier = 'AA'
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				airline.uniquecarrier = 'UA'
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(false, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionDiffOrderLiteralComparison() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.uniquecarrier = 'AA'
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				'UA' = airline.uniquecarrier
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(false, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionSameOrderGreaterThan() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear > 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				airline.monthofyear > 2
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	@Test
	public void testWhereConditionDiffOrderGreaterThanLessThanEquals() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear > 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				2 <= airline.monthofyear
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionSameOrderLessThan() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear < 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				airline.monthofyear < 2
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	@Test
	public void testWhereConditionDiffOrderLessThanGreaterThanEquals() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear < 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				2 >= airline.monthofyear
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionSameOrderLessThanEquals() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear <= 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				airline.monthofyear <= 2
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionDiffOrderLessThanEqualsGreaterThan() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear <= 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				2 > airline.monthofyear
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	
	@Test
	public void testWhereConditionSameOrderGreaterThanEquals() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear >= 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				airline.monthofyear >= 2
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionDiffOrderGreaterThanEqualsLessThan() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear >= 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay),sum(arrdelay+depdelay) from airline where 
				2 < airline.monthofyear
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	
	@Test
	public void testWhereConditionSameOrderAnd() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear = 2 and airline.dayofmonth = 4
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear = 2 and airline.dayofmonth = 4
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	
	@Test
	public void testWhereConditionDiffOrderAnd() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear = 2 and airline.dayofmonth = 4
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.dayofmonth = 4 and airline.monthofyear = 2 
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionSameOrderOr() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear = 2 or airline.dayofmonth = 4
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear = 2 or airline.dayofmonth = 4
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionDiffOrderOr() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear = 2 or airline.dayofmonth = 4
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.dayofmonth = 4 or airline.monthofyear = 2
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	
	@Test
	public void testWhereConditionSameOrderNotEquals() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear <> 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear <> 2
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionDiffOrderNotEquals() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear <> 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				2 <> airline.monthofyear
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	
	@Test
	public void testWhereConditionSameOrderMultiply() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear * 1 = 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear * 1 = 2
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	@Test
	public void testWhereConditionDiffOrderMultiply() throws Exception {
		
		String sql = """
				select sum(arrdelay),sum(depdelay) from airline where 
				airline.monthofyear * 1 = 2
				""";
		String sql1 = """
				select sum(arrdelay),sum(depdelay) from airline where 
				2 = airline.monthofyear * 1
				""";
		
		SqlNode sqlTree = optimizer.parse(sql);
		SqlNode sqlTree1 = optimizer.parse(sql1);
		sqlTree = optimizer.validate(sqlTree);
		sqlTree1 = optimizer.validate(sqlTree1);		
		RelNode relTree = optimizer.convert(sqlTree);
		RelNode relTree1 = optimizer.convert(sqlTree1);
		

		RelNode optimizerRelTree = optimizer.optimize(relTree,
				relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		RelNode optimizerRelTree1 = optimizer.optimize(relTree1,
				relTree1.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		
		EnumerableFilter filter1 = traverseRelNode(optimizerRelTree, 0);
		EnumerableFilter filter2 = traverseRelNode(optimizerRelTree1, 0);
		RexNodeComparator rexnodecomparator = new RexNodeComparator();
		assertEquals(true, rexnodecomparator.compareWhereConditions(filter1.getCondition(), filter2.getCondition()));
	}
	
	protected EnumerableFilter traverseRelNode(RelNode relNode, int depth) {
		// Traverse child nodes
		List<RelNode> childNodes = relNode.getInputs();
		for (RelNode child : childNodes) {
			if(child instanceof EnumerableFilter) {
				return (EnumerableFilter) child;
			}
			return traverseRelNode(child, depth + 1);
		}
		return null;
	}
	
}
