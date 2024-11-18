package com.github.datasamudaya.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

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
	
	
	//Test cases from chatgpt prompt
	private static RexNodeComparator comparator;

    @BeforeClass
    public static void setUp() {
        comparator = new RexNodeComparator();
    }

    @Test
    public void testCompareIdenticalRexNodes() {
    	// Tests that two semantically equivalent RexLiterals are considered equal.
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral(42, intType, false);
        RexLiteral literal2 = (RexLiteral) rexBuilder.makeLiteral(42, intType, false);
        assertTrue(comparator.compareRexNodesSemantically(literal1, literal2));
    }

    @Test
    public void testCompareDifferentRexLiterals() {
    	//Tests that two different RexLiterals are not semantically equal.
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral(42, intType, false);
        RexLiteral literal2 = (RexLiteral) rexBuilder.makeLiteral(43, intType, false);
        assertFalse(comparator.compareRexNodesSemantically(literal1, literal2));
    }

    @Test
    public void testCompareRexInputRefs() {
    	//Tests comparison of two identical RexInputRefs
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 1);
        assertTrue(comparator.compareRexNodesSemantically(inputRef1, inputRef2));
    }

    @Test
    public void testCompareDifferentRexInputRefs() {
    	// Tests comparison of two different RexInputRefs based on their index.
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);
        assertFalse(comparator.compareRexNodesSemantically(inputRef1, inputRef2));
    }

    @Test
    public void testCompareEqualRexCallsWithSameOperands() {
    	//Tests RexCalls with identical operands and the same operator.
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);        
        RexCall call1 = (RexCall) rexBuilder.makeCall( SqlStdOperatorTable.EQUALS, List.of(inputRef1, inputRef2));
        RexCall call2 = (RexCall) rexBuilder.makeCall( SqlStdOperatorTable.EQUALS, List.of(inputRef1, inputRef2));

        assertTrue(comparator.compareRexNodesSemantically(call1, call2));
    }

    @Test
    public void testCompareEqualRexCallsWithReversedOperands() {
    	//Tests RexCalls with reversed operands but the same operator (=).
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);
        RexCall call1 = (RexCall) rexBuilder.makeCall( SqlStdOperatorTable.EQUALS, List.of(inputRef1, inputRef2));
        RexCall call2 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(inputRef2, inputRef1));

        assertTrue(comparator.compareRexNodesSemantically(call1, call2));
    }

    @Test
    public void testCompareRexCallsWithDifferentOperands() {
    	//Tests RexCalls with different operands.
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);
        RexInputRef inputRef3 = rexBuilder.makeInputRef(intType, 3);
        RexCall call1 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(inputRef1, inputRef2));
        RexCall call2 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(inputRef1, inputRef3));

        assertFalse(comparator.compareRexNodesSemantically(call1, call2));
    }

    @Test
    public void testCompareLogicalOperatorsWithOperandsInDifferentOrder() {
    	//Tests logical operators (AND, OR) with operands in a different order.
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);
        RexCall call1 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.AND, List.of(inputRef1, inputRef2));
        RexCall call2 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.AND, List.of(inputRef2, inputRef1));

        assertTrue(comparator.compareRexNodesSemantically(call1, call2));
    }

    @Test
    public void testCompareComparisonOperators() {
    	//Tests comparison operators with semantically equivalent reversed operands (>, <, >=, <=).
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);
        RexCall call1 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, List.of(inputRef1, inputRef2));
        RexCall call2 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, List.of(inputRef2, inputRef1));

        assertTrue(comparator.compareRexNodesSemantically(call1, call2));
    }

    @Test
    public void testCompareDifferentOperatorTypes() {
    	//Tests comparison between different operators (>, =).
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);
        RexCall call1 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, List.of(inputRef1, inputRef2));
        RexCall call2 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(inputRef1, inputRef2));

        assertFalse(comparator.compareRexNodesSemantically(call1, call2));
    }

    @Test
    public void testCompareComplexExpressionsWithLogicalAndComparisonOperators() {
    	//Tests more complex expressions with logical and comparison operators.
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexInputRef inputRef1 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef inputRef2 = rexBuilder.makeInputRef(intType, 2);
        RexInputRef inputRef3 = rexBuilder.makeInputRef(intType, 3);
        
        RexCall comparisonCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(inputRef1, inputRef2));
        RexCall logicalCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.AND, List.of(comparisonCall, (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, List.of(inputRef3, inputRef2))));
        
        RexCall comparisonCall2 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(inputRef2, inputRef1));
        RexCall logicalCall2 = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.AND, List.of(comparisonCall2,(RexCall) rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, List.of(inputRef3, inputRef2))));

        assertTrue(comparator.compareRexNodesSemantically(logicalCall, logicalCall2));
    }

    @Test
    public void testCompareIdenticalRexLiterals() {
        // Identical literals with the same type and value
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral(42, intType, false);
        RexLiteral literal2 = (RexLiteral) rexBuilder.makeLiteral(42, intType, false);
        
        Assertions.assertTrue(comparator.compareRexNodesSemantically(literal1, literal2), "Identical RexLiterals should be considered equal");
    }

    @Test
    public void testCompareRexLiteralsWithDifferentTypes() {
        // Same values, different types
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RelDataType stringType = typeFactory.createJavaType(String.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral(42, intType, false);
        RexLiteral literal2 = (RexLiteral) rexBuilder.makeLiteral("42", stringType, false);
        
        Assertions.assertFalse(comparator.compareRexNodesSemantically(literal1, literal2), "RexLiterals with different types should not be considered equal");
    }

    @Test
    public void testCompareRexLiteralsWithNullValues() {
        // Null values (should be treated as equal if both are null of the same type)
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType1 = typeFactory.createJavaType(Integer.class);
        RelDataType intType2 = typeFactory.createJavaType(Integer.class);
        RelDataType stringType = typeFactory.createJavaType(String.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral(null, intType1, false);
        RexLiteral literal2 = (RexLiteral) rexBuilder.makeLiteral(null, intType2, false);
        
        Assertions.assertTrue(comparator.compareRexNodesSemantically(literal1, literal2), "RexLiterals with null values of the same type should be considered equal");
        
        // Null values with different types should not be equal
        RexLiteral literal3 = (RexLiteral) rexBuilder.makeLiteral(null, stringType, false);
        Assertions.assertFalse(comparator.compareRexNodesSemantically(literal1, literal3), "Null RexLiterals with different types should not be considered equal");
    }

    @Test
    public void testCompareEmptyStringAndNull() {
        // Empty string vs null should not be considered the same
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType stringType = typeFactory.createJavaType(String.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral("", stringType, false);
        RexLiteral literal2 = (RexLiteral) rexBuilder.makeLiteral(null, stringType, false);
        
        Assertions.assertFalse(comparator.compareRexNodesSemantically(literal1, literal2), "Empty string and null RexLiterals should not be considered equal");
    }

    @Test
    public void testCompareZeroAndNull() {
        // Zero vs null should not be considered the same (for numeric types)
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral(0, intType, false);
        RexLiteral literal2 =(RexLiteral) rexBuilder.makeLiteral(null, intType, false);
        
        Assertions.assertFalse(comparator.compareRexNodesSemantically(literal1, literal2), "Zero and null RexLiterals should not be considered equal");
    }

    @Test
    public void testCompareRexLiteralsWithEdgeCaseValues() {
        // Edge case values, e.g., maximum integer value, special literals
    	JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        RelDataType intType = typeFactory.createJavaType(Integer.class);
        RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        RexLiteral literal1 = (RexLiteral) rexBuilder.makeLiteral(Integer.MAX_VALUE, intType, false);
        RexLiteral literal2 = (RexLiteral) rexBuilder.makeLiteral(Integer.MAX_VALUE, intType, false);
        RexLiteral literal3 = (RexLiteral) rexBuilder.makeLiteral(Integer.MIN_VALUE, intType, false);

        Assertions.assertTrue(comparator.compareRexNodesSemantically(literal1, literal2), "RexLiterals with the same edge case values should be equal");
        Assertions.assertFalse(comparator.compareRexNodesSemantically(literal1, literal3), "RexLiterals with different edge case values should not be equal");
    }
    
  //Test cases from chatgpt prompt end
	
}
