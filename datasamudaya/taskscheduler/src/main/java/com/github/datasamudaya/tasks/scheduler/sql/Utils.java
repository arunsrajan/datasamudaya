package com.github.datasamudaya.tasks.scheduler.sql;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import com.github.datasamudaya.common.utils.sql.Optimizer;
import com.github.datasamudaya.common.utils.sql.SimpleSchema;
import com.github.datasamudaya.common.utils.sql.SimpleTable;

/**
 * @author arun
 * Utils for validating and optimizing sql Relational Node 
 */
public class Utils {
	
	/**
	 * Validates and optimizes sql query nd returns Optimized RelNode.
	 * @param tablecolumnsmap
	 * @param tablecolumntypesmap
	 * @param sql
	 * @param db
	 * @param isDistinct
	 * @return Optimized RelNode
	 * @throws Exception
	 */
	public static RelNode validateSql(ConcurrentMap<String, List<String>> tablecolumnsmap,
			ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap, String sql, String db,
			AtomicBoolean isDistinct) throws Exception {
		Set<String> tablesfromconfig = tablecolumnsmap.keySet();
		SimpleSchema.Builder builder = SimpleSchema.newBuilder(db);
		for (String table : tablesfromconfig) {
			List<String> columns = tablecolumnsmap.get(table);
			List<SqlTypeName> sqltypes = tablecolumntypesmap.get(table);
			builder.addTable(getSimpleTable(table, columns.toArray(new String[columns.size()]),
					sqltypes.toArray(new SqlTypeName[sqltypes.size()])));
		}
		SimpleSchema schema = builder.build();
		Optimizer optimizer = Optimizer.create(schema);
		SqlNode sqlTree = optimizer.parse(sql);
		sqlTree = optimizer.validate(sqlTree);
		if (sqlTree.getKind() == SqlKind.SELECT) {
			SqlSelect selectNode = (SqlSelect) sqlTree;
			isDistinct.set(selectNode.isDistinct());
		}
		RelNode relTree = optimizer.convert(sqlTree);
		RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.FILTER_MERGE,
				CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE,
				CoreRules.AGGREGATE_JOIN_TRANSPOSE,
				CoreRules.PROJECT_MERGE,
				CoreRules.FILTER_INTO_JOIN,
				CoreRules.AGGREGATE_PROJECT_MERGE,
				CoreRules.PROJECT_FILTER_VALUES_MERGE, EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_FILTER_RULE,
				EnumerableRules.ENUMERABLE_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_SORT_RULE,
				EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
				EnumerableRules.ENUMERABLE_UNION_RULE, EnumerableRules.ENUMERABLE_INTERSECT_RULE);
		relTree = trimUnusedFields(relTree);
		return optimizer.optimize(relTree, relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
	}
	/**
	 * Get simple table given the tablename, fields and types
	 * 
	 * @param tablename
	 * @param fields
	 * @param types
	 * @return SimpleTable Object
	 */
	private static SimpleTable getSimpleTable(String tablename, String[] fields, SqlTypeName[] types) {
		SimpleTable.Builder builder = SimpleTable.newBuilder(tablename);
		int typecount = 0;
		for (String field : fields) {
			builder = builder.addField(field, types[typecount]);
			typecount++;
		}
		return builder.withRowCount(60000L).build();
	}
	
	/**
	 * Columns Pruning only selected fields.
	 * @param relNode
	 * @return RelNode with selected fields.
	 */
	private static RelNode trimUnusedFields(RelNode relNode) {
	    final List<RelOptTable> relOptTables = RelOptUtil.findAllTables(relNode);
	    RelOptSchema relOptSchema = null;
	    if (relOptTables.size() != 0) {
	      relOptSchema = relOptTables.get(0).getRelOptSchema();
	    }
	    final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(
	        relNode.getCluster(), relOptSchema);
	    final RelFieldTrimmer relFieldTrimmer = new RelFieldTrimmer(null, relBuilder);
	    final RelNode rel = relFieldTrimmer.trim(relNode);
	    return rel;
	  
	}
}
