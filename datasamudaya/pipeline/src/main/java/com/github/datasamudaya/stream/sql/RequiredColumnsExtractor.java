package com.github.datasamudaya.stream.sql;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableAggregateBase;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.stream.utils.SQLUtils;

/**
 * Required Columns Extractor
 * @author arun
 *
 */
public class RequiredColumnsExtractor extends RelShuttleImpl {

	Map<String, List<String>> tablecolumns;
	private final Map<String, Set<String>> requiredColumns;
	Map<String, Integer> tablemaxindexmap = new LinkedHashMap<>();
	Map<RelNode, RelNode> childparentrel = new HashMap<>();
	Map<RelNode, Boolean> visited = new LinkedHashMap<>();

	public RequiredColumnsExtractor(Map<String, Set<String>> requiredColumns, Map<String, List<String>> tablecolumns) {
		this.requiredColumns = requiredColumns;
		this.tablecolumns = tablecolumns;
	}

	public Map<String, Set<String>> getRequiredColumns(RelNode relNode) {
		relNode.accept(this);
		return requiredColumns;
	}

	@Override
	protected RelNode visitChild(RelNode parent, int i, RelNode child) {
		if (child instanceof EnumerableTableScan ets) {
			addColumnsToMap(parent, child);
		}
		childparentrel.put(child, parent);
		return super.visitChild(parent, i, child);
	}

	/**
	 * The function adds column indexes to Map
	 * @param parent
	 */
	protected void addColumnsToMap(RelNode parent, RelNode child) {
		if (parent instanceof EnumerableProject ep) {
			if (nonNull(child.getTable())) {
				addEntriesTo(child.getTable().getQualifiedName().get(1), child.getTable().getRowType().getFieldCount());
			}
			for (RexNode rexNode : ep.getProjects()) {
				rexNode.accept(new RexColumnVisitor(true
				));
			}
		} else if (parent instanceof EnumerableAggregateBase eab) {
			if (nonNull(child.getTable())) {
				addEntriesTo(child.getTable().getQualifiedName().get(1), child.getTable().getRowType().getFieldCount());
			}
			int[] colindexes = SQLUtils.getGroupByColumnIndexes(eab);
			for (int colindex : colindexes) {
				addColumnsToTable(colindex);
			}
			for (AggregateCall agg : eab.getAggCallList()) {
				agg.getArgList().stream().forEach(colindex -> {
					addColumnsToTable(colindex);
				});
			}
		} else if (parent instanceof EnumerableFilter ef) {
			if (nonNull(child.getTable())) {
				addEntriesTo(child.getTable().getQualifiedName().get(1), child.getTable().getRowType().getFieldCount());
			}
			addColumnsToMap(childparentrel.get(ef), ef);
			visited.put(child, true);
			if (child instanceof EnumerableTableScan ets && isNull(childparentrel.get(ef))) {
				for (int index = 0;index < child.getTable().getRowType().getFieldCount();index++) {
					addColumnsToRequiredColumnsMap(ets.getTable().getQualifiedName().get(1), DataSamudayaConstants.EMPTY + index);
				}
			}
			analyzeCondition(ef.getCondition());
		} else if (parent instanceof EnumerableHashJoin ehj) {
			RelNode left = ehj.getLeft();
			RelNode right = ehj.getRight();

			if (left instanceof EnumerableTableScan etsleft && !isVisited(left)) {
				addEntriesTo(etsleft.getTable().getQualifiedName().get(1), etsleft.getTable().getRowType().getFieldCount());
				visited.put(left, true);
			}
			if (right instanceof EnumerableTableScan etsright && !isVisited(right)) {
				addEntriesTo(etsright.getTable().getQualifiedName().get(1), etsright.getTable().getRowType().getFieldCount());
				visited.put(right, true);
			}
			if (!isVisited(ehj)) {
				analyzeCondition(ehj.getCondition());
				visited.put(ehj, true);
			}
			if (!(childparentrel.get(ehj) instanceof EnumerableHashJoin)) {
				addColumnsToMap(childparentrel.get(ehj), ehj);
			}
		}
	}

	/**
	 * The function returns visited to true or false for a given RelNode 
	 * @param relnode
	 * @return true or false
	 */
	protected Boolean isVisited(RelNode relnode) {
		if (nonNull(visited.get(relnode)) && visited.get(relnode)) {
			return true;
		}
		return false;
	}


	/**
	 * Add Entries to 
	 * @param column
	 * @param maxindex
	 */
	protected void addEntriesTo(String tablename, int maxindex) {
		List<Entry<String, Integer>> entries = new ArrayList<>(tablemaxindexmap.entrySet());
		if (!CollectionUtils.isEmpty(entries)) {
			Entry<String, Integer> entry = entries.get(entries.size() - 1);
			tablemaxindexmap.put(tablename, maxindex + entry.getValue());
		} else {
			tablemaxindexmap.put(tablename, maxindex);
		}
	}

	class RexColumnVisitor extends RexVisitorImpl<Void> {
		protected RexColumnVisitor(boolean deep) {
			super(deep);
		}

		@Override
		public Void visitInputRef(RexInputRef inputRef) {
			// Add the referenced column to the set of required columns.
			analyzeCondition(inputRef);
			return super.visitInputRef(inputRef);
		}
		// Override other visit methods based on your requirements.
	}

	/**
	 * The function analyzes the condition from join or filter
	 * @param condition
	 */
	private void analyzeCondition(RexNode condition) {
		if (condition.isA(SqlKind.AND) || condition.isA(SqlKind.OR)
				|| condition.isA(SqlKind.EQUALS) || condition.isA(SqlKind.NOT_EQUALS)
				|| condition.isA(SqlKind.GREATER_THAN) || condition.isA(SqlKind.GREATER_THAN_OR_EQUAL)
				|| condition.isA(SqlKind.LESS_THAN)
				|| condition.isA(SqlKind.LESS_THAN_OR_EQUAL)
				|| condition.isA(SqlKind.LIKE)
		) {
			// For AND nodes, recursively evaluate left and right children
			RexCall rexcall = (RexCall) condition;
			for (RexNode rexnode : rexcall.operands) {
				analyzeCondition(rexnode);
			}

		} else if (condition.isA(SqlKind.INPUT_REF)) {
			RexInputRef colref = (RexInputRef) condition;
			addColumnsToTable(colref.getIndex());
		}
	}

	/**
	 * The function adds the index column to corresponding table
	 * @param index
	 */
	private void addColumnsToTable(int index) {
		List<Entry<String, Integer>> entries = new ArrayList<>(tablemaxindexmap.entrySet());
		for (int entindex = 0;entindex < entries.size();entindex++) {
			Entry<String, Integer> entry = entries.get(entindex);
			if (index < entry.getValue()) {
				if (entindex == 0) {
					addColumnsToRequiredColumnsMap(entry.getKey(), index + DataSamudayaConstants.EMPTY);
				} else {
					addColumnsToRequiredColumnsMap(entry.getKey(), (index - entries.get(entindex - 1).getValue()) + DataSamudayaConstants.EMPTY);
				}
				break;
			}

		}
	}

	/**
	 * The function adds the index to the corresponding table
	 * @param tablename
	 * @param index
	 */
	protected void addColumnsToRequiredColumnsMap(String tablename, String index) {
		Set<String> columnset = requiredColumns.get(tablename);
		if (isNull(columnset)) {
			columnset = new LinkedHashSet<>();
			requiredColumns.put(tablename, columnset);
		}
		columnset.add(index);
	}

}
