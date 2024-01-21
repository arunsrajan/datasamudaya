package com.github.datasamudaya.stream.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.calcite.adapter.enumerable.EnumerableAggregateBase;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.stream.utils.SQLUtils;

import static java.util.Objects.isNull;

/**
 * Required Columns Extractor
 * @author arun
 *
 */
public class RequiredColumnsExtractor extends RelShuttleImpl {

	Map<String,List<String>> tablecolumns;
	Map<RelNode, Integer> relnodetotalcolumns = new HashMap<>(); 
	public RequiredColumnsExtractor(Map<String,Set<String>> requiredColumns, Map<String,List<String>> tablecolumns) {
		this.requiredColumns = requiredColumns;
		this.tablecolumns = tablecolumns;
		
	}
	
	private final Map<String,Set<String>> requiredColumns;

	public Map<String,Set<String>> getRequiredColumns(RelNode relNode) {
		relNode.accept(this);
		return requiredColumns;
	}

	@Override
	protected RelNode visitChild(RelNode parent, int i, RelNode child) {
		if (child instanceof EnumerableTableScan ets) {
			if (parent instanceof EnumerableProject ep) {
				for (RexNode rexNode : ep.getProjects()) {
					rexNode.accept(new RexColumnVisitor(true, ets.getTable().getQualifiedName().get(1)));
				}
			} else if (parent instanceof EnumerableAggregateBase eab) {
				int[] colindexes = SQLUtils.getGroupByColumnIndexes(eab);
				for (int colindex : colindexes) {
					addColumnsToRequiredColumnsMap(ets.getTable().getQualifiedName().get(1), colindex+DataSamudayaConstants.EMPTY);
				}
				for (AggregateCall agg : eab.getAggCallList()) {
					agg.getArgList().stream().map(index->index+DataSamudayaConstants.EMPTY).forEach(index->addColumnsToRequiredColumnsMap(ets.getTable().getQualifiedName().get(1), index));
				}
			} else if(parent instanceof EnumerableFilter ef) {
				IntStream.range(0, ets.getTable().getRowType().getFieldCount()).mapToObj(colindex->(colindex+DataSamudayaConstants.EMPTY)).forEach(index->addColumnsToRequiredColumnsMap(ets.getTable().getQualifiedName().get(1), index));
			} else if (parent instanceof EnumerableHashJoin ehj) {
				RelNode left = ehj.getLeft();
				RelNode right = ehj.getRight();
				left.accept(this);
				right.accept(this);
				if(left instanceof EnumerableTableScan etsleft) {
					String tablename = etsleft.getTable().getQualifiedName().get(1);
					IntStream.range(0, etsleft.getTable().getRowType().getFieldCount()).mapToObj(colindex->(colindex+DataSamudayaConstants.EMPTY)).forEach(index->addColumnsToRequiredColumnsMap(tablename, index));
				}
				if(right instanceof EnumerableTableScan etsright) {
					String tablename = etsright.getTable().getQualifiedName().get(1);
					IntStream.range(0, etsright.getTable().getRowType().getFieldCount()).mapToObj(colindex->(colindex+DataSamudayaConstants.EMPTY)).forEach(index->addColumnsToRequiredColumnsMap(tablename, index));
				}				
			}
		}
		return super.visitChild(parent, i, child);
	}

	class RexColumnVisitor extends RexVisitorImpl<Void> {
		String tablename;
		protected RexColumnVisitor(boolean deep, String tablename) {
			super(deep);
			this.tablename = tablename;
		}

		@Override
		public Void visitInputRef(RexInputRef inputRef) {
			// Add the referenced column to the set of required columns.
			addColumnsToRequiredColumnsMap(tablename,inputRef.getIndex()+DataSamudayaConstants.EMPTY);
			return super.visitInputRef(inputRef);
		}
		// Override other visit methods based on your requirements.
	}
	
	private void analyzeJoinCondition(RexNode condition, String tablename, int start, int end) {
        if (condition.isA(SqlKind.EQUALS)) {
        	RexCall rexcall = (RexCall) condition;
            RexNode left = rexcall.getOperands().get(0);
            RexNode right = rexcall.getOperands().get(1);

            addColumnsToTable(left, tablename, start, end);
            addColumnsToTable(right, tablename, start, end);
        } else {
            // Handle other conditions if needed
            // You may need to recursively analyze more complex conditions
        }
    }
	
	private void addColumnsToTable(RexNode rexNode, String tablename, int start, int end) {
        if (rexNode.isA(SqlKind.INPUT_REF)) {
        	RexInputRef colref = (RexInputRef) rexNode;
        	if(colref.getIndex()>=start && colref.getIndex()<end) {
        		addColumnsToRequiredColumnsMap(tablename, colref.getIndex()+"");
        	}
        }
    }
	
	protected void addColumnsToRequiredColumnsMap(String tablename, String index) {
		Set<String> columnset = requiredColumns.get(tablename);
		if(isNull(columnset)) {
			columnset = new LinkedHashSet<>();
			requiredColumns.put(tablename, columnset);
		}
		columnset.add(index);
	}
	
}