package com.github.datasamudaya.stream.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import com.github.datasamudaya.common.DataSamudayaConstants;

import static java.util.Objects.isNull;

/**
 * The Columns Extractor for the Apache Calcite Optimized RelNode
 * @author arun
 *
 */
public class RequiredColumnsExtractor extends RelShuttleImpl {
    private final Map<String, Set<String>> requiredColumnsByTable = new HashMap<>();

    public Map<String, Set<String>> getRequiredColumnsByTable(RelNode relNode) {
    	relNode.accept(this);
        return requiredColumnsByTable;
    }

    @Override
    public RelNode visitChild(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
            handleTableScan((TableScan) node);
        } else if (node instanceof Project) {
            handleProject((Project) node);
        } else if (node instanceof Filter) {
            handleFilter((Filter) node);
        } else if (node instanceof Join) {
            handleJoin((Join) node);
        } else if (node instanceof Sort) {
            handleSort((Sort) node);
        } else if (node instanceof Aggregate) {
            handleAggregate((Aggregate) node);
        }
        return super.visitChild(node, ordinal, parent);
    }

    private void handleSort(Sort sort) {
        String tableName = getTableName(sort.getInput());
        sort.getCollation().getFieldCollations().forEach(fc -> {
            requiredColumnsByTable.computeIfAbsent(tableName, k -> new HashSet<>()).add(fc.getFieldIndex()+DataSamudayaConstants.EMPTY);
        });
    }

    private void handleAggregate(Aggregate aggregate) {    	

        // Add grouping columns
        aggregate.getGroupSet().asList().stream().forEach(colIndex->{
        	String tableName = getTableName(aggregate.getInput(), colIndex);
            Set<String> columnIndexes = requiredColumnsByTable.computeIfAbsent(tableName, k -> new HashSet<>());
        	columnIndexes.add(DataSamudayaConstants.EMPTY+colIndex);
        	
        });

        // Add columns involved in aggregation functions
        for (AggregateCall call : aggregate.getAggCallList()) {
            call.getArgList().stream().forEach(colIndex->{
            	String tableName = getTableName(aggregate.getInput(), colIndex);
                Set<String> columnIndexes = requiredColumnsByTable.computeIfAbsent(tableName, k -> new HashSet<>());
            	columnIndexes.add(DataSamudayaConstants.EMPTY+colIndex);
            	
            });
        }
    }
    
    private class AggregateFunctionVisitor extends RexVisitorImpl<Void> {
        private final String tableName;
        private final RelNode relNode;
        protected AggregateFunctionVisitor(String tableName, RelNode relNode) {
            super(true);
            this.tableName = tableName;
            this.relNode = relNode;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
        	analyzeCondition(inputRef, tableName, relNode);
            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            // Recursively handle complex expressions within aggregate functions
        	analyzeCondition(call, tableName, relNode);
            call.getOperands().forEach(operand -> operand.accept(this));
            return null;
        }
    }
    private void handleTableScan(TableScan scan) {
        String tableName = scan.getTable().getQualifiedName().get(1);
        // Initialize the set for this table, indicating we've seen it but not necessarily added any specific columns yet
        requiredColumnsByTable.putIfAbsent(tableName, new HashSet<>());
    }

    private void handleProject(Project project) {
        String tableName = getTableName(project.getInput());
        project.getProjects().forEach(expr -> expr.accept(new ColumnIndexVisitor(tableName, project)));
    }

    private void handleFilter(Filter filter) {
        String tableName = getTableName(filter.getInput());
        filter.getCondition().accept(new ColumnIndexVisitor(tableName, filter));
    }

    private void handleJoin(Join join) {
        String leftTableName = getTableName(join.getLeft());
        String rightTableName = getTableName(join.getRight());

        // Recursively handle both sides of the join
        join.getLeft().accept(this);
        join.getRight().accept(this);

        // Process the join condition to extract required columns
        join.getCondition().accept(new JoinConditionColumnVisitor(leftTableName, rightTableName, join));
    }
    private class JoinConditionColumnVisitor extends RexVisitorImpl<Void> {
        private final String leftTable;
        private final String rightTable;
        private final Join join;

        protected JoinConditionColumnVisitor(String leftTable, String rightTable, Join join) {
            super(true);
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.join = join;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();
            // Determine if the column belongs to the left or right table based on index
            if (index < join.getLeft().getRowType().getFieldCount()) {
                requiredColumnsByTable.computeIfAbsent(isNull(leftTable)?getTableName(join.getLeft(), index): leftTable, k -> new HashSet<>()).add(index+DataSamudayaConstants.EMPTY);
            } else {
                // Adjust the index for the right table columns
                int adjustedIndex = index - join.getLeft().getRowType().getFieldCount();
                requiredColumnsByTable.computeIfAbsent(isNull(rightTable)?getTableName(join.getRight(), index): rightTable, k -> new HashSet<>()).add(adjustedIndex+DataSamudayaConstants.EMPTY);
            }
            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            // For complex conditions (e.g., expressions within the join condition), recursively visit arguments
            call.operands.forEach(operand -> operand.accept(this));
            return null;
        }
    }
    private class ColumnIndexVisitor extends RexVisitorImpl<Void> {
        private String tableName;
        private final RelNode relNode;
        
        protected ColumnIndexVisitor(String tableName, RelNode relNode) {
            super(true);
            this.tableName = tableName;
            this.relNode = relNode;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
        	analyzeCondition(inputRef, tableName, relNode);
            return null;
        }
        
        @Override
        public Void visitCall(RexCall call) {
            // Recursively handle complex expressions within aggregate functions        	
            call.getOperands().forEach(operand -> operand.accept(this));
            return null;
        }
    }
    /**
	 * The function analyzes the condition from join or filter
	 * @param condition
	 */
	private void analyzeCondition(RexNode condition, String tableName,RelNode relNode) {
		if (condition.isA(SqlKind.AND) || condition.isA(SqlKind.OR)
				|| condition.isA(SqlKind.EQUALS) || condition.isA(SqlKind.NOT_EQUALS)
				|| condition.isA(SqlKind.GREATER_THAN) || condition.isA(SqlKind.GREATER_THAN_OR_EQUAL)
				|| condition.isA(SqlKind.LESS_THAN)
				|| condition.isA(SqlKind.LESS_THAN_OR_EQUAL)
				|| condition.isA(SqlKind.LIKE)
				|| condition.isA(SqlKind.PLUS) 
				|| condition.isA(SqlKind.MINUS) 
				|| condition.isA(SqlKind.TIMES)
				|| condition.isA(SqlKind.DIVIDE)
		) {
			// For AND nodes, recursively evaluate left and right children
			RexCall rexcall = (RexCall) condition;
			for (RexNode rexnode : rexcall.operands) {
				analyzeCondition(rexnode, tableName, relNode);
			}

		} else if (condition.isA(SqlKind.INPUT_REF)) {
			RexInputRef colref = (RexInputRef) condition;
			if(isNull(tableName)) {
				tableName = getTableName(relNode, colref.getIndex());
			}
			int colindex = getIndexFromRexInputRef(relNode, colref);
			requiredColumnsByTable.computeIfAbsent(tableName, k -> new HashSet<>()).add(colindex+DataSamudayaConstants.EMPTY);
		}
	}
	private int getIndexFromRexInputRef(RelNode node, RexInputRef colref) {
        // This simplistic approach assumes direct children of TableScan nodes. You might need more complex logic here.
        if (node instanceof TableScan) {
            return colref.getIndex()<((TableScan) node).getTable().getRowType().getFieldCount()?colref.getIndex():-1;
        } else if (node instanceof Join) {
            Join join = (Join) node;
            int leftCount = join.getLeft().getRowType().getFieldCount();
            if (colref.getIndex() < leftCount) {
            	if(join.getLeft() instanceof Join) {
            		return getIndexFromRexInputRef(join.getLeft(), colref);
            	}
                // Column is from the left side of the join
                return colref.getIndex();
            } else {
            	if(join.getRight() instanceof Join) {
            		return getIndexFromRexInputRef(join.getRight(), colref);
            	}
                // Column is from the right side of the join, adjust index accordingly
                return colref.getIndex() - leftCount;
            }
        } else if (node instanceof Filter || node instanceof Project) {
            // For Filter and Project, simply pass through to the child, as they don't change the source table of columns
            return getIndexFromRexInputRef(node.getInput(0), colref);
        }
        return -1; // Fallback or error handling for unsupported cases
    }
    private String getTableName(RelNode node) {
        // This simplistic approach assumes direct children of TableScan nodes. You might need more complex logic here.
        if (node instanceof TableScan) {
            return ((TableScan) node).getTable().getQualifiedName().get(1);
        } else if (node instanceof Filter) {
            return getTableName(node.getInput(0));
        }
        return null; // Fallback or error handling for unsupported cases
    }
    private String getTableName(RelNode node, int columnIndex) {
        // This simplistic approach assumes direct children of TableScan nodes. You might need more complex logic here.
        if (node instanceof TableScan) {
            return ((TableScan) node).getTable().getQualifiedName().get(1);
        } else if (node instanceof Join) {
            Join join = (Join) node;
            int leftCount = join.getLeft().getRowType().getFieldCount();
            if (columnIndex < leftCount) {
                // Column is from the left side of the join
                return getTableName(join.getLeft(), columnIndex);
            } else {
                // Column is from the right side of the join, adjust index accordingly
                return getTableName(join.getRight(), columnIndex - leftCount);
            }
        } else if (node instanceof Filter || node instanceof Project) {
            // For Filter and Project, simply pass through to the child, as they don't change the source table of columns
            return getTableName(node.getInput(0), columnIndex);
        }
        return null; // Fallback or error handling for unsupported cases
    }
}
