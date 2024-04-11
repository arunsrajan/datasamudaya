package com.github.datasamudaya.stream.sql;

import static java.util.Objects.isNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.rel.RelNode;
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
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.shaded.org.apache.commons.collections.MapUtils;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The Columns Extractor for the Apache Calcite Optimized RelNode
 * @author arun
 *
 */
public class RequiredColumnsExtractor {
    private final Map<String, Set<String>> requiredColumnsByTable = new HashMap<>();
    private final Map<String, Set<String>> requiredColumnsByTableCondition = new HashMap<>();
    private final Map<String, Integer> tableNumElemMap = new LinkedHashMap<>();
    private final Map<RelNode, RelNode> childToParentMap = new HashMap<>();
    private final Map<RelNode, Set<Integer>> nodeColumnIndexSet = new HashMap<>();
    private Stack<Set<Integer>> columnStack = new Stack<>();
    private Stack<String> tablestack = new Stack<>();
    private boolean isanyproject = false;
    public Map<String, Set<String>> getRequiredColumnsByTable(RelNode relNode) {
    	buildParentMap(relNode);
    	if(isanyproject) {
    		return requiredColumnsByTableCondition;
    	}
        return requiredColumnsByTable;
    }
    
    public void buildParentMap(RelNode node) {
    	if (node instanceof TableScan) {
            handleTableScan((TableScan) node);
        } else if (node instanceof Project) {
        	isanyproject = true;
            handleProject((Project) node);
        } else if (node instanceof Filter) {
            handleFilter((Filter) node);
        } else if (node instanceof Sort) {
            handleSort((Sort) node);
        } else if (node instanceof Aggregate) {
            handleAggregate((Aggregate) node);
        }
        if(node instanceof Join join) {
    		if(join.getLeft() instanceof TableScan left) {
    			 handleTableScan((TableScan) left);
    		} else {
    			buildParentMap(join.getLeft());
    		}
    		if(join.getRight() instanceof TableScan right) {
   			 handleTableScan((TableScan) right);
	   		} else {
	   			buildParentMap(join.getRight());
	   		}
    		Set<Integer> requiredColumns = new LinkedHashSet<>();
            // Process the join condition to extract required columns
            join.getCondition().accept(new JoinConditionColumnVisitor(requiredColumns));
            nodeColumnIndexSet.put(join, requiredColumns); 
            columnStack.push(requiredColumns);
    		if (!columnStack.isEmpty()) {                
    			mergeOriginsJoin(join.getLeft(), join.getRight(), node);
            }
    	} else {
			for (RelNode child : node.getInputs()) {
				childToParentMap.put(child, node);
				buildParentMap(child); // Recurse to handle all levels
				if (!columnStack.isEmpty()) {
					mergeOrigins(child, node);
				}
			}
    	}
    }
    private void mergeOriginsJoin(RelNode child1,RelNode child2, RelNode parent) {
    	Join join = (Join) parent;    	
    	Set<Integer> childindexesroot = columnStack.pop();
        Set<Integer> childindexesright = columnStack.pop(); 
        Set<Integer> childindexesleft = columnStack.pop();
        Set<Integer> topushindexes = new LinkedHashSet<>(childindexesleft);
        topushindexes.addAll(childindexesright);
        topushindexes.stream().forEach(index->{
			addColumnIndexToRequiredColumn(index, false);			
		});
		columnStack.push(topushindexes);
		childindexesroot.stream().forEach(index->{
			addColumnIndexToRequiredColumn(index, true);			
		});
    }
    
    private void addColumnIndexToRequiredColumn(int index, boolean condition) {    	
    	boolean isfirstelem = true;
    	Entry<String, Integer> entryforindextoadd = null;
    	Entry<String, Integer> preventryforindextoadd = null;
    	for(Entry<String, Integer> entryforindex : tableNumElemMap.entrySet()) {
    		entryforindextoadd = entryforindex;
    		if(index<entryforindex.getValue()) {  
    			break;
    		} else {
    			isfirstelem = false;
    		}
    		preventryforindextoadd = entryforindextoadd;
    	}
    	if(condition) {
    		requiredColumnsByTableCondition.computeIfAbsent(entryforindextoadd.getKey(), k -> new LinkedHashSet<>());
    		requiredColumnsByTableCondition.get(entryforindextoadd.getKey()).add((isfirstelem?index:index-preventryforindextoadd.getValue())+DataSamudayaConstants.EMPTY);
    	} else {
    		requiredColumnsByTable.computeIfAbsent(entryforindextoadd.getKey(), k -> new LinkedHashSet<>());
    		requiredColumnsByTable.get(entryforindextoadd.getKey()).add((isfirstelem?index:index-preventryforindextoadd.getValue())+DataSamudayaConstants.EMPTY);
    	}
    }    
    
    private void mergeOrigins(RelNode child, RelNode parent) {
    	Set<Integer> topushindexes = new LinkedHashSet<>();
    	if(parent instanceof Filter) {
    		if(isNull(getParent(parent))) {
    			Set<Integer> childindexes = columnStack.pop(); 
    			Set<Integer> parentindexes = columnStack.pop();
        		topushindexes.addAll(childindexes);
        		topushindexes.addAll(parentindexes);    	
    			String tablename = tablestack.pop();
    			topushindexes.stream().forEach(index->{
    				addColumnIndexToRequiredColumn(index, true);
    			});
    			tablestack.push(tablename);
    		} else {
    			String tablename = tablestack.pop();
    			Set<Integer> childindexes = columnStack.pop(); 
    			Set<Integer> parentindexes = columnStack.pop();
    			topushindexes.addAll(childindexes);
    			parentindexes.stream().forEach(index->{
    				addColumnIndexToRequiredColumn(index, true);
    			});
    			tablestack.push(tablename);
    		}
    	} else if(parent instanceof Project || parent instanceof Aggregate || parent instanceof Sort) {
    		Set<Integer> childindexes = columnStack.pop(); 
    		Set<Integer> parentindexes = columnStack.pop();
    		List<Integer> childindexl = new ArrayList<>(childindexes);
    		parentindexes.stream().filter(index->index!=-1 && index<childindexl.size()).map(index->childindexl.get(index)).forEach(topushindexes::add);
    		String tablename = tablestack.pop();
    		tablestack.push(tablename);
    		topushindexes.stream().forEach(index->{
				addColumnIndexToRequiredColumn(index, true);
			});
    	}
    	columnStack.push(topushindexes);
    }
    public RelNode getParent(RelNode child) {
        return childToParentMap.get(child);
    }

    private void handleSort(Sort sort) {
    	Set<Integer> requiredColumns = new LinkedHashSet<>();
    	sort.getCollation().getFieldCollations().forEach(fc -> {
    		requiredColumns.add(fc.getFieldIndex());    		
    	});
    	nodeColumnIndexSet.put(sort, requiredColumns); 
    	columnStack.push(requiredColumns);
    }

    private void handleAggregate(Aggregate aggregate) {    	
    	Set<Integer> requiredColumns = new LinkedHashSet<>();
        // Add grouping columns
        aggregate.getGroupSet().asList().stream().forEach(colIndex->{
            requiredColumns.add(colIndex);
        });

        // Add columns involved in aggregation functions
        for (AggregateCall call : aggregate.getAggCallList()) {
        	if(CollectionUtils.isEmpty(call.getArgList())){
        		requiredColumns.add(-1);
        	}
            call.getArgList().stream().forEach(colIndex->{
            	 requiredColumns.add(colIndex);            	
            });
        }
        nodeColumnIndexSet.put(aggregate, requiredColumns); 
        columnStack.push(requiredColumns);
    }
    
    private void handleTableScan(TableScan scan) {
        String tableName = scan.getTable().getQualifiedName().get(1);        
        Set<Integer> requiredColumns = new LinkedHashSet<>();        
        requiredColumnsByTable.putIfAbsent(tableName, new LinkedHashSet<>());
        int greatestindex = greatestInMap(tableNumElemMap);
        tableNumElemMap.putIfAbsent(tableName, scan.getTable().getRowType().getFieldCount()+greatestInMap(tableNumElemMap));
        for(int fieldindex=0; fieldindex < scan.getTable().getRowType().getFieldCount(); fieldindex++) {
        	requiredColumns.add(greatestindex+fieldindex);
        	addColumnIndexToRequiredColumn(greatestindex+fieldindex, false);
        }        
        nodeColumnIndexSet.put(scan, requiredColumns); 
        columnStack.push(requiredColumns);
        tablestack.push(tableName);                
    }

    
    private Integer greatestInMap(Map<String, Integer> tableStartIndexMap) {
    	if(MapUtils.isEmpty(tableStartIndexMap)) {
    		return 0;
    	}
    	return tableStartIndexMap.values().stream().mapToInt(value->value).max().getAsInt();
    }
    
    private void handleProject(Project project) {
    	Set<Integer> requiredColumns = new LinkedHashSet<>();
    	ColumnIndexVisitor civ = new ColumnIndexVisitor(requiredColumns);
        project.getProjects().forEach(expr -> expr.accept(civ));
        nodeColumnIndexSet.put(project, requiredColumns); 
        columnStack.push(requiredColumns);
    }

    private void handleFilter(Filter filter) {
    	Set<Integer> requiredColumns = new LinkedHashSet<>();
        filter.getCondition().accept(new ColumnIndexVisitor(requiredColumns));
        nodeColumnIndexSet.put(filter, requiredColumns); 
        columnStack.push(requiredColumns);
    }

    private void handleJoin(Join join) {
        // Recursively handle both sides of the join
        buildParentMap(join.getLeft());
        buildParentMap(join.getRight());
        Set<Integer> requiredColumns = new LinkedHashSet<>();
        // Process the join condition to extract required columns
        join.getCondition().accept(new JoinConditionColumnVisitor(requiredColumns));
        nodeColumnIndexSet.put(join, requiredColumns); 
        columnStack.push(requiredColumns);
    }
    private class JoinConditionColumnVisitor extends RexVisitorImpl<Void> {
    	Set<Integer> requiredColumns;
        protected JoinConditionColumnVisitor(Set<Integer> requiredColumns) {
            super(true);
            this.requiredColumns = requiredColumns;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();
            requiredColumns.add(index);
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
        Set<Integer> requiredColumns;
        protected ColumnIndexVisitor(Set<Integer> requiredColumns) {
            super(true);
            this.requiredColumns = requiredColumns;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
        	requiredColumns.add(inputRef.getIndex());
            return null;
        }
        
        @Override
        public Void visitCall(RexCall call) {
            // Recursively handle complex expressions within aggregate functions    
        	analyzeCondition(call, requiredColumns);
            call.getOperands().forEach(operand -> operand.accept(this));
            return null;
        }
    }
    /**
	 * The function analyzes the condition from join or filter
	 * @param condition
	 */
	private void analyzeCondition(RexNode condition, Set<Integer> requiredColumns) {
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
				analyzeCondition(rexnode, requiredColumns);
			}

		} else if (condition.isA(SqlKind.INPUT_REF)) {
			RexInputRef colref = (RexInputRef) condition;			
			requiredColumns.add(colref.getIndex());
		}
	}
}
