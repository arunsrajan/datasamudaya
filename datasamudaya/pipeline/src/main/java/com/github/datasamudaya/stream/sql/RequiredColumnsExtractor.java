package com.github.datasamudaya.stream.sql;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.shaded.org.apache.commons.collections.MapUtils;
import org.jgrapht.Graphs;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The Columns Extractor for the Apache Calcite Optimized RelNode
 * @author arun
 *
 */
public class RequiredColumnsExtractor {
	private static final Logger log = LoggerFactory.getLogger(RequiredColumnsExtractor.class);
    private final Map<String, Set<String>> requiredColumnsByTable = new HashMap<>();
    private final Map<String, Set<String>> requiredColumnsByTableCondition = new HashMap<>();
    
    private final Map<RelNode, List<String>> relNodeTablesMap = new HashMap<>();
    
    private final Map<RelNode, List<Set<Integer>>> nodeColumnIndexSet = new HashMap<>();
    private boolean isanyproject = false;
    SimpleDirectedGraph<RelNode,DAGEdge> sdg = new SimpleDirectedGraph<RelNode,DAGEdge>(DAGEdge.class);
    public Map<String, Set<String>> getRequiredColumnsByTable(RelNode relNode) {
    	buildParentMap(relNode);
    	EdgeReversedGraph<RelNode,DAGEdge> erg = new EdgeReversedGraph<RelNode,DAGEdge>(sdg);
    	TopologicalOrderIterator<RelNode,DAGEdge> toi = new TopologicalOrderIterator<>(erg);
    	while(toi.hasNext()) {
    		RelNode child = toi.next();
    		if(child instanceof EnumerableTableScan tscan) {    			
    			relNodeTablesMap.put(tscan, Arrays.asList(tscan.getTable().getQualifiedName().get(1)));
    			handleTableScan(tscan);
    		} else if(child instanceof Filter filter) {
    			var predecessors = Graphs.predecessorListOf(erg, child);    			
    			var successors = Graphs.successorListOf(erg, child);
    			if(CollectionUtils.isEmpty(successors)) {
    				List<String> tablenames = relNodeTablesMap.get(predecessors.get(0));
    				if(isNull(requiredColumnsByTable.get(tablenames.get(0)))) {
    					requiredColumnsByTable.put(tablenames.get(0), convertToString(nodeColumnIndexSet.get(predecessors.get(0))).get(0));
    				} else {
    					requiredColumnsByTable.get(tablenames.get(0)).addAll(convertToString(nodeColumnIndexSet.get(predecessors.get(0))).get(0));
    				}
    			}
    			relNodeTablesMap.put(filter, relNodeTablesMap.get(predecessors.get(0)));
    			handleFilter(filter, nodeColumnIndexSet.get(predecessors.get(0)));
    		} else if(child instanceof Project project) {
    			isanyproject = true;
    			var predecessors = Graphs.predecessorListOf(erg, child);    			
    			var successors = Graphs.successorListOf(erg, child);
    			List<String> tablenames = relNodeTablesMap.get(predecessors.get(0));
    			relNodeTablesMap.put(project, tablenames);
    			if(CollectionUtils.isEmpty(successors)) {    				
    				if(isNull(requiredColumnsByTable.get(tablenames.get(0)))) {
    					requiredColumnsByTable.put(tablenames.get(0), convertToString(nodeColumnIndexSet.get(predecessors.get(0))).get(0));
    				} else {
    					requiredColumnsByTable.get(tablenames.get(0)).addAll(convertToString(nodeColumnIndexSet.get(predecessors.get(0))).get(0));
    				}
    			}
    			handleProject(project, nodeColumnIndexSet.get(predecessors.get(0)));
    			relNodeTablesMap.put(project, relNodeTablesMap.get(predecessors.get(0)));
    		} else if(child instanceof Aggregate agg) {
    			var predecessors = Graphs.predecessorListOf(erg, child);    			
    			var successors = Graphs.successorListOf(erg, child);
    			List<String> tablenames = relNodeTablesMap.get(predecessors.get(0));
    			relNodeTablesMap.put(agg, tablenames);
    			if(CollectionUtils.isEmpty(successors)) {    				
    				if(isNull(requiredColumnsByTable.get(tablenames.get(0)))) {
    					requiredColumnsByTable.put(tablenames.get(0), convertToString(nodeColumnIndexSet.get(predecessors.get(0))).get(0));
    				} else {
    					requiredColumnsByTable.get(tablenames.get(0)).addAll(convertToString(nodeColumnIndexSet.get(predecessors.get(0))).get(0));
    				}
    			}
    			handleAggregate(agg, nodeColumnIndexSet.get(predecessors.get(0)));
    			relNodeTablesMap.put(agg, relNodeTablesMap.get(predecessors.get(0)));
    		} else if(child instanceof Join join) {
    			var predecessors = Graphs.predecessorListOf(erg, child);    			
    			List<String> tablenames = new ArrayList<>();
    			tablenames.addAll(relNodeTablesMap.get(predecessors.get(0)));
    			tablenames.addAll(relNodeTablesMap.get(predecessors.get(1)));
    			relNodeTablesMap.put(join, tablenames);
    			handleJoin(join, nodeColumnIndexSet.get(predecessors.get(0)),nodeColumnIndexSet.get(predecessors.get(1)));
    		} else if(child instanceof Union union) {
    			var predecessors = Graphs.predecessorListOf(erg, child);    			
    			List<String> tablenames = new ArrayList<>();
    			tablenames.addAll(relNodeTablesMap.get(predecessors.get(0)));
    			tablenames.addAll(relNodeTablesMap.get(predecessors.get(1)));
    			relNodeTablesMap.put(union, tablenames);
    			handleUnion(union, nodeColumnIndexSet.get(predecessors.get(0)),nodeColumnIndexSet.get(predecessors.get(1)));
    		} else if(child instanceof Intersect intersect) {
    			var predecessors = Graphs.predecessorListOf(erg, child);    			
    			List<String> tablenames = new ArrayList<>();
    			tablenames.addAll(relNodeTablesMap.get(predecessors.get(0)));
    			tablenames.addAll(relNodeTablesMap.get(predecessors.get(1)));
    			relNodeTablesMap.put(intersect, tablenames);
    			handleIntersect(intersect, nodeColumnIndexSet.get(predecessors.get(0)),nodeColumnIndexSet.get(predecessors.get(1)));
    		}
    	}
    	if(isanyproject) {
    		return requiredColumnsByTableCondition;
    	}
        return requiredColumnsByTable;
    }
    
    private List<Set<String>> convertToString(List<Set<Integer>> lindexes){
    	return lindexes.stream().map(indexes-> indexes.stream().map(index->index+DataSamudayaConstants.EMPTY).collect(Collectors.toCollection(LinkedHashSet::new))).collect(Collectors.toList());
    }
    
    public void buildParentMap(RelNode node) {
    	sdg.addVertex(node);
		for (RelNode child : node.getInputs()) {
			sdg.addVertex(child);
			sdg.addEdge(node, child);
			buildParentMap(child); // Recurse to handle all levels
		}
    }

    
    private void createHashSetInList(List<Set<Integer>> lst, int numberofsetstocreate) {
    	for(int index=0;index<numberofsetstocreate; index++) {
    		lst.add(new LinkedHashSet<>());
    	}
    }
    
    private void handleAggregate(Aggregate aggregate, List<Set<Integer>> parentindexes) {    	
    	List<Set<Integer>> lRequiredColumns = new ArrayList<>();
    	List<Set<Integer>> lparentindexes = new ArrayList<>(parentindexes);
    	List<Set<Integer>> requiredColumns = new ArrayList<>();
    	createHashSetInList(requiredColumns, parentindexes.size());
        // Add grouping columns
        aggregate.getGroupSet().asList().stream().forEach(colIndex->{
            addColumnToMap(aggregate, colIndex, requiredColumns, lparentindexes);
        });

        // Add columns involved in aggregation functions
        for (AggregateCall call : aggregate.getAggCallList()) {
            call.getArgList().stream().forEach(colIndex->{
            	 addColumnToMap(aggregate, colIndex, requiredColumns, lparentindexes);            	
            });
        }
        lRequiredColumns.addAll(requiredColumns);
        nodeColumnIndexSet.put(aggregate, lRequiredColumns); 
    }
    
    private void handleTableScan(TableScan scan) {
        Set<Integer> requiredColumns = new LinkedHashSet<>();
        List<String> tablenames = relNodeTablesMap.get(scan);
        String tablename = tablenames.get(0);
        for(int fieldindex=0; fieldindex < scan.getTable().getRowType().getFieldCount(); fieldindex++) {
        	requiredColumns.add(fieldindex);
        	Set<String> finalcolumns = requiredColumnsByTable.get(tablename);
        	if(isNull(finalcolumns)) {
        		finalcolumns = new LinkedHashSet<>();
        		requiredColumnsByTable.put(tablename, finalcolumns);
        	}
        	finalcolumns.add(fieldindex+DataSamudayaConstants.EMPTY);
        }
        List<Set<Integer>> lRequiredColumns = new ArrayList<>();
        lRequiredColumns.add(requiredColumns);
        nodeColumnIndexSet.put(scan, lRequiredColumns);         
    }

    
    private Integer greatestInMap(Map<String, Integer> tableStartIndexMap) {
    	if(MapUtils.isEmpty(tableStartIndexMap)) {
    		return 0;
    	}
    	return tableStartIndexMap.values().stream().mapToInt(value->value).max().getAsInt();
    }
    
    private void handleProject(Project project, List<Set<Integer>> parentindexes) {
    	List<Set<Integer>> requiredColumns = new ArrayList<>();
    	createHashSetInList(requiredColumns, parentindexes.size());
    	ColumnIndexVisitor civ = new ColumnIndexVisitor(project, requiredColumns, parentindexes);
        project.getProjects().forEach(expr -> expr.accept(civ));
        List<Set<Integer>> lRequiredColumns = new ArrayList<>();
        lRequiredColumns.addAll(requiredColumns);
        nodeColumnIndexSet.put(project, lRequiredColumns); 
    }

    private void handleFilter(Filter filter, List<Set<Integer>> parent) {
    	List<Set<Integer>> requiredColumns = new ArrayList<>();
    	createHashSetInList(requiredColumns, parent.size());
    	filter.getCondition().accept(new ColumnIndexVisitor(filter, requiredColumns, parent));
        nodeColumnIndexSet.put(filter, parent); 
    }

    private void handleJoin(Join join, List<Set<Integer>> parentindexes1, List<Set<Integer>> parentindexes2) {
        // Recursively handle both sides of the join       
        List<Set<Integer>> requiredColumns = new ArrayList<>();
        createHashSetInList(requiredColumns, parentindexes1.size()+parentindexes2.size());
        List<Set<Integer>> parents = new ArrayList<>();
        parents.addAll(parentindexes1);
        parents.addAll(parentindexes2);
        // Process the join condition to extract required columns
        join.getCondition().accept(new JoinConditionColumnVisitor(join, requiredColumns, parents));
        nodeColumnIndexSet.put(join, parents); 
    }
    
    private void handleUnion(Union union, List<Set<Integer>> parentindexes1, List<Set<Integer>> parentindexes2) {
        List<Set<Integer>> parents = new ArrayList<>();
        parents.addAll(parentindexes1);
        parents.addAll(parentindexes2);
        nodeColumnIndexSet.put(union, parents); 
    }
    
    private void handleIntersect(Intersect intersect, List<Set<Integer>> parentindexes1, List<Set<Integer>> parentindexes2) {
        List<Set<Integer>> parents = new ArrayList<>();
        parents.addAll(parentindexes1);
        parents.addAll(parentindexes2);
        nodeColumnIndexSet.put(intersect, parents); 
    }
    
    private class JoinConditionColumnVisitor extends RexVisitorImpl<Void> {
    	List<Set<Integer>> requiredColumns;
    	List<Set<Integer>> parents;
    	Join join;
        protected JoinConditionColumnVisitor(Join join, List<Set<Integer>> requiredColumns, List<Set<Integer>> parents) {
            super(true);
            this.join = join;
            this.requiredColumns = requiredColumns;
            this.parents = parents;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            addColumnToMap(join, inputRef.getIndex(), requiredColumns, parents);
            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            // For complex conditions (e.g., expressions within the join condition), recursively visit arguments
        	analyzeCondition(join, call, requiredColumns, parents);
            call.operands.forEach(operand -> operand.accept(this));
            return null;
        }
    }
    private class ColumnIndexVisitor extends RexVisitorImpl<Void> {
    	List<Set<Integer>> requiredColumns;
        List<Set<Integer>> parentindexes;
        RelNode relNode;
        protected ColumnIndexVisitor(RelNode relNode, List<Set<Integer>> requiredColumns, List<Set<Integer>> parentindexes) {
            super(true);
            this.relNode = relNode;
            this.requiredColumns = requiredColumns;
            this.parentindexes = parentindexes;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
        	addColumnToMap(relNode, inputRef.getIndex(), requiredColumns, parentindexes);
            return null;
        }
        
        @Override
        public Void visitCall(RexCall call) {
            // Recursively handle complex expressions within aggregate functions    
        	analyzeCondition(relNode, call, requiredColumns, parentindexes);
            call.getOperands().forEach(operand -> operand.accept(this));
            return null;
        }
    }
    /**
	 * The function analyzes the condition from join or filter
	 * @param condition
	 */
	private void analyzeCondition(RelNode relNode, RexNode condition, List<Set<Integer>> requiredColumns, List<Set<Integer>> parentindexes) {
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
				analyzeCondition(relNode, rexnode, requiredColumns, parentindexes);
			}

		} else if (condition.isA(SqlKind.INPUT_REF)) {
			RexInputRef colref = (RexInputRef) condition;		
			int index = colref.getIndex();
			addColumnToMap(relNode, index, requiredColumns, parentindexes);
		}
	}
	
	
	public void addColumnToMap(RelNode node, int index,List<Set<Integer>> requiredColumns, List<Set<Integer>> parentindexes) {
		int totalcolcount = 0;
		int prevtotalcolcount = 0;
		Set<Integer> currparent = null;
		int indextable = 0;
		String tablename = null;
		int requiredcolumnsindex = indextable;
		for(Set<Integer> parent:parentindexes) {
			prevtotalcolcount = totalcolcount;
			totalcolcount += parent.size();
			if(index<totalcolcount) {
				requiredcolumnsindex = indextable;
				tablename = relNodeTablesMap.get(node).get(indextable);
				currparent = parent;
				break;					
			}
			indextable++;
		}				
		if(nonNull(currparent)) {
			List<Integer> lcurrparent = new ArrayList<>(currparent);
			requiredColumns.get(requiredcolumnsindex).add(index-prevtotalcolcount);
			if(nonNull(tablename)) {
				Set<String> columnstable = requiredColumnsByTableCondition.get(tablename);
				if(isNull(columnstable)) {
					columnstable = new LinkedHashSet<>();
					requiredColumnsByTableCondition.put(tablename, columnstable);
				}
				columnstable.add(lcurrparent.get(index-prevtotalcolcount)+DataSamudayaConstants.EMPTY);
			}
		}
	}
	
	
}
