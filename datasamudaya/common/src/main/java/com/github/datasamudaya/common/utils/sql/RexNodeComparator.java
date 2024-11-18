package com.github.datasamudaya.common.utils.sql;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
/**
 * @author arun
 * The class for compare two RexNode
 */
public class RexNodeComparator {
	/**
	 * Compare two RexNodes semantically
	 * @param node1
	 * @param node2
	 * @return true if the two RexNodes are semantically equivalent
	 */
	public boolean compareRexNodesSemantically(RexNode node1, RexNode node2) {
	    // Check if both nodes are the same instance
	    if (node1 == node2) {
	        return true;
	    }

	    // Check types
	    if (node1 instanceof RexLiteral && node2 instanceof RexLiteral) {
	        return compareRexLiterals((RexLiteral) node1, (RexLiteral) node2);
	    } else if (node1 instanceof RexInputRef && node2 instanceof RexInputRef) {
	        return compareRexInputRefs((RexInputRef) node1, (RexInputRef) node2);
	    } else if (node1 instanceof RexCall && node2 instanceof RexCall) {
	        return compareRexCallsSemantically((RexCall) node1, (RexCall) node2);
	    }

	    return false; // Different types or unsupported types
	}
	/**
	 * compare two RexLiterals semantically
	 * @param literal1
	 * @param literal2
	 * @return true if the two RexLiterals are semantically equivalent
	 */
	private boolean compareRexLiterals(RexLiteral literal1, RexLiteral literal2) {
	    return literal1.getType() == literal2.getType() && (
	    		isNull(literal1.getValue()) && isNull(literal2.getValue())
	    		|| nonNull(literal1.getValue()) &&
	           literal1.getValue().equals(literal2.getValue()));
	}
	/**
	 * compare two RexInputRefs semantically
	 * @param inputRef1
	 * @param inputRef2
	 * @return true if the two RexInputRefs are semantically equivalent
	 */
	private boolean compareRexInputRefs(RexInputRef inputRef1, RexInputRef inputRef2) {
	    return inputRef1.getIndex() == inputRef2.getIndex();
	}
	
	/**
	 * compare two RexCalls semantically
	 * @param call1
	 * @param call2
	 * @return true if the two RexCalls are semantically equivalent
	 */
	public boolean compareRexCallsSemantically(RexCall call1, RexCall call2) {
	    if (isReversedComparison(call1.getOperator().getName(), call2.getOperator().getName())) {
	        // Get the operands
	        List<RexNode> operands1 = call1.getOperands();
	        List<RexNode> operands2 = call2.getOperands();

	        // Check that the operands are equivalent (the first operand in the reversed comparison should match the second)
	        if (operands1.size() == 2 && operands2.size() == 2) {
	            return compareRexNodesSemantically(operands1.get(0), operands2.get(1)) &&
	                   compareRexNodesSemantically(operands1.get(1), operands2.get(0));
	        }
	    }
	    // Get the operands
	    List<RexNode> operands1 = call1.getOperands();
	    List<RexNode> operands2 = call2.getOperands();

	    // Handle logical operators AND and OR
	    if (call1.getOperator().getName().equals("AND") || call1.getOperator().getName().equals("OR")) {
	        return compareOperandsIgnoringOrder(operands1, operands2);
	    }
	    
	    // Handle comparison operators (including equality and others)
	    if(!call1.getOperator().getName().equals(call2.getOperator().getName())) {
	    	return false;
	    }
	    else if (call1.getOperator().getName().equals("=") && 
	    		call2.getOperator().getName().equals("=")) {
	        // For equality, check both combinations of operand orders
	        return (operands1.size() == operands2.size()) && 
	               (compareOperandsIgnoringOrder(operands1, operands2) || 
	                compareOperandsIgnoringOrder(operands2, operands1));
	    } else if (isComparisonOperator(call1.getOperator().getName()) && 
	    		isComparisonOperator(call2.getOperator().getName()) &&
	    		call1.getOperator().getName().equals(call2.getOperator().getName())) {
	        // For comparison operators, check the number of operands
	        if (operands1.size() != operands2.size()) {
	            return false; // Different number of operands
	        }
	        
	        // Compare each operand
	        for (int i = 0; i < operands1.size(); i++) {
	            if (!compareRexNodesSemantically(operands1.get(i), operands2.get(i))) {
	                return false; // Operands are not semantically equal
	            }
	        }
	        return true; // All checks passed, the calls are semantically equal
	    }

	    // Check the number of operands
	    if (operands1.size() != operands2.size()) {
	        return false; // Different number of operands
	    }

	    // Compare each operand
	    for (int i = 0; i < operands1.size(); i++) {
	        if (!compareRexNodesSemantically(operands1.get(i), operands2.get(i))) {
	            return false; // Operands are not semantically equal
	        }
	    }

	    return true; // All checks passed, the calls are semantically equal
	}
	/**
	 * reversed comparison
	 * @param operator1
	 * @param operator2
	 * @return true if the two operators are semantically equivalent
	 */
	private boolean isReversedComparison(String operator1, String operator2) {
	    // Define pairs of semantically equivalent operators
	    return (operator1.equals(">") && operator2.equals("<=")) ||
	           (operator1.equals("<") && operator2.equals(">=")) ||
	           (operator1.equals(">=") && operator2.equals("<")) ||
	           (operator1.equals("<=") && operator2.equals(">")) ||
	           (operator1.equals("<>") && operator2.equals("<>")); // Treat <> as self-equivalent
	}
	
	private boolean isComparisonOperator(String operatorName) {
	    return operatorName.equals(">") ||
	           operatorName.equals(">=") ||
	           operatorName.equals("<") ||
	           operatorName.equals("<=") ||
	           operatorName.equals("<>");
	}
	/**
	 * compare operands ignoring order
	 * @param operands1
	 * @param operands2
	 * @return true if the operands are semantically equivalent
	 */
	public boolean compareOperandsIgnoringOrder(List<RexNode> operands1, List<RexNode> operands2) {
		if (operands1.size() != operands2.size()) {
			return false;
		}
	    // Create a copy of the operands to sort them
	    List<RexNode> sortedOperands1 = new ArrayList<>(operands1);
	    List<RexNode> sortedOperands2 = new ArrayList<>(operands2);

	    // Sort the operands to compare
	    Collections.sort(sortedOperands1, this::compareRexNodeOrder);
	    Collections.sort(sortedOperands2, this::compareRexNodeOrder);

	    // Compare sorted operands
	    for (int i = 0; i < sortedOperands1.size(); i++) {
	        if (!compareRexNodesSemantically(sortedOperands1.get(i), sortedOperands2.get(i))) {
	            return false; // Operands are not equal
	        }
	    }
	    return true; // All operands match
	}
	/**
	 * compare two RexNodes
	 * @param node1
	 * @param node2
	 * @return true if the two nodes are semantically equivalent
	 */
	private int compareRexNodeOrder(RexNode node1, RexNode node2) {
	    // This method can be customized to provide consistent ordering for sorting
	    return node1.hashCode() - node2.hashCode();
	}
	/**
	 * compare where conditions
	 * @param rexnode1
	 * @param rexnode2
	 * @return true if the where conditions are semantically equivalent
	 */
	public boolean compareWhereConditions(RexNode rexnode1, RexNode rexnode2) {
		List<RexNode> conditions1 = ((RexCall)rexnode1).getOperands();
		List<RexNode> conditions2 = ((RexCall)rexnode2).getOperands();
	    // Check if both condition lists have the same size
	    if (conditions1.size() != conditions2.size()) {
	        return false; // Different number of conditions
	    }

	    // Compare the conditions ignoring order
	    for (RexNode condition1 : conditions1) {
	        boolean foundEquivalent = false;
	        for (RexNode condition2 : conditions2) {
	            if (compareRexNodesSemantically(condition1, condition2)) {
	                foundEquivalent = true;
	                break;
	            }
	        }
	        if (!foundEquivalent) {
	            return false; // No equivalent condition found
	        }
	    }
	    return true; // All conditions match
	}
}
