package com.github.datasamudaya.stream.sql.dataframe.build;

import java.util.function.Predicate;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

/**
 * The class converts Predicate to RexNode Converter
 * @author arun
 *
 */
public class PredicateToRexNodeConverter {

    private final RelBuilder relBuilder;

    public PredicateToRexNodeConverter(RelBuilder relBuilder) {
        this.relBuilder = relBuilder;
    }

    public RexNode convertPredicateToRexNode(Predicate predicate) {
        return convertPredicateToRexNodeRecursive(predicate);
    }

    /**
     * The function which converts predicate to rexnode recursively
     * @param predicate
     * @return
     */
    private RexNode convertPredicateToRexNodeRecursive(Predicate predicate) {
        if (predicate == null) {
            return null;
        }

        if (predicate instanceof AndPredicate andPredicate) {
            RexNode leftNode = convertPredicateToRexNodeRecursive(andPredicate.getLeftPredicate());
            RexNode rightNode = convertPredicateToRexNodeRecursive(andPredicate.getRightPredicate());
            return relBuilder.call(SqlStdOperatorTable.AND, leftNode, rightNode);
        } else if (predicate instanceof OrPredicate orPredicate) {
            RexNode leftNode = convertPredicateToRexNodeRecursive(orPredicate.getLeftPredicate());
            RexNode rightNode = convertPredicateToRexNodeRecursive(orPredicate.getRightPredicate());
            return relBuilder.call(SqlStdOperatorTable.OR, leftNode, rightNode);
        } else if (predicate instanceof NumericExpressionPredicate numericPredicate) {
            RexNode fieldRef = numericPredicate.getLeft() instanceof Column column?relBuilder.field(column.getName()):relBuilder.literal(((Literal)numericPredicate.getLeft()).getValue());
            RexNode literal = numericPredicate.getRight() instanceof Column column?relBuilder.field(column.getName()):relBuilder.literal(((Literal)numericPredicate.getRight()).getValue());
            switch (numericPredicate.getOperator()) {
                case EQUALS:
                    return relBuilder.call(SqlStdOperatorTable.EQUALS, fieldRef, literal);
                case GREATER_THAN:
                    return relBuilder.call(SqlStdOperatorTable.GREATER_THAN, fieldRef, literal);
                case LESS_THAN:
                    return relBuilder.call(SqlStdOperatorTable.LESS_THAN, fieldRef, literal);
                case LESS_THAN_EQUALS:
                    return relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, fieldRef, literal);
                case GREATER_THAN_EQUALS:
                    return relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal);
                // Add more cases as needed
                default:
                    throw new IllegalArgumentException("Unsupported operator: " + numericPredicate.getOperator());
            }
        } else {
            throw new IllegalArgumentException("Unsupported predicate type: " + predicate.getClass().getName());
        }
    }
}

