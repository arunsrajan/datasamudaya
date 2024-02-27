package com.github.datasamudaya.stream.sql.dataframe.build;

import java.util.function.Predicate;

public class NumericExpressionPredicate implements Predicate {
    private final NumericOperator operator;
    private final Object left;
    private final Object right;

    public NumericExpressionPredicate(NumericOperator operator, Object left,  Object right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    public NumericOperator getOperator() {
        return operator;
    }

    public Object getLeft() {
        return left;
    }
    
    public Object getRight() {
        return right;
    }

    @Override
    public boolean test(Object myData) {
        switch (operator) {
            case EQUALS:
                return left == right;            
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }
}