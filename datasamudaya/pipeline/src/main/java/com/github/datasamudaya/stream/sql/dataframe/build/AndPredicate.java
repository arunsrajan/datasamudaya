package com.github.datasamudaya.stream.sql.dataframe.build;

import java.util.function.Predicate;

/**
 * The And Predicate class
 * @author Administrator
 *
 */
public class AndPredicate implements Predicate {
    private final Predicate leftPredicate;
    private final Predicate rightPredicate;

    public AndPredicate(Predicate leftPredicate, Predicate rightPredicate) {
        this.leftPredicate = leftPredicate;
        this.rightPredicate = rightPredicate;
    }

    public Predicate getLeftPredicate() {
        return leftPredicate;
    }

    public Predicate getRightPredicate() {
        return rightPredicate;
    }

    @Override
    public boolean test(Object myData) {
        return leftPredicate.test(myData) && rightPredicate.test(myData);
    }
}