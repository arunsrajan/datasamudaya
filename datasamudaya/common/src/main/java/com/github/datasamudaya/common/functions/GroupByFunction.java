package com.github.datasamudaya.common.functions;

import java.io.Serializable;
import java.util.function.Function;

/**
 * This is an functional interface of group by
 * @author arun
 *
 * @param <T>
 * @param <R>
 */
@FunctionalInterface
public interface GroupByFunction <T, R> extends Function<T, R>,Serializable{

}
