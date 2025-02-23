package com.github.datasamudaya.common.utils.sql;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * The class stores join Keys for left and right tables for SQL JOINS 
 */
@Setter
@Getter
public class JoinKeysSQL {
	private final List<Integer> leftKeys = new ArrayList<>();
    private final List<Integer> rightKeys = new ArrayList<>();
}
