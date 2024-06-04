package com.github.datasamudaya.common;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * This class holds the column information from table.
 * @author arun
 *
 */
@ToString
@Getter
@EqualsAndHashCode
public class ColumnMetadata {

	private String columnName;
	private String dataType;
	private int columnSize;
	private String columnDefault;

	public ColumnMetadata(String columnName, String dataType, int columnSize, String columnDefault) {
		this.columnName = columnName;
		this.dataType = dataType;
		this.columnSize = columnSize;
		this.columnDefault = columnDefault;
	}

}
