package com.github.datasamudaya.stream;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CsvOptionsSQL extends CsvOptions {
	private static final long serialVersionUID = -5592994943064203479L;
	
	public CsvOptionsSQL(String[] header, List<SqlTypeName> types, List<String> requiredcolumns) {
		super(header);
		this.types.addAll(types);
		this.requiredcolumns.addAll(requiredcolumns);
	}
	List<SqlTypeName> types = new ArrayList<>();
	List<String> requiredcolumns = new ArrayList<>();
}
