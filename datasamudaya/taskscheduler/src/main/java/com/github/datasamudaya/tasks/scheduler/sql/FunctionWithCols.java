package com.github.datasamudaya.tasks.scheduler.sql;

import java.io.Serializable;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FunctionWithCols implements Serializable {
	private static final long serialVersionUID = -5020165830300971425L;
	private String name;
	private String alias;
	private String tablename;
	private List<String> parameters;
}
