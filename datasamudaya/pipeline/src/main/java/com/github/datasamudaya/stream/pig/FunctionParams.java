package com.github.datasamudaya.stream.pig;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import org.apache.pig.newplan.logical.expression.LogicalExpression;
/**
 * POJO Object holding Function with params
 * @author arun
 *
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class FunctionParams {

	private String functionName;
	
	LogicalExpression params;
	
	private String alias;
	
}
