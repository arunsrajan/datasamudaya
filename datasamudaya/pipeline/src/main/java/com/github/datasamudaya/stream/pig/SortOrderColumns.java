package com.github.datasamudaya.stream.pig;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Column With Sort Order For PIG LOSort
 * @author arun
 *
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SortOrderColumns {

	private boolean isasc;

	private String column;
}
