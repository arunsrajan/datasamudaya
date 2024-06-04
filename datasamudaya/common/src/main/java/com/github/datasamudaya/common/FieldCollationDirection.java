package com.github.datasamudaya.common;

import org.apache.calcite.rel.RelFieldCollation.Direction;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The field collation with direction used for distributed sorting
 * @author arun
 *
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class FieldCollationDirection implements java.io.Serializable{
	private static final long serialVersionUID = -2012790776995115655L;
	private int fieldindex;
	private Direction direction;
}
