/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.common.utils.sql;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;

public class SimpleTableStatistic implements Statistic {

	private final long rowCount;

	public SimpleTableStatistic(long rowCount) {
		this.rowCount = rowCount;
	}

	@Override
	public Double getRowCount() {
		return (double) rowCount;
	}

	@Override
	public boolean isKey(ImmutableBitSet columns) {
		return false;
	}

	@Override
	public List<ImmutableBitSet> getKeys() {
		return Collections.emptyList();
	}

	@Override
	public List<RelReferentialConstraint> getReferentialConstraints() {
		return Collections.emptyList();
	}

	@Override
	public List<RelCollation> getCollations() {
		return Collections.emptyList();
	}

	@Override
	public RelDistribution getDistribution() {
		return RelDistributionTraitDef.INSTANCE.getDefault();
	}
}
