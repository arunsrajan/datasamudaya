package com.github.datasamudaya.stream.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

public class ORCRecordSpliterator extends java.util.Spliterators.AbstractSpliterator<Map<String, Object>> {
    private final RecordReader recordReader;
    private final TypeDescription schema;
    VectorizedRowBatch batch;
    public ORCRecordSpliterator(RecordReader recordReader, TypeDescription schema,
    		VectorizedRowBatch batch) {
        super(Long.MAX_VALUE, Spliterator.ORDERED);
        this.recordReader = recordReader;
        this.schema = schema;
        this.batch = batch;
    }
    public Object getValueFromVector(int index, ColumnVector cv) {
		if (cv instanceof LongColumnVector lcv) {
			return Long.valueOf(lcv.vector[index]);
		} else if (cv instanceof BytesColumnVector bcv) {
			return bcv.toString(index);
		}
		return null;
	}
    @Override
    public boolean tryAdvance(Consumer<? super Map<String, Object>> action) {
        try {
            if (!recordReader.nextBatch(batch)) {
                return false; // No more records
            }

            // Read the next record into the row array
            for (int r = 0; r < batch.size; ++r) {
            	Map<String, Object> record = new HashMap<>();
                for (int i = 0; i < schema.getMaximumId(); i++) {
                    String fieldName = schema.getFieldNames().get(i);
                    Object value = getValueFromVector(r, batch.cols[i]);
                    record.put(fieldName, value);
                }
                action.accept(record);
			}            
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}