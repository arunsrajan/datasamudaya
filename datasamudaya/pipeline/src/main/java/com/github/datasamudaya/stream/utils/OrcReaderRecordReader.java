package com.github.datasamudaya.stream.utils;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class which holds orc related objects
 * @author arun
 *
 */
@Getter
@AllArgsConstructor
public class OrcReaderRecordReader {
	private Reader reader;
	private RecordReader rows;
	private Stream<Map<String, Object>> valuesmapstream;
}
