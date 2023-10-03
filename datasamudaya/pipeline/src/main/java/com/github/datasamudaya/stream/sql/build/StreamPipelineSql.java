package com.github.datasamudaya.stream.sql.build;

import java.net.URI;
import java.util.function.IntSupplier;

import com.github.datasamudaya.stream.MapPair;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.StreamPipeline;

/**
 * The Streamed Pipeline SQL to trigger execution.
 * @author arun
 *
 */
public class StreamPipelineSql {
	Object mdpmp;

	public StreamPipelineSql(Object mdpmp) {
		this.mdpmp = mdpmp;
	}

	/**
	 * Collect method to trigger SQl query execution.
	 * @param toexecute
	 * @param supplier
	 * @return object
	 * @throws PipelineException
	 */
	@SuppressWarnings("rawtypes")
	public Object collect(boolean toexecute, IntSupplier supplier) throws PipelineException {
		if (mdpmp instanceof StreamPipeline mdp) {
			return mdp.collect(toexecute, supplier);
		}
		else if (mdpmp instanceof MapPair mp) {
			return mp.collect(toexecute, supplier);
		}
		return mdpmp;
	}
	
	/**
	 * Saves the output in text file.
	 * @param uri
	 * @param path
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public void  saveAsTextFile(URI uri, String path) throws Exception {
		if (mdpmp instanceof StreamPipeline mdp) {
			mdp.saveAsTextFile(uri, path);
		}
		else if (mdpmp instanceof MapPair mp) {
			mp.saveAsTextFile(uri, path);
		}

	}

}

