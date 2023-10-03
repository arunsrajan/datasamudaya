package com.github.datasamudaya.stream.sql.build;

import java.net.URI;
import java.util.function.IntSupplier;

import com.github.datasamudaya.stream.IgnitePipeline;
import com.github.datasamudaya.stream.MapPair;
import com.github.datasamudaya.stream.MapPairIgnite;
import com.github.datasamudaya.stream.PipelineException;

/**
 * The Ignite Pipeline SQL to trigger execution.
 * @author arun
 *
 */
public class IgnitePipelineSql {
	Object ip;

	public IgnitePipelineSql(Object ip) {
		this.ip = ip;
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
		if (ip instanceof IgnitePipeline mdp) {
			return mdp.collect(toexecute, supplier);
		}
		else if (ip instanceof MapPairIgnite mp) {
			return mp.collect(toexecute, supplier);
		}
		return ip;
	}
	
	/**
	 * Saves the output in text file.
	 * @param uri
	 * @param path
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public void  saveAsTextFile(URI uri, String path) throws Exception {
		if (ip instanceof IgnitePipeline mdp) {
			mdp.saveAsTextFile(uri, path);
		}
		else if (ip instanceof MapPairIgnite mp) {
			mp.saveAsTextFile(uri, path);
		}

	}

}

