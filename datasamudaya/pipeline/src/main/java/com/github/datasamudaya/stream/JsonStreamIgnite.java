package com.github.datasamudaya.stream;

/**
 * This class is a stream with parametered types for pipeline api to process json files
 * in ignite server.
 * @author arun
 *
 * @param <I1>
 */
public final class JsonStreamIgnite<I1> extends IgnitePipeline<I1> {
	@SuppressWarnings({"rawtypes"})
	public JsonStreamIgnite(IgnitePipeline root) {
		this.root = root;
		this.task = new Json();
		root.childs.add(this);
		this.parents.add(root);
		this.protocol = root.protocol;
	}
}
