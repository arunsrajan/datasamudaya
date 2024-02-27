package com.github.datasamudaya.stream.sql.dataframe.build;

import com.github.datasamudaya.common.PipelineConfig;

import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * DataFrameContext to build DataFrame
 * @author arun
 *
 */
public class DataFrameContext {
	PipelineConfig pipelineconfig;
	private DataFrameContext(PipelineConfig pipelineconfig) {
		this.pipelineconfig = pipelineconfig;
	}
	protected String folder;
	protected String[] columns;
	protected String tablename;
	protected List<SqlTypeName> types;
	protected String db;
	protected String fileformat;
	protected String hdfs;
	public static DataFrameContext newDataFrameContext(PipelineConfig pipelineconfig) {
		return new DataFrameContext(pipelineconfig);
	}
	public DataFrameContext setFolder(String folder) {
		this.folder = folder;
		return this;
	}
	public DataFrameContext setColumns(String[] columns) {
		this.columns = columns;
		return this;
	}
	public DataFrameContext setTablename(String tablename) {
		this.tablename = tablename;
		return this;
	}
	public DataFrameContext setTypes(List<SqlTypeName> types) {
		this.types = types;
		return this;
	}
	public DataFrameContext setDb(String db) {
		this.db = db;
		return this;
	}
	public DataFrameContext setFileFormat(String fileformat) {
		this.fileformat = fileformat;
		return this;
	}
	public DataFrameContext setHdfs(String hdfs) {
		this.hdfs = hdfs;
		return this;
	}
	public DataFrame build() {
		return new DataFrame(this); 
	}
}
