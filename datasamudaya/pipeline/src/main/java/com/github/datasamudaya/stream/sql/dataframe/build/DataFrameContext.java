package com.github.datasamudaya.stream.sql.dataframe.build;

import com.github.datasamudaya.common.PipelineConfig;

import java.util.ArrayList;
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
		folder = new ArrayList<>();
		columns = new ArrayList<>();
		tablename = new ArrayList<>();
		types = new ArrayList<>();
	}
	protected List<String> folder;
	protected List<String[]> columns;
	protected List<String> tablename;
	protected List<List<SqlTypeName>> types;
	protected String db;
	protected String fileformat;
	protected String hdfs;

	public static DataFrameContext newDataFrameContext(PipelineConfig pipelineconfig) {
		return new DataFrameContext(pipelineconfig);
	}

	public DataFrameContext addTable(String folder, String[] columns, String tablename, List<SqlTypeName> types) {
		this.folder.add(folder);
		this.columns.add(columns);
		this.tablename.add(tablename);
		this.types.add(types);
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
