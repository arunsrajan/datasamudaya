package com.github.datasamudaya.stream.sql;

import javax.servlet.*;
import javax.servlet.http.*;

import org.springframework.ai.chat.ChatResponse;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.ollama.api.OllamaOptions;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class SQLServlet extends HttpServlet {
	private static final long serialVersionUID = -5673438665011903754L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String task = request.getParameter("task");
		String tableName = request.getParameter("table");
		String numQueriesStr = request.getParameter("numQueries");
		String sqlStr = request.getParameter("sql");
		String numInsightsStr = request.getParameter("numInsights");
		String user = request.getParameter("user");

		// Set response content type
		response.setContentType("text/plain");
		PrintWriter out = response.getWriter();

		// If generating a single query (Page 1)
		if (task.equalsIgnoreCase(DataSamudayaConstants.GENERATESQLTASK)) {
			if (tableName != null && !tableName.isEmpty()) {
				out.println(generateSingleSQL(user, tableName));
			} else {
				out.println("Error: Table name is required.");
			}
		} else if (task.equalsIgnoreCase(DataSamudayaConstants.GENERATEMULTIPLESQLTASK)) {
			try {
				int numQueries = Integer.parseInt(numQueriesStr);
				if (tableName != null && !tableName.isEmpty() && numQueries > 0) {
					out.println(generateMultipleSQL(user, tableName, numQueries));
				} else {
					out.println("Error: Table name and number of queries are required.");
				}
			} catch (NumberFormatException e) {
				out.println("Error: Invalid number of queries.");
			}
		} else if (task.equalsIgnoreCase(DataSamudayaConstants.GENERATEINSIGHTSSQLTASK)) {
			try {
				int numInsights = Integer.parseInt(numInsightsStr);
				if (sqlStr != null && numInsights > 0) {
					out.println(generateInsightsSQL(sqlStr, numInsights));
				} else {
					out.println("Error: Sql and number of insights are required.");
				}
			} catch (NumberFormatException e) {
				out.println("Error: Invalid number of insights.");
			}
		}
	}

	// Method to generate a single SQL query
	private String generateSingleSQL(String user, String tableName) {
		try {
			String dbdefault = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SQLDB,
					DataSamudayaConstants.SQLMETASTORE_DB);
			var columns = new ArrayList<ColumnMetadata>();
			TableCreator.getColumnMetadataFromTable(user, dbdefault, tableName, columns);
			List<String> columnsNames = columns.stream().map(ColumnMetadata::getColumnName)
					.collect(Collectors.toList());
			String query = String.format(DataSamudayaConstants.SQL_QUERY_AGG_PROMPT, tableName,
					columnsNames.toString());
			ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
					OllamaOptions.create()
							.withTemperature(Float.parseFloat(DataSamudayaProperties.get().getProperty(
									DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
									DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
							.withModel(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.OLLAMA_MODEL_NAME,
									DataSamudayaConstants.OLLAMA_MODEL_DEFAULT))));
			return response.getResult().getOutput().getContent().replace(DataSamudayaConstants.NEWLINE, DataSamudayaConstants.BR);
		} catch (Exception ex) {

		}
		return DataSamudayaConstants.EMPTY;
	}

	// Method to generate multiple SQL queries
	private String generateMultipleSQL(String user, String tableName, int numQueries) {
		try {
			String dbdefault = DataSamudayaProperties.get()
					.getProperty(DataSamudayaConstants.SQLDB, DataSamudayaConstants.SQLMETASTORE_DB);
			var columns = new ArrayList<ColumnMetadata>();
			TableCreator.getColumnMetadataFromTable(user, dbdefault, tableName, columns);
			List<String> columnsNames = columns.stream().map(ColumnMetadata::getColumnName).collect(Collectors.toList());
			String query = String.format(DataSamudayaConstants.SQL_QUERY_MUL_AGG_PROMPT, numQueries, tableName, columnsNames.toString());
			ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
					OllamaOptions.create()
							.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
									getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
									DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
							.withModel(DataSamudayaProperties.get().
									getProperty(DataSamudayaConstants.OLLAMA_MODEL_NAME,
									DataSamudayaConstants.OLLAMA_MODEL_DEFAULT))));
			return response.getResult().getOutput().getContent().replace(DataSamudayaConstants.NEWLINE, DataSamudayaConstants.BR);
		} catch (Exception ex) {

		}
		return DataSamudayaConstants.EMPTY;
	}

	// Method to generate multiple SQL queries
	private String generateInsightsSQL(String sql, int numInsightsToGenerate) {
		try {
			String formattedquery = String.format(DataSamudayaConstants.SQL_QUERY_INFERENCE_PROMPT, numInsightsToGenerate, sql);
			ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(formattedquery),
					OllamaOptions.create()
							.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
									getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
									DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
							.withModel(DataSamudayaProperties.get().
									getProperty(DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME,
									DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME_DEFAULT))));
			return response.getResult().getOutput().getContent().replace(DataSamudayaConstants.NEWLINE, DataSamudayaConstants.BR);
		} catch (Exception ex) {

		}
		return DataSamudayaConstants.EMPTY;
	}
}
