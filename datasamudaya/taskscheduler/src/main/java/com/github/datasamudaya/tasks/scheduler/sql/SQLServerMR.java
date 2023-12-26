package com.github.datasamudaya.tasks.scheduler.sql;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.sql.TableCreator;

/**
 * Sql server to process queries.
 * @author arun
 *
 */
public class SQLServerMR {
	static Logger log = LoggerFactory.getLogger(SQLServerMR.class);
	static ServerSocket serverSocket;
	
	/**
	 * Start the SQL server.
	 * @throws Exception
	 */
	public static void start() throws Exception {		
		ExecutorService executors = Executors.newFixedThreadPool(10);
		serverSocket = new ServerSocket(Integer.valueOf(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.SQLPORTMR, DataSamudayaConstants.SQLPORTMR_DEFAULT)));
		executors.execute(() -> {
			while (true) {
				Socket sock;
				try {
					sock = serverSocket.accept();
					executors.execute(() -> {
						String user = "";
						int numberofcontainers = 1;
						int cpupercontainer = 1;
						int memorypercontainer = 1024;
						String scheduler = "";
						String teappid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueAppID();
						boolean isignite = false;
						boolean isyarn = false;
						boolean iscontainerlaunched = false;
						boolean isyarncontainerlaunched = false;
						try (Socket clientSocket = sock;
								PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
								BufferedReader in = new BufferedReader(
										new InputStreamReader(clientSocket.getInputStream()));) {
							user = in.readLine();
							numberofcontainers = Integer.valueOf(in.readLine());
							cpupercontainer = Integer.valueOf(in.readLine());							
							memorypercontainer = Integer.valueOf(in.readLine());
							scheduler = in.readLine();
							if (!Utils.isUserExists(user)) {
								String usernotexistsmessage = "User " + user + " is not configured. Exiting...";
								out.println(usernotexistsmessage);
								out.println("Quit");
								throw new Exception(usernotexistsmessage);
							}
							List<LaunchContainers> containers = null;
							Map<String, Object> cpumemory = null;
							if (scheduler.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_DEFAULT) 
									|| scheduler.equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
								teappid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueAppID();
								containers = Utils.launchContainersUserSpec(user, teappid, cpupercontainer, memorypercontainer, numberofcontainers);
								cpumemory = Utils.getAllocatedContainersResources(containers);
								out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS) + " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " mb");
								Utils.printNodesAndContainers(containers, out);
								iscontainerlaunched = true;
							} else if(scheduler.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_IGNITE)) {
								iscontainerlaunched = false;
								isignite = true;
								isyarn = false;
							} else if(scheduler.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_YARN)) {
								iscontainerlaunched = false;
								isignite = false;
								isyarn = true;
								if (!isyarncontainerlaunched) {
									try {
										teappid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueAppID();
										Utils.launchYARNExecutors(teappid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.CONTEXT_FILE_CLIENT);
									} catch (Exception ex) {
										log.error(DataSamudayaConstants.EMPTY, ex);
									}
									isyarncontainerlaunched = true;
								}
							}
							out.println("Welcome to the Map Reduce SQL Server!");
							out.println("Type 'quit' to exit.");
							out.println("Done");							
							String inputLine;							
							String dbdefault = DataSamudayaProperties.get()
									.getProperty(DataSamudayaConstants.SQLDB, DataSamudayaConstants.SQLMETASTORE_DB);
							outer:
							while (true) {
								try {
									while ((inputLine = in.readLine()) != null) {
										if ("quit".equalsIgnoreCase(inputLine)) {
											out.println("Quit");
											break outer;
										}
										out.println(Utils.formatDate(new Date()) + "> " + inputLine);
										inputLine = inputLine.trim();
										if (inputLine.startsWith("setmode")) {
											String[] mode = inputLine.split(" ");
											if (mode.length == 2) {
												if (mode[1].equalsIgnoreCase(DataSamudayaConstants.MODE_DEFAULT)) {
													isignite = true;
													isyarn = false;
													if (isyarncontainerlaunched) {
														try {
															Utils.shutDownYARNContainer(teappid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														isyarncontainerlaunched = false;
													}
													if (iscontainerlaunched) {
														try {
															Utils.destroyContainers(user, teappid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														iscontainerlaunched = false;
													}
													out.println("ignite mode set");
												} else if (mode[1].equalsIgnoreCase(DataSamudayaConstants.YARN)) {
													isignite = false;
													isyarn = true;
													if (iscontainerlaunched) {
														try {
															Utils.destroyContainers(user, teappid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														iscontainerlaunched = false;
													}
													if (!isyarncontainerlaunched) {
														try {
															teappid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueAppID();
															Utils.launchYARNExecutors(teappid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.CONTEXT_FILE_CLIENT);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														isyarncontainerlaunched = true;
													}
													out.println("yarn mode set");
												} else {
													isignite = false;
													isyarn = false;
													if (isyarncontainerlaunched) {
														try {
															Utils.shutDownYARNContainer(teappid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														isyarncontainerlaunched = false;
													}
													if (!iscontainerlaunched) {
														teappid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueAppID();
														containers = Utils.launchContainersUserSpec(user, teappid, cpupercontainer, memorypercontainer, numberofcontainers);
														cpumemory = Utils.getAllocatedContainersResources(containers);
														iscontainerlaunched = true;
														out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS) + " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " mb");
														Utils.printNodesAndContainers(containers, out);
														iscontainerlaunched = true;
													}
													out.println("yarn and ignite mode unset");
												}
											}
										} else if (inputLine.startsWith("getmode")) {
											if (isignite) {
												out.println("ignite");
											} else if (isyarn) {
												out.println("yarn");
											} else {
												out.println("standalone");
											}
										} else if (inputLine.startsWith("use")) {
											dbdefault = StringUtils.normalizeSpace(inputLine.trim()).split(" ")[1];
										} else if (inputLine.startsWith("getdb")) {
											out.println(dbdefault);
										} else if (inputLine.startsWith("create") 
												|| inputLine.startsWith("alter")) {
											out.println(TableCreator.createAlterTable(dbdefault, inputLine));
										} else if (inputLine.startsWith("drop")) {
											out.println(TableCreator.dropTable(dbdefault, inputLine));
										} else if (inputLine.startsWith("show")) {
											Utils.printTableOrError(TableCreator.showTables(dbdefault, inputLine), out, JOBTYPE.NORMAL);
										} else if (inputLine.startsWith("describe")) {
											var columns = new ArrayList<ColumnMetadata>();
											TableCreator.getColumnMetadataFromTable(dbdefault, inputLine.split(" ")[1], columns);
											for (ColumnMetadata colmetadata : columns) {
												out.println(colmetadata);
											}
										} else if (inputLine.startsWith("select")) {
											long starttime = System.currentTimeMillis();
											String appid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueAppID();
											List results = null; 
											if (isignite) {
												results = SelectQueryExecutorMR.executeSelectQueryIgnite(dbdefault, inputLine, user, appid, teappid);
											} else {
												results = SelectQueryExecutorMR.executeSelectQuery(dbdefault, inputLine, user, appid, teappid, isyarn);
											}
											double timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
											int partitionno = 1;
											out.println("Partition" + partitionno);
											Utils.printTableOrError(results, out, JOBTYPE.NORMAL);
											partitionno++;
											out.println("Time taken " + timetaken + " seconds");
											out.println("");
										} else {
											out.println("Enter use,getdb,create,alter,drop,show,describe,select,setmode to execute");
										}
										out.println("Done");
									}
								} catch (SocketException socketexception) {
									log.error(DataSamudayaConstants.EMPTY, socketexception);
									break outer;
								} catch (Exception exception) {
									log.error(DataSamudayaConstants.EMPTY, exception);
								}
							}							
						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);							
						} finally {
							if (iscontainerlaunched) {
								try {
									Utils.destroyContainers(user, teappid);
								} catch (Exception ex) {
									log.error(DataSamudayaConstants.EMPTY, ex);
								}
							}
							if (isyarncontainerlaunched) {
								try {
									Utils.shutDownYARNContainer(teappid);
								} catch (Exception ex) {
									log.error(DataSamudayaConstants.EMPTY, ex);
								}
								isyarncontainerlaunched = false;
							}
						}
					});
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
			}
		});
	}

	private SQLServerMR() {
	}
}
