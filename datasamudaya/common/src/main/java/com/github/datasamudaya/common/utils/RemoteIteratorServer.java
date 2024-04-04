package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.Task;

/**
 * The class iterates the list from cache which is a remote server.
 * @author arun
 *
 * @param <T>
 */
public class RemoteIteratorServer<T> {
	private Cache<String,byte[]> cache;
	private static final Logger log = LoggerFactory.getLogger(RemoteIteratorServer.class);
	public RemoteIteratorServer(Cache<String,byte[]> cache) {
		this.cache = cache;
	}

	public Tuple2<ServerSocket,ExecutorService> start() {
		ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		try {
			ServerSocket serverSocket = new ServerSocket(0);
			log.info("Server listening on port {}" , serverSocket.getLocalPort());
			es.execute(() -> {
				while(true) {
				final Socket clientSocket;
				try {
					if(serverSocket.isClosed()) {
						break;
					}
					clientSocket = serverSocket.accept();
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
					continue;
				}
				log.info("Client connected");
				es.execute(() -> {
					Kryo kryo = Utils.getKryo();
					InputStream baistream = null;
					SnappyInputStream sis = null;
					Input inputfile = null;
					List currentList = null;
					Task task = null;
					List<FieldCollationDirection> lfcds = null;
					try (Socket socket = clientSocket;
							InputStream istream = socket.getInputStream();
							Input input = new Input(istream);
							OutputStream ostream = socket.getOutputStream();
							Output output = new Output(ostream);) {						
						int indexperlist = 0;
						int totindex = 0;
						log.info("Input Stream and Output Stream Obtained");
						while (true) {
							if(socket.isClosed()) {
								break;
							}
							Object deserobj = Utils.getKryo().readClassAndObject(input);
							if (deserobj instanceof RemoteListIteratorTask rlit) {
								task = rlit.getTask();
								lfcds = rlit.getFcsc();
								log.info("Obtaining Cache for File {} with FCD {}",task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid, lfcds);
								byte[] bt = cache.get(task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid);
								baistream = nonNull(bt)?new ByteArrayInputStream(bt):new FileInputStream(Utils.getLocalFilePathForTask(task, null, false, false, false));
								sis = new SnappyInputStream(baistream);
								inputfile = new Input(sis);
								log.info("Obtaining Input {}", baistream);
								indexperlist = 0;
								totindex = 0;
							} else if (deserobj instanceof RemoteListIteratorHasNext rlit) {
								boolean isavailable = inputfile.available() > 0 || nonNull(currentList) && indexperlist<currentList.size();
								kryo.writeClassAndObject(output, isavailable);
								output.flush();
								if (!isavailable) {
									break;
								}
							} else if (deserobj instanceof RemoteListIteratorNext rlin) {
								if(isNull(currentList) || indexperlist>=currentList.size()) {
									Object data = kryo.readClassAndObject(inputfile);
									currentList = (List) ((data instanceof Set)?new ArrayList<>((Collection)data):(List)data);
									indexperlist = 0;									
								}
								if (rlin.getRequestType() == RequestType.ELEMENT) {
									Object objfromfile = currentList.get(indexperlist);
									kryo.writeClassAndObject(output, getObjectToNik(lfcds, totindex, task, objfromfile));
									output.flush();
									indexperlist++;
									totindex++;
								} else if (rlin.getRequestType() == RequestType.LIST) {
							        // Send the entire list
									List<NodeIndexKey> niks = new ArrayList<>();
									for(Object obj:currentList) {
										niks.add(getObjectToNik(lfcds, totindex, task, obj));
										totindex++;
									}
							        kryo.writeClassAndObject(output, Utils.convertObjectToBytesCompressed(niks, null));
							        output.flush();
							        indexperlist = currentList.size(); // Ensure we mark as done
							    }
							}
						}
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					} finally {
						if(nonNull(inputfile)) {
							inputfile.close();
						}
						if(nonNull(sis)) {
							try {
								sis.close();
							} catch (IOException e) {
								
							}
						}
						if(nonNull(baistream)) {
							try {
								baistream.close();
							} catch (IOException e) {
								
							}
						}
					}
				});
			}
			});
			return new Tuple2<ServerSocket,ExecutorService>(serverSocket, es);
		} catch (Exception e) {
			log.error(DataSamudayaConstants.EMPTY, e);
		}
		return null;
	}
	
	protected NodeIndexKey getObjectToNik(List<FieldCollationDirection> lfcds, int totindex, Task task, Object objfromfile) {
		NodeIndexKey nik = new NodeIndexKey();
		if(objfromfile instanceof Tuple2 tup2) {
			nik.setIndex(totindex);
			nik.setNode(task.getHostport());
			if(nonNull(lfcds)) {
				nik.setKey(Utils.getKeyFromNodeIndexKey(lfcds, (Object[]) tup2.v1()));
			} else {
				nik.setValue(objfromfile);
			}
		} else {
			nik.setIndex(totindex);
			nik.setNode(task.getHostport());
			if(nonNull(lfcds)) {
				nik.setKey(Utils.getKeyFromNodeIndexKey(lfcds, (Object[]) objfromfile));
			}
			else {
				nik.setValue(objfromfile);
			}
		}
		return nik;
	}
	
}
