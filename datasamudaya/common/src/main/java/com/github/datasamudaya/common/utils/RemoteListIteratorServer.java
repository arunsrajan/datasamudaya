package com.github.datasamudaya.common.utils;

import static java.util.Objects.nonNull;
import static java.util.Objects.isNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
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

import net.jpountz.lz4.LZ4BlockOutputStream;

/**
 * The class iterates the list from cache which is a remote server.
 * @author arun
 *
 * @param <T>
 */
public class RemoteListIteratorServer<T> {
	private Cache<String,byte[]> cache;
	private static final Logger log = LoggerFactory.getLogger(RemoteListIteratorServer.class);
	public RemoteListIteratorServer(Cache<String,byte[]> cache) {
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
							Object deserobj = kryo.readClassAndObject(input);
							if (deserobj instanceof RemoteListIteratorTask rlit) {
								task = rlit.getTask();
								lfcds = rlit.getFcsc();
								log.info("Obtaining Cache for File {}",task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid);
								baistream = new ByteArrayInputStream(cache.get(task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid));
								sis = new SnappyInputStream(baistream);
								inputfile = new Input(sis);
								log.info("Obtaining Cache {}", inputfile);
							} else if (deserobj instanceof RemoteListIteratorHasNext rlit) {
								boolean isavailable = inputfile.available() > 0 || nonNull(currentList) && indexperlist<currentList.size();
								kryo.writeClassAndObject(output, isavailable);
								output.flush();
								if (!isavailable) {
									break;
								}
							} else if (deserobj instanceof RemoteListIteratorNext rlin) {
								if(isNull(currentList) || indexperlist>currentList.size()) {
									currentList = (List) kryo.readClassAndObject(inputfile);
									indexperlist = 0;									
								}
								Object objfromfile = currentList.get(indexperlist);
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
								kryo.writeClassAndObject(output, nik);
								output.flush();
								indexperlist++;
								totindex++;
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
}
