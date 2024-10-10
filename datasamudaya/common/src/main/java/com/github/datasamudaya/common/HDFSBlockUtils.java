/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.utils.Utils;

/**
 * 
 * @author arun
 * The helper or utility class to obtain the blocks information with multiple datanode location.
 */
public class HDFSBlockUtils {

	private HDFSBlockUtils() {
	}
	static Logger log = LoggerFactory.getLogger(HDFSBlockUtils.class);

	private static final Semaphore lock = new Semaphore(1);

	/**
	 * This function returns list of blocks location using the block size obtained from HDFS.
	 * @param hdfs
	 * @param filepaths
	 * @return list of blocks information with multiple location from HDFS.
	 * @throws Exception
	 */
	public static List<BlocksLocation> getBlocksLocation(FileSystem hdfs, List<Path> filepaths,
			List<String> columns)
			throws Exception {
		lock.acquire();
		var blocklocationsl = new Vector<BlocksLocation>();
		filepaths.parallelStream().forEachOrdered(filepath -> {
			try {
				long starttime = System.currentTimeMillis();
				try (var hdis = (HdfsDataInputStream) hdfs.open(filepath);) {
					var locatedblocks = hdis.getAllBlocks();
					int lbindex = 0;
					var lb = locatedblocks.get(lbindex);
					var dinfoa = lb.getLocations();
					var dninfos = Arrays.asList(dinfoa);
					log.debug("In getBlocksLocation dninfos TimeTaken {}",
							(System.currentTimeMillis() - starttime) / 1000.0);
					var skipbytes = 0l;
					while (true) {
						var bls = new BlocksLocation();
						bls.setBlockid(Utils.getUUID());
						bls.setColumns(columns);
						var block = new Block[2];
						block[0] = new Block();
						block[0].setBlockstart(skipbytes);
						block[0].setBlockend(lb.getBlockSize());
						block[0].setBlockOffset(lb.getStartOffset());
						block[0].setFilename(filepath.toUri().toString());
						Map<String, Set<String>> dnxref = dninfos.stream().map(dninfo -> dninfo.getXferAddr()).collect(
								Collectors.groupingBy(xrefaddr -> xrefaddr.split(DataSamudayaConstants.COLON)[0], Collectors
										.mapping(xrefaddr -> xrefaddr, Collectors.toCollection(HashSet::new))));
						block[0].setDnxref(dnxref);
						bls.setBlock(block);
						blocklocationsl.add(bls);
						skipbytes = 0l;
						log.debug("In getBlocksLocation skipbytes TimeTaken {}",
								(System.currentTimeMillis() - starttime) / 1000.0);
						boolean isnewline = isNewLineAtEnd(hdfs, lb, lb.getStartOffset() + block[0].getBlockend() - 1,
								dninfos.get(0).getXferAddr());
						log.debug("In getBlocksLocation isnewline TimeTaken {}",
								(System.currentTimeMillis() - starttime) / 1000.0);
						if (!isnewline && lbindex < locatedblocks.size() - 1) {
							log.debug(
									"In getBlocksLocation lbindex < locatedblocks.size TimeTaken {}",
									(System.currentTimeMillis() - starttime) / 1000.0);
							lbindex++;
							lb = locatedblocks.get(lbindex);
							dinfoa = lb.getLocations();
							dninfos = Arrays.asList(dinfoa);
							skipbytes = skipBlockToNewLine(hdfs, lb, lb.getStartOffset(),
									dninfos.get(0).getXferAddr());
							if (skipbytes > 0) {
								bls = blocklocationsl.get(blocklocationsl.size() - 1);
								bls.getBlock()[1] = new Block();
								bls.getBlock()[1].setBlockstart(0);
								bls.getBlock()[1].setBlockend(skipbytes);
								bls.getBlock()[1].setBlockOffset(lb.getStartOffset());
								bls.getBlock()[1].setFilename(filepath.toUri().toString());
								bls.getBlock()[1].setDnxref(dninfos.stream().map(dninfo -> dninfo.getXferAddr())
										.collect(Collectors.groupingBy(
												xrefaddr -> xrefaddr.split(DataSamudayaConstants.COLON)[0],
												Collectors.mapping(xrefaddr -> xrefaddr,
														Collectors.toCollection(HashSet::new)))));
							}
						} else if (lbindex < locatedblocks.size() - 1) {
							lbindex++;
							lb = locatedblocks.get(lbindex);
						} else {
							break;
						}
						log.debug("In getBlocksLocation blockslocations TimeTaken {}",
								(System.currentTimeMillis() - starttime) / 1000.0);
					}
					var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
					log.debug("In getBlocksLocation TimeTaken {}", timetaken);
				}
			} catch (Exception ex) {
				log.error("Blocks Unavailable due to error", ex);
			}

		});
		lock.release();
		return blocklocationsl;
	}

	/**
	 * This function returns the number of bytes to skip to new line from currrent blocks offset given local datanode xref address. 
	 * @param hdfs
	 * @param lblock
	 * @param l
	 * @param xrefaddress
	 * @return offset to skip bytes to new line. 
	 * @throws Exception
	 */
	public static synchronized long skipBlockToNewLine(FileSystem hdfs, LocatedBlock lblock, long l, String xrefaddress) throws Exception {
		log.debug("Entered HDFSBlockUtils.skipBlockToNewLine");
		var read1byt = new byte[1];
		var blockReader = HdfsBlockReader.getBlockReader((DistributedFileSystem) hdfs, lblock, l, xrefaddress);
		var skipbytes = 0;
		long starttime = System.currentTimeMillis();
		if (blockReader.available() > 0) {
			read1byt[0] = 0;
			while (blockReader.available() > 0) {
				var bytesread = blockReader.read(read1byt, 0, 1);
				if (bytesread == 0 || bytesread == -1) {
					break;
				}
				if (read1byt[0] == '\n') {
					skipbytes += 1;
					break;
				}
				skipbytes += 1;
			}
		}
		blockReader.close();
		var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.debug("In skipBlockToNewLine TimeTaken {}", timetaken);
		log.debug("Exiting HDFSBlockUtils.skipBlockToNewLine");
		return skipbytes;
	}

	/**
	 * Check whether the blocks has newline at the end.
	 * @param hdfs
	 * @param lblock
	 * @param l
	 * @param xrefaddress
	 * @return Returns true if the block has newline at the end of the file else false.
	 * @throws Exception
	 */
	public static boolean isNewLineAtEnd(FileSystem hdfs, LocatedBlock lblock, long l, String xrefaddress) throws Exception {
		long starttime = System.currentTimeMillis();
		log.debug("Entered HDFSBlockUtils.skipBlockToNewLine");
		var read1byt = new byte[1];
		var blockReader = HdfsBlockReader.getBlockReader((DistributedFileSystem) hdfs, lblock, l, xrefaddress);

		boolean isnewlineatend = false;
		//if (blockReader.available() > 0) {
		
				var bytesread = blockReader.read(read1byt, 0, 1);

		if (bytesread == 0 || bytesread == -1) {
			isnewlineatend = false;
		}
		//return true if newline character else false if not
		if (read1byt[0] == '\n') {
			isnewlineatend = true;
		} else {
			isnewlineatend = false;
		}

		//}
		//blockReader.close();
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.debug("In isNewLineAtEnd TimeTaken {}", timetaken);
		return isnewlineatend;
	}
}
