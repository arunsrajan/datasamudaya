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

import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.lang.ref.SoftReference;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.NioInetPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;


/**
 * 
 * @author 
 * Arun HDFS Block Reader for data locality
 */
public class HdfsBlockReader {

	private static final Logger log = Logger.getLogger(HdfsBlockReader.class);

	/**
	 * This method gets the data in bytes from hdfs given the blocks location.
	 * @param bl
	 * @param hdfs
	 * @return byte array 
	 * @throws Exception
	 */
	public static byte[] getBlockDataMR(final BlocksLocation bl, FileSystem hdfs) throws Exception {
		try {
			log.debug("Entered HdfsBlockReader.getBlockDataMR");
			var baos = new ByteArrayOutputStream();
			var mapfilenamelb = new HashMap<String, List<LocatedBlock>>();
			for (var block : bl.getBlock()) {
				log.debug("In getBlockDataMR block: " + block);
				if (!Objects.isNull(block) && Objects.isNull(mapfilenamelb.get(block.getFilename()))) {
					try (var fsinput = (HdfsDataInputStream) hdfs.open(new Path(block.getFilename()));) {
						mapfilenamelb.put(block.getFilename(), new ArrayList<>(fsinput.getAllBlocks()));
					}
				}
				if (!Objects.isNull(block)) {
					var locatedBlocks = mapfilenamelb.get(block.getFilename());
					for (var lb : locatedBlocks) {
						if (lb.getStartOffset() == block.getBlockOffset()) {
							log.debug("Obtaining Data for the " + block + " with offset: " + lb.getStartOffset());
							getDataBlock(block, lb, hdfs, baos, block.getHp().split(DataSamudayaConstants.UNDERSCORE)[0]);
							break;
						}
					}
				}
			}
			// Data bytes for processing
			baos.flush();
			var byt = baos.toByteArray();
			var srbaos = new SoftReference<ByteArrayOutputStream>(baos);
			srbaos.clear();
			baos = null;
			log.debug("Exiting HdfsBlockReader.getBlockDataMR");
			return byt;
		} catch (Exception ex) {
			log.error("Unable to Obtain Block Data getBlockDataMR: ", ex);
		}

		return null;

	}

	/**
	 * This method gets the data in bytes.
	 * @param block
	 * @param lb
	 * @param hdfs
	 * @param baos
	 * @param containerhost
	 * @return byte array
	 */
	public static byte[] getDataBlock(Block block, LocatedBlock lb, FileSystem hdfs, OutputStream baos,
			String containerhost) {
		log.debug("Entered HdfsBlockReader.getDataBlock");
		int totalbytestoread = (int) (block.getBlockend() - block.getBlockstart());
		try (var breader = getBlockReader((DistributedFileSystem) hdfs, lb, lb.getStartOffset() + block.getBlockstart(), containerhost);) {
			log.debug("In getDataBlock Read Bytes: " + totalbytestoread);
			var readsize = 4096;
			var byt = new byte[readsize];

			var sum = 0;
			// Number of bytes to read.
			if (breader.available() > 0) {
				log.debug("In getDataBlock BlockReader: " + breader.getNetworkDistance());
				while (breader.available() > 0) {
					var read = breader.read(byt, 0, readsize);
					if (read == -1) {
						break;
					}
					// If bytes chunk read are less than or equal to
					// total number of bytes to read for processing.
					if (sum + read < totalbytestoread) {
						baos.write(byt, 0, read);
						baos.flush();
					} else {
						baos.write(byt, 0, (totalbytestoread - sum));
						baos.flush();
						break;
					}
					sum += read;
				}
			}
			log.debug("Exiting HdfsBlockReader.getDataBlock");
		} catch (Exception ex) {
			ex.printStackTrace();
			log.error("Unable to Obtain Block Data: ", ex);
		}

		return null;
	}


	/**
	 * 
	 * This function returns compressed data stream using LZF compression.
	 * @param bl
	 * @param hdfs
	 * @return
	 * @throws Exception
	 */
	public static InputStream getBlockDataInputStream(final BlocksLocation bl, FileSystem hdfs) throws Exception {
		try {
			var inputstreams = new ArrayList<BlockReaderInputStream>();
			log.debug("Entered HdfsBlockReader.getBlockDataSnappyStream");
			for (var block : bl.getBlock()) {
				if (nonNull(block)) {
					log.debug("Obtaining Data for the " + block + " with offset: " + block.getBlockOffset());
					FSDataInputStream dfsis = hdfs.open(new Path(block.getFilename()));
					BlockReaderInputStream bris = new BlockReaderInputStream(dfsis, (long) (block.getBlockOffset() + block.getBlockstart()),
							block.getBlockend() - block.getBlockstart());
					inputstreams.add(bris);
				}
			}
			log.debug("Exiting HdfsBlockReader.getBlockDataSnappyStream");
			return new SequenceInputStream(Collections.enumeration(inputstreams));
		} catch (Exception ex) {
			log.error("Unable to Obtain Block Data getBlockDataSnappyStream: ", ex);
		}

		return null;

	}

	/**
	 * The Merged InputStream
	 * @param bl
	 * @param hdfs
	 * @return inputstream object
	 * @throws Exception
	 */
	public static InputStream getBlockDataInputStreamMerge(final BlocksLocation bl, FileSystem hdfs) throws Exception {
		try {
			log.debug("Entered HdfsBlockReader.getBlockDataInputStreamMerge");
			var block = bl.getBlock();
			log.debug("Obtaining Data for the " + block + " with offset: " + block[0].getBlockOffset());
			FSDataInputStream dfsis = hdfs.open(new Path(block[0].getFilename()));
			long blocklimit = block[0].getBlockend() - block[0].getBlockstart();
			if (block.length > 1 && nonNull(block[1])) {
				blocklimit += (block[1].getBlockend() - block[1].getBlockstart());
			}
			BlockReaderInputStream bris = new BlockReaderInputStream(dfsis,
					(long) (block[0].getBlockOffset() + block[0].getBlockstart()), blocklimit);
			log.debug("Exiting HdfsBlockReader.getBlockDataInputStreamMerge");
			return bris;
		} catch (Exception ex) {
			log.error("Unable to Obtain Block Data getBlockDataInputStreamMerge: ", ex);
		}

		return null;

	}

	/**
	 * To calculate the total amount of bytes required;
	 * @param block
	 * @return
	 */
	public static long calculateBytesRequired(Block[] block) {
		long totalmemoryrequired = (long) (block[0].getBlockend() - block[0].getBlockstart());
		if (block.length > 1 && nonNull(block[1])) {
			totalmemoryrequired += block[1].getBlockend() - block[1].getBlockstart();
		}
		return totalmemoryrequired;
	}

	/**
	 * The block reader for reading block information.
	 * @param fs
	 * @param lb
	 * @param offset
	 * @param xrefaddress
	 * @return block reader object.
	 * @throws IOException
	 */
	public static BlockReader getBlockReader(final DistributedFileSystem fs, LocatedBlock lb, long offset,
			String xrefaddress) throws IOException {
		log.debug("Entered HdfsBlockReader.getBlockReader");
		InetSocketAddress targetAddr = null;
		var eblock = lb.getBlock();
		var nodes = lb.getLocations();
		var dninfos = Arrays.asList(nodes);
		var dnaddress = dninfos.stream().filter(dninfo -> dninfo.getXferAddr().contains(xrefaddress))
				.findFirst();
		DatanodeInfo dninfo;
		if (dnaddress.isEmpty()) {
			targetAddr = NetUtils.createSocketAddr(nodes[0].getXferAddr());
			dninfo = nodes[0];
		} else {
			targetAddr = NetUtils.createSocketAddr(dnaddress.get().getXferAddr());
			dninfo = dnaddress.get();
		}
		var offsetIntoBlock = offset - lb.getStartOffset();
		log.debug("Extended Block Num Bytes: " + eblock.getNumBytes());
		log.debug("Offset Within Block: " + offsetIntoBlock);
		log.debug("Xref Address Address: " + dninfo);
		log.debug("Target Address: " + targetAddr);
		fs.getConf().setBoolean("dfs.client.read.shortcircuit", true);
		fs.getConf().setBoolean("dfs.client.use.legacy.blockreader.local", true);
		var dfsClientConf = new DfsClientConf(fs.getConf());
		var clientContext = ClientContext.get("DATASAMUDAYAContext", dfsClientConf, fs.getConf());
		log.debug("Use Legacy Block Reader Local: " + clientContext.getUseLegacyBlockReaderLocal());
		log.debug("Using Legacy Block Reader: " + dfsClientConf.getShortCircuitConf().isUseLegacyBlockReaderLocal());
		log.debug("Using Legacy Block Reader Local: "
				+ dfsClientConf.getShortCircuitConf().isUseLegacyBlockReaderLocal());
		log.debug("Exiting HdfsBlockReader.getBlockReader");
		return new BlockReaderFactory(dfsClientConf).setInetSocketAddress(targetAddr).setBlock(eblock)
				.setFileName(targetAddr.toString() + DataSamudayaConstants.COLON + eblock.getBlockId())
				.setBlockToken(lb.getBlockToken()).setStartOffset(offsetIntoBlock).setLength(lb.getBlockSize() - offsetIntoBlock)
				.setVerifyChecksum(false).setClientName(DataSamudayaConstants.DATASAMUDAYA).setDatanodeInfo(dninfo)
				.setClientCacheContext(clientContext).setCachingStrategy(CachingStrategy.newDefaultStrategy())
				.setConfiguration(fs.getConf())
				.setStorageType(StorageType.DISK).setAllowShortCircuitLocalReads(false)
				.setRemotePeerFactory(new RemotePeerFactory() {

					public Peer newConnectedPeer(InetSocketAddress addr, Token<BlockTokenIdentifier> blockToken,
							DatanodeID datanodeId) throws IOException {
						Peer peer = null;
						var sock = NetUtils.getDefaultSocketFactory(fs.getConf()).createSocket();
						try {
							sock.connect(addr, 100);
							sock.setSoTimeout(100);
							peer = new NioInetPeer(sock);
						} finally {
							if (peer == null) {
								IOUtils.closeQuietly(sock);
							}
						}
						return peer;
					}
				}).build();
	}

	private HdfsBlockReader() {
	}
}
