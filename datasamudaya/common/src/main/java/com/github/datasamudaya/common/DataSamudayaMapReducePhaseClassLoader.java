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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * 
 * @author Arun
 * Classloader to load jar bytes dynamically for loading classes.
 */
public class DataSamudayaMapReducePhaseClassLoader extends ClassLoader {

	private final byte[] jarBytes;
	private final Set<String> names;
	private static final Logger log = Logger.getLogger(DataSamudayaMapReducePhaseClassLoader.class);
	@SuppressWarnings("rawtypes")
	private static List instances = new ArrayList();

	@SuppressWarnings({"unchecked"})
	public static DataSamudayaMapReducePhaseClassLoader newInstance(byte[] jarBytes, ClassLoader parent) throws IOException {
		DataSamudayaMapReducePhaseClassLoader clsloader = new DataSamudayaMapReducePhaseClassLoader(jarBytes, parent);
		instances.add(clsloader);
		return clsloader;
	}

	public byte[] getJarBytes() {
		return this.jarBytes;
	}

	@SuppressWarnings("rawtypes")
	static List getInstances() {
		return instances;
	}

	private DataSamudayaMapReducePhaseClassLoader(byte[] jarBytes, ClassLoader parent) throws IOException {
		super(parent);
		this.jarBytes = jarBytes;
		this.names = DataSamudayaMapReducePhaseClassLoader.loadNames(jarBytes);
	}

	/**
	 * Loads all the classes names using jar entries.
	 * @param jarBytes
	 * @return
	 * @throws IOException
	 */
	private static Set<String> loadNames(byte[] jarBytes) throws IOException {
		Set<String> set = new HashSet<>();
		try (ZipInputStream jis = new ZipInputStream(new ByteArrayInputStream(jarBytes))) {
			ZipEntry entry;
			while ((entry = jis.getNextEntry()) != null) {
				set.add(entry.getName().replace(DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.EMPTY + File.separatorChar));
			}
		}
		return Collections.unmodifiableSet(set);
	}

	/**
	 * Finds the classes if not already the classes are loaded.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Class<?> findClass(String name) throws ClassNotFoundException {
		String file = name.replace(DataSamudayaConstants.DOT, File.separatorChar) + DataSamudayaConstants.DOT + DataSamudayaConstants.CLASS;
		byte[] b = null;
		try {

			InputStream is = getResourceAsStream(file);
			if (is != null) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				//Copy the .class file from file stream to obtain bytes array.
				IOUtils.copy(is, baos);
				b = baos.toByteArray();
				//Get the class from .class file which is obtained as bytes from jar file
				Class c = defineClass(name, b, 0, b.length);
				resolveClass(c);
				return c;
			}
			return null;
		} catch (Exception ex) {
			log.error("ClassLoader findClass error, See Cause below: \n", ex);
			return null;
		}
	}

	/**
	 * Get resource such as class information from jar bytes.
	 */
	@Override
	public InputStream getResourceAsStream(String name) {

		if (!names.contains(name)) {
			return null;
		}

		boolean found = false;
		ZipInputStream jis = null;
		try {
			jis = new ZipInputStream(new ByteArrayInputStream(jarBytes));
			ZipEntry entry;
			while ((entry = jis.getNextEntry()) != null) {
				if (entry.getName().replace(DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.EMPTY + File.separatorChar).equals(name)) {
					found = true;
					return jis;
				}
			}
		} catch (Exception ex) {
			log.error("ClassLoader get zip entry error, See Cause below: \n", ex);
		} finally {

			if (jis != null && !found) {
				try {
					jis.close();
				} catch (Exception ex) {
					log.error("ClassLoader zip stream close error, See Cause below: \n", ex);
				}
			}
		}
		return null;
	}

}
