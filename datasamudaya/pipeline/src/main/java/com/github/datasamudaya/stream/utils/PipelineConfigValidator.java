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
package com.github.datasamudaya.stream.utils;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;

/**
 * Properties validator for streams.
 * @author arun
 *
 */
public class PipelineConfigValidator implements ConfigValidator {

	public static final String BLOCKSIZENUMBER = "The block size should be number";
	public static final String BLOCKSIZERANGE = "The block size should greater than 1 and less than 128";
	public static final String BATCHSIZENUMBER = "The batch size should be number";
	public static final String MESOSTRUEFALSE = "Mesos mode config must be set to true or false";
	public static final String YARNTRUEFALSE = "Yarn mode config must be set to true or false";
	public static final String LOCALTRUEFALSE = "Local mode config must be set to true or false";
	public static final String JGROUPSTRUEFALSE = "JGroups mode config must be set to true or false";
	public static final String ERRORJGROUPSYARNLOCALMESOSSET = "Only one mode needs to set for session either JGROUPS or YARN or LOCAL or MESOS";
	public static final String MINMEMNUMBER = "The minimum heap size should be number";
	public static final String MAXMEMNUMBER = "The maximum heap size should be number";
	public static final String GCTYPEERROR = "The GC Type should be either G1GC (-XX:+UseG1GC) or ZGC (-XX:+UseZGC)";
	public static final String NUMCONTERROR = "The numnber of containers should be positive number";
	public static final String MESOSMASTERNOTSETERROR = "The mesos master url ([mesoshost]:[mesosport]) needs to set";

	/**
	 * Validates with configuration and returns the errors if any.
	 */
	@Override
	public List<String> validate(PipelineConfig pc) {
		var errormessage = new ArrayList<String>();
		var value = pc.getBlocksize();
		if (!StringUtils.isNumeric(value)) {
			errormessage.add(BLOCKSIZENUMBER);
		} else {
			long i = Long.parseLong(value);
			if (i < 1 || i > 256) {
				errormessage.add(BLOCKSIZERANGE);
			}
		}
		value = pc.getBatchsize();
		if (!StringUtils.isNumeric(value)) {
			errormessage.add(BATCHSIZENUMBER);
		}
		var mesos = pc.getMesos();
		var yarn = pc.getYarn();
		var local = pc.getLocal();
		var jgroups = pc.getJgroups();
		var ismodetruefalseerror = false;
		if (!("true".equalsIgnoreCase(mesos) || "false".equalsIgnoreCase(mesos))) {
			ismodetruefalseerror = true;
			errormessage.add(MESOSTRUEFALSE);
		}
		if (!("true".equalsIgnoreCase(yarn) || "false".equalsIgnoreCase(yarn))) {
			ismodetruefalseerror = true;
			errormessage.add(YARNTRUEFALSE);
		}
		if (!("true".equalsIgnoreCase(local) || "false".equalsIgnoreCase(local))) {
			ismodetruefalseerror = true;
			errormessage.add(LOCALTRUEFALSE);
		}
		if (!("true".equalsIgnoreCase(jgroups) || "false".equalsIgnoreCase(jgroups))) {
			ismodetruefalseerror = true;
			errormessage.add(JGROUPSTRUEFALSE);
		}
		if (!(!ismodetruefalseerror && (
				("true".equalsIgnoreCase(yarn) && "false".equalsIgnoreCase(mesos)
						&& "false".equalsIgnoreCase(local) && "false".equalsIgnoreCase(jgroups))
						|| ("false".equalsIgnoreCase(yarn) && "true".equalsIgnoreCase(mesos)
						&& "false".equalsIgnoreCase(local) && "false".equalsIgnoreCase(jgroups))
						|| ("false".equalsIgnoreCase(yarn) && "false".equalsIgnoreCase(mesos)
						&& "true".equalsIgnoreCase(local) && "false".equalsIgnoreCase(jgroups))
						|| ("false".equalsIgnoreCase(yarn) && "false".equalsIgnoreCase(mesos)
						&& "false".equalsIgnoreCase(local) && "true".equalsIgnoreCase(jgroups))
						|| ("false".equalsIgnoreCase(yarn) && "false".equalsIgnoreCase(mesos)
						&& "false".equalsIgnoreCase(local) && "false".equalsIgnoreCase(jgroups))
		))) {
			errormessage.add(ERRORJGROUPSYARNLOCALMESOSSET);
		}
		value = pc.getMinmem();
		if (!StringUtils.isNumeric(value)) {
			errormessage.add(MINMEMNUMBER);
		}
		value = pc.getMaxmem();
		if (!StringUtils.isNumeric(value)) {
			errormessage.add(MAXMEMNUMBER);
		}
		value = pc.getGctype();
		if (!(value.equals(DataSamudayaConstants.ZGC) || value.equals(DataSamudayaConstants.G1GC))) {
			errormessage.add(GCTYPEERROR);
		}
		value = pc.getNumberofcontainers();
		if (!StringUtils.isNumeric(value)) {
			errormessage.add(NUMCONTERROR);
		}
		value = pc.getMesosmaster();
		if (!ismodetruefalseerror && "true".equalsIgnoreCase(mesos) && "".equals(value.trim())) {
			errormessage.add(MESOSMASTERNOTSETERROR);
		}
		return errormessage;
	}

}
