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

import static org.junit.Assert.assertEquals;
import java.util.List;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.utils.Utils;

public class PipelineConfigValidatorTest {

	@Test
	public void testBlockSizeNumberError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setBlocksize("BlockSize");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.BLOCKSIZENUMBER, errormsgs.get(0));
	}

	@Test
	public void testBlockSizeRangeErrorPositiveGreaterThan256() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setBlocksize("512");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.BLOCKSIZERANGE, errormsgs.get(0));
	}

	@Test
	public void testBatchSizeNumberError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setBatchsize("BlockSize");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.BATCHSIZENUMBER, errormsgs.get(0));
	}

	@Test
	public void testMesosTrueFalseError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setMesos("OtherThanTrueFalse");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.MESOSTRUEFALSE, errormsgs.get(0));
	}

	@Test
	public void testYARNTrueFalseError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setYarn("OtherThanTrueFalse");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.YARNTRUEFALSE, errormsgs.get(0));
	}

	@Test
	public void testLocalTrueFalseError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setLocal("OtherThanTrueFalse");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.LOCALTRUEFALSE, errormsgs.get(0));
	}

	@Test
	public void testJGroupsTrueFalseError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setJgroups("OtherThanTrueFalse");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.JGROUPSTRUEFALSE, errormsgs.get(0));
	}

	@Test
	public void testOneModeSetError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setJgroups("true");
		config.setYarn("true");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.ERRORJGROUPSYARNLOCALMESOSSET, errormsgs.get(0));
	}

	@Test
	public void testMinMemValueNotNumberError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setMinmem("NotNumeric");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.MINMEMNUMBER, errormsgs.get(0));
	}

	@Test
	public void testMaxMemValueNotNumberError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setMaxmem("NotNumeric");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.MAXMEMNUMBER, errormsgs.get(0));
	}

	@Test
	public void testGCError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setGctype("NotAValidGcType");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.GCTYPEERROR, errormsgs.get(0));
	}

	@Test
	public void testNumContError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setNumberofcontainers("NoANumber");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.NUMCONTERROR, errormsgs.get(0));
	}

	@Test
	public void testMesosUrlNotSetError() throws Exception {
		PipelineConfigValidator configvalidator = new PipelineConfigValidator();
		PipelineConfig config = getPipelineConfig();
		config.setLocal("false");
		config.setMesos("true");
		config.setMesosmaster("");
		List<String> errormsgs = configvalidator.validate(config);
		assertEquals(PipelineConfigValidator.MESOSMASTERNOTSETERROR, errormsgs.get(0));
	}

	public PipelineConfig getPipelineConfig() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "datasamudayatest.properties");
		PipelineConfig config = new PipelineConfig();
		return config;
	}

}
