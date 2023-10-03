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

import org.junit.Test;

import com.github.datasamudaya.common.Resources;

import junit.framework.TestCase;

public class ResourcesTest extends TestCase {

	public static final String nodeport = "127.0.0.1:3000";
	public static final Long totalmemory = (long) (1024 * 1024 * 1024);
	public static final Long freememory = (long) (1024 * 1024 * 1024);
	public static final Integer numberofprocessors = 4;
	public static final Double totaldisksize = 1024 * 1024 * 1024 * 1024d;
	public static final Double usabledisksize = 1024 * 1024 * 1024 * 1024d;
	public static final Long physicalmemorysize = (long) (8 * 1024 * 1024 * 1024d);

	@Test
	public void testNodeport() {
		Resources resources = new Resources();
		resources.setNodeport(nodeport);
		assertEquals(nodeport, resources.getNodeport());
	}

	@Test
	public void testNodeportNull() {
		Resources resources = new Resources();
		resources.setNodeport(null);
		assertEquals(null, resources.getNodeport());
	}

	@Test
	public void testTotalMemory() {
		Resources resources = new Resources();
		resources.setTotalmemory(totalmemory);
		assertEquals(totalmemory, resources.getTotalmemory());
	}

	@Test
	public void testTotalMemoryNull() {
		Resources resources = new Resources();
		resources.setTotalmemory(null);
		assertEquals(null, resources.getTotalmemory());
	}

	@Test
	public void testFreeMemory() {
		Resources resources = new Resources();
		resources.setFreememory(freememory);
		assertEquals(freememory, resources.getFreememory());
	}

	@Test
	public void testFreeMemoryNull() {
		Resources resources = new Resources();
		resources.setFreememory(null);
		assertEquals(null, resources.getFreememory());
	}

	@Test
	public void testNumberofprocessors() {
		Resources resources = new Resources();
		resources.setNumberofprocessors(numberofprocessors);
		assertEquals(numberofprocessors, resources.getNumberofprocessors());
	}

	@Test
	public void testNumberofprocessorsNull() {
		Resources resources = new Resources();
		resources.setNumberofprocessors(null);
		assertEquals(null, resources.getNumberofprocessors());
	}


	@Test
	public void testTotaldisksize() {
		Resources resources = new Resources();
		resources.setTotaldisksize(totaldisksize);
		assertEquals(totaldisksize, resources.getTotaldisksize());
	}

	@Test
	public void testTotaldisksizeNull() {
		Resources resources = new Resources();
		resources.setTotaldisksize(null);
		assertEquals(null, resources.getTotaldisksize());
	}

	@Test
	public void testUsabledisksize() {
		Resources resources = new Resources();
		resources.setUsabledisksize(usabledisksize);
		assertEquals(usabledisksize, resources.getUsabledisksize());
	}


	@Test
	public void testUsabledisksizeNull() {
		Resources resources = new Resources();
		resources.setUsabledisksize(null);
		assertEquals(null, resources.getUsabledisksize());
	}

	@Test
	public void testPhysicalmemorysize() {
		Resources resources = new Resources();
		resources.setPhysicalmemorysize(physicalmemorysize);
		assertEquals(physicalmemorysize, resources.getPhysicalmemorysize());
	}

	@Test
	public void testPhysicalmemorysizeNull() {
		Resources resources = new Resources();
		resources.setPhysicalmemorysize(null);
		assertEquals(null, resources.getPhysicalmemorysize());
	}
}
