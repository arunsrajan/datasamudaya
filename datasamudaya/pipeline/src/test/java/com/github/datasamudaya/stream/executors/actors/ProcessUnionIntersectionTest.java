package com.github.datasamudaya.stream.executors.actors;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.NodeIndexKeyComparator;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.Address;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;
import scala.collection.JavaConverters;

public class ProcessUnionIntersectionTest {
	private static ActorTestKit testKit;
	private static EntityRef<Command> processUnion;
	private static TestProbe<OutputObject> probe;
	private static Task task1;
	private static Task task2;
	private static Task task3;
	private static Task task4;
	private static Task task;
	private static Integer diskspillpercentage;
	private static ExecutorService es = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("ProcessUnionIntersectionTest-", 0).factory());
	EntityTypeKey<Command> entityKey;

	@BeforeClass
	public static void setUp() throws IOException, Exception {
		Utils.initializeProperties(
				DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH,
				DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE, DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		testKit = ActorTestKit.create(Utils.getAkkaSystemConfig(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST, DataSamudayaConstants.AKKA_HOST_DEFAULT),
				Utils.getRandomPort(), "1"));
		task = new Task();
		task.setJobid("Job-1");
		task.setStageid("Stage-1");
		task.setTaskid("Task");
		task1 = new Task();
		task1.setJobid("Job-1");
		task1.setStageid("Stage-1");
		task1.setTaskid("Task-1");
		task2 = new Task();
		task2.setJobid("Job-1");
		task2.setStageid("Stage-1");
		task2.setTaskid("Task-2");
		task3 = new Task();
		task3.setJobid("Job-1");
		task3.setStageid("Stage-1");
		task3.setTaskid("Task-3");
		task4 = new Task();
		task4.setJobid("Job-1");
		task4.setStageid("Stage-1");
		task4.setTaskid("Task-4");
		probe = testKit.createTestProbe();
		Cluster cluster = Cluster.get(testKit.system());
		Address address = cluster.selfMember().address();
		cluster.manager().tell(new Join(address));
		boolean up = false;
		Set<Member> members = JavaConverters.setAsJavaSetConverter(cluster.state().members()).asJava();
		while (members.size() < 1 || !up) {
			Thread.sleep(1000);
			members = JavaConverters.setAsJavaSetConverter(cluster.state().members()).asJava();
			if (members.size() < 1)
				continue;
			up = true;
			for (Member member : members) {
				if (member.status() != MemberStatus.up()) {
					up = false;
					break;
				}
			}
		}
	}

	@Test
	public void testProcessUnionWithDiskSpillingList() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingList diskspilllist = new DiskSpillingList(task1, diskspillpercentage, null, false, false, false,
				null, null, 1);
		OutputObject object = new OutputObject(diskspilllist, false, false, DiskSpillingList.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithDiskSpillingSet() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingSet diskspillingset = new DiskSpillingSet(task1, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator());
		OutputObject object = new OutputObject(diskspillingset, false, false, DiskSpillingSet.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithTreeSet() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(new TreeSet<>(), false, false, TreeSet.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithDummyClass() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(new Dummy(), false, false, Dummy.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithNodeIndexKeyClass() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(new NodeIndexKey(), false, false, NodeIndexKey.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithMultipleDiskSpillingList() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingList diskspilllist1 = new DiskSpillingList(task1, diskspillpercentage, null, false, false, false,
				null, null, 1);
		DiskSpillingList diskspilllist2 = new DiskSpillingList(task2, diskspillpercentage, null, false, false, false,
				null, null, 1);

		OutputObject object1 = new OutputObject(diskspilllist1, false, false, DiskSpillingList.class);
		OutputObject object2 = new OutputObject(diskspilllist2, false, false, DiskSpillingList.class);
		processUnion.tell(object1);
		processUnion.tell(object2);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithMultipleDiskSpillingSet() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingSet diskspillingset1 = new DiskSpillingSet(task1, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator());
		OutputObject object1 = new OutputObject(diskspillingset1, false, false, DiskSpillingSet.class);
		DiskSpillingSet diskspillingset2 = new DiskSpillingSet(task2, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator());
		OutputObject object2 = new OutputObject(diskspillingset1, false, false, DiskSpillingSet.class);
		processUnion.tell(object1);
		processUnion.tell(object2);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithMultipleTreeSet() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object1 = new OutputObject(new TreeSet<>(), false, false, TreeSet.class);
		OutputObject object2 = new OutputObject(new TreeSet<>(), false, false, TreeSet.class);
		processUnion.tell(object1);
		processUnion.tell(object2);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithMultipleDummyClass() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object1 = new OutputObject(new Dummy(), false, false, Dummy.class);
		OutputObject object2 = new OutputObject(new Dummy(), false, false, Dummy.class);
		processUnion.tell(object1);
		processUnion.tell(object2);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithMultipleNodeIndexKeyClass() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object1 = new OutputObject(new NodeIndexKey(), false, false, NodeIndexKey.class);
		OutputObject object2 = new OutputObject(new NodeIndexKey(), false, false, NodeIndexKey.class);
		processUnion.tell(object1);
		processUnion.tell(object2);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithMixedClasses() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 3, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingList diskspilllist1 = new DiskSpillingList(task1, diskspillpercentage, null, false, false, false,
				null, null, 1);
		OutputObject object1 = new OutputObject(diskspilllist1, false, false, DiskSpillingList.class);
		OutputObject object2 = new OutputObject(new DiskSpillingSet(task2, diskspillpercentage, null, false, false,
				false, null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class);
		OutputObject object3 = new OutputObject(new TreeSet<>(), false, false, TreeSet.class);
		processUnion.tell(object1);
		processUnion.tell(object2);
		processUnion.tell(object3);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithEmptyDiskSpillingList() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingList diskspilllist1 = new DiskSpillingList(task1, diskspillpercentage, null, false, false, false,
				null, null, 1);
		OutputObject object = new OutputObject(diskspilllist1, false, false, DiskSpillingList.class);
		((DiskSpillingList) object.getValue()).clear();
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithEmptyDiskSpillingSet() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingSet diskspillingset = new DiskSpillingSet(task1, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator());
		OutputObject object = new OutputObject(diskspillingset, false, false, DiskSpillingSet.class);
		((DiskSpillingSet) object.getValue()).clear();
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithEmptyTreeSet() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(new TreeSet<>(), false, false, TreeSet.class);
		((TreeSet<?>) object.getValue()).clear();
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithEmptyDummyClass() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(new Dummy(), false, false, Dummy.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithEmptyNodeIndexKeyClass() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(new NodeIndexKey(), false, false, NodeIndexKey.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithNonNullObject() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(new Object(), false, false, Dummy.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithNullObject() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		OutputObject object = new OutputObject(null, false, false, Dummy.class);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnionWithNullValue() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 1, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingSet diskspillingset = new DiskSpillingSet(task1, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator());
		OutputObject object = new OutputObject(diskspillingset, false, false, DiskSpillingSet.class);
		object.setValue(null);
		processUnion.tell(object);
		probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
	}

	@Test
	public void testProcessUnion() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingList diskspilllist1 = new DiskSpillingList(task1, diskspillpercentage, null, false, false, false,
				null, null, 1);
		Object[] object = ObjectArrayTestGenerator.generate(100);	
		diskspilllist1.addAll(Arrays.asList(object));
		
		OutputObject object1 = new OutputObject(diskspilllist1, false, false, DiskSpillingList.class);
		DiskSpillingList diskspilllist2 = new DiskSpillingList(task2, diskspillpercentage, null, false, false, false,
				null, null, 1);
		object = ObjectArrayTestGenerator.generate(100);	
		diskspilllist2.addAll(Arrays.asList(object));
		OutputObject object2 = new OutputObject(diskspilllist2, false, false, DiskSpillingList.class);
		processUnion.tell(object1);
		processUnion.tell(object2);
		OutputObject oo3 = probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
		assertTrue(oo3.getValue() instanceof DiskSpillingSet);
		assertTrue(((DiskSpillingSet)oo3.getValue()).size()<=200);
	}
	
	@Test
	public void testProcessUnionUnion() throws PipelineException, Exception {
		String name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		DiskSpillingList diskspilllist1 = new DiskSpillingList(task1, diskspillpercentage, null, false, false, false,
				null, null, 1);
		Object[] object = ObjectArrayTestGenerator.generate(100);	
		diskspilllist1.addAll(Arrays.asList(object));		
		OutputObject object1 = new OutputObject(diskspilllist1, false, false, DiskSpillingList.class);
		DiskSpillingList diskspilllist2 = new DiskSpillingList(task2, diskspillpercentage, null, false, false, false,
				null, null, 1);
		
		object = ObjectArrayTestGenerator.generate(100);	
		diskspilllist2.addAll(Arrays.asList(object));
		OutputObject object2 = new OutputObject(diskspilllist2, false, false, DiskSpillingList.class);
		
		DiskSpillingList diskspilllist3 = new DiskSpillingList(task3, diskspillpercentage, null, false, false, false,
				null, null, 1);
		object = ObjectArrayTestGenerator.generate(5);	
		diskspilllist3.addAll(Arrays.asList(object));
		OutputObject object3 = new OutputObject(diskspilllist3, false, false, DiskSpillingList.class);
		
		processUnion.tell(object1);
		processUnion.tell(object2);
		OutputObject oo3 = probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
		assertTrue(oo3.getValue() instanceof DiskSpillingSet);
		assertTrue(((DiskSpillingSet)oo3.getValue()).size()<=6);
		
		probe = testKit.createTestProbe();
		name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		processUnion.tell(object3);
		processUnion.tell(oo3);
		OutputObject oo4 = probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
		assertTrue(oo4.getValue() instanceof DiskSpillingSet);
		assertTrue(((DiskSpillingSet)oo4.getValue()).size()<=6);
	}
	
	
	@Test
	public void testProcessIntersectionUnion() throws PipelineException, Exception {
		String name = "ProcessIntersection-" + System.currentTimeMillis();
		entityKey = ProcessIntersection.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessIntersection.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task, Arrays.asList(probe.ref()), 2, es)));
		EntityRef<Command> processIntersection = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		
		DiskSpillingList diskspilllist1 = new DiskSpillingList(task1, diskspillpercentage, null, false, false, false,
				null, null, 1);
		Object[] object = ObjectArrayTestGenerator.generate(100);
		Arrays.<Object>sort(object, new ArrayComparator());
		diskspilllist1.addAll(Arrays.asList(object));		
		OutputObject object1 = new OutputObject(diskspilllist1, false, false, DiskSpillingList.class);
		
		DiskSpillingList diskspilllist2 = new DiskSpillingList(task2, diskspillpercentage, null, false, false, false,
				null, null, 1);		
		object = ObjectArrayTestGenerator.generate(100);
		Arrays.<Object>sort(object, new ArrayComparator());
		diskspilllist2.addAll(Arrays.asList(object));
		OutputObject object2 = new OutputObject(diskspilllist2, false, false, DiskSpillingList.class);
		
		DiskSpillingSet diskspillset3 = new DiskSpillingSet(task3, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new ArrayComparator());
		object = ObjectArrayTestGenerator.generate(5);
		Arrays.<Object>sort(object, new ArrayComparator());
		diskspillset3.addAll(Arrays.asList(object));
		OutputObject object3 = new OutputObject(diskspillset3, false, false, DiskSpillingList.class);
		
		processIntersection.tell(object1);
		processIntersection.tell(object2);
		OutputObject oo3 = probe.expectMessage(new OutputObject(new DiskSpillingSet(task, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
		assertTrue(oo3.getValue() instanceof DiskSpillingSet);
		assertTrue(((DiskSpillingSet)oo3.getValue()).size()<=6);
		
		probe = testKit.createTestProbe();
		name = "ProcessUnion-" + System.currentTimeMillis();
		entityKey = ProcessUnion.createTypeKey(name);
		ClusterSharding.get(testKit.system()).init(Entity.of(entityKey, ctx -> ProcessUnion.create(ctx.getEntityId(),
				null, null, new ConcurrentHashMap<>(), task4, Arrays.asList(probe.ref()), 2, es)));
		processUnion = ClusterSharding.get(testKit.system()).entityRefFor(entityKey, name);
		int totalrec = (((DiskSpillingSet)object3.getValue()).size()+ ((DiskSpillingSet)oo3.getValue()).size());
		processUnion.tell(object3);
		processUnion.tell(oo3);
		OutputObject oo4 = probe.expectMessage(new OutputObject(new DiskSpillingSet(task4, diskspillpercentage, null, false, false, false,
				null, null, 1, true, new NodeIndexKeyComparator()), false, false, DiskSpillingSet.class));
		assertTrue(oo4.getValue() instanceof DiskSpillingSet);		
		assertTrue(((DiskSpillingSet)oo4.getValue()).size()<=totalrec);
	}

	@AfterClass
	public static void cleanup() {
		testKit.shutdownTestKit();
	}
}
