package net.soundvibe.reacto.discovery.couchbase;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.view.*;
import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;
import net.soundvibe.reacto.client.events.CommandHandlerRegistry;
import net.soundvibe.reacto.discovery.types.ServiceRecord;
import net.soundvibe.reacto.mappers.jackson.JacksonMapper;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.*;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Linas Naginionis
 */
public class CouchbaseServiceRegistryTest {

    public static final String DEFAULT_BUCKET = "default";
    private final ServiceRecord typedRecord = ServiceRecord.createWebSocketEndpoint(
            new ServiceOptions("service", "/", "1", false, 8181),
            CommandRegistry.ofTyped(String.class, String.class, Flowable::just, new JacksonMapper(JacksonMapper.JSON)));

    private final ServiceRecord untypedRecord = ServiceRecord.createWebSocketEndpoint(
            new ServiceOptions("service", "/", "1", false, 8181),
            CommandRegistry.of("foo", command -> Flowable.just(Event.create("bar"))));


    public static CouchbaseContainer couchbase =
            new CouchbaseContainer()
                    .withKeyValue(true)
                    .withClusterUsername("Administrator")
                    .withClusterPassword("password")
                    .withFTS(false)
                    .withQuery(true)
                    .withIndex(true)
                    .withNewBucket(DefaultBucketSettings.builder()
                            .enableFlush(true)
                            .indexReplicas(false)
                            .name(DEFAULT_BUCKET)
                            .password(DEFAULT_BUCKET)
                            .replicas(0)
                            .quota(100)
                            .type(BucketType.COUCHBASE)
                            .build()
                    );

    static {
        couchbase.start();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        System.out.println("Getting cluster...");
        cluster = couchbase.getCouchbaseCluster();
        System.out.println("Creating default view...");
        getCouchbaseServiceRegistry().updateDefaultView(viewQuery.getDesign(), viewQuery.getView())
                .blockingSubscribe();
    }

    private static CouchbaseCluster cluster;

    private final static ViewQuery viewQuery = ViewQuery.from("reacto", "services").stale(Stale.FALSE);

    private CouchbaseServiceRegistry sut = getCouchbaseServiceRegistry();

    private final static ServiceRecord serviceRecord = ServiceRecord.createWebSocketEndpoint(
            new ServiceOptions("couchbase-test", "/", "1",
                    false, 8181),
            CommandRegistry.of("MakeDemo", command -> Flowable.just(Event.create("Made"))));

    @After
    @Before
    public void cleanDatabase() throws Exception {
        TestSubscriber<Any> subscriber = new TestSubscriber<>();
        sut.unpublish(serviceRecord).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
    }

    @Test
    public void shouldHaveDefaultBucket() throws InterruptedException {
        assertNotNull("Should have default bucket", cluster.openBucket(DEFAULT_BUCKET, DEFAULT_BUCKET));
    }

    @Test
    public void shouldFindPublishedRecord() throws Exception {
        TestSubscriber<Any> testSubscriber = new TestSubscriber<>();
        sut.publish().subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);

        TestSubscriber<ServiceRecord> subscriber = new TestSubscriber<>();
        sut.findRecords().subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertComplete();
        subscriber.assertValue(serviceRecord);

        TestSubscriber<List<ServiceRecord>> servicesSubscriber = new TestSubscriber<>();
        sut.findRecordsOf(Command.create("MakeDemo")).subscribe(servicesSubscriber);
        servicesSubscriber.awaitTerminalEvent();
        servicesSubscriber.assertNoErrors();
        servicesSubscriber.assertValue(Collections.singletonList(serviceRecord));
    }

    @Test
    public void shouldFindNothing() throws Exception {
        TestSubscriber<ServiceRecord> subscriber = new TestSubscriber<>();
        sut.findRecords().subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertComplete();
        subscriber.assertNoValues();
    }

    @Test
    public void shouldReturnEmptyListIfServiceNotFound() throws Exception {
        TestSubscriber<List<ServiceRecord>> subscriber = new TestSubscriber<>();
        sut.findRecordsOf(Command.create("Unknown")).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValue(Collections.emptyList());
    }

    @Test
    public void shouldUnPublishService() throws Exception {
        shouldFindPublishedRecord();

        TestSubscriber<Any> subscriber = new TestSubscriber<>();
        sut.unpublish(serviceRecord).subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

        shouldFindNothing();
    }

    @Test
    public void shouldUpdateService() throws Exception {
        shouldFindPublishedRecord();

        TestSubscriber<Any> subscriber = new TestSubscriber<>();
        sut.update().subscribe(subscriber);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);
    }

    @Test
    public void shouldRegister() throws Exception {
        TestSubscriber<Any> testSubscriber = new TestSubscriber<>();
        sut.register().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
    }

    @Test
    public void shouldUnregister() throws Exception {
        shouldRegister();

        TestSubscriber<Any> testSubscriber = new TestSubscriber<>();
        sut.unregister().subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
    }

    @Test
    public void shouldMapToCouchbaseObjectAndBack() throws Exception {
        JsonObject actual = CouchbaseServiceRegistry.toCouchbaseObject(typedRecord);

        assertEquals("service", actual.getString("name"));
        JsonArray commands = actual.getObject("metadata").getArray(ServiceRecord.METADATA_COMMANDS);
        assertEquals(1, commands.size());
        assertEquals(String.class.getName(), commands.getObject(0).getString(CommandDescriptor.COMMAND));
        assertEquals(String.class.getName(), commands.getObject(0).getString(CommandDescriptor.EVENT));

        assertEquals("/", actual.getObject("location").getString("root"));

        ServiceRecord actualRecord = CouchbaseServiceRegistry.toRecord(actual);
        assertEquals(typedRecord, actualRecord);
    }

    @Test
    public void shouldBeCompatibleWith() throws Exception {
        assertFalse(typedRecord.isCompatibleWith(Command.create("foo")));

        assertTrue(untypedRecord.isCompatibleWith(Command.create("foo")));

        assertFalse(untypedRecord.isCompatibleWith(Command.create("bar")));
    }

    private static CouchbaseServiceRegistry getCouchbaseServiceRegistry() {
        return new CouchbaseServiceRegistry(
                () -> cluster.openBucket(DEFAULT_BUCKET, DEFAULT_BUCKET), viewQuery, CommandHandlerRegistry.empty(),
                new JacksonMapper(JacksonMapper.JSON),
                serviceRecord, 15);
    }

}