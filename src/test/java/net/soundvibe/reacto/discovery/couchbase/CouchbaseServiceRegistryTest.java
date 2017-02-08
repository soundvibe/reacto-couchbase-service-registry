package net.soundvibe.reacto.discovery.couchbase;

import com.couchbase.client.java.document.json.*;
import net.soundvibe.reacto.discovery.types.ServiceRecord;
import net.soundvibe.reacto.mappers.jackson.JacksonMapper;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.*;
import org.junit.Test;
import rx.Observable;

import static net.soundvibe.reacto.discovery.couchbase.CouchbaseServiceRegistry.json;
import static org.junit.Assert.*;

/**
 * @author Linas Naginionis
 */
public class CouchbaseServiceRegistryTest {

    private final ServiceRecord typedRecord = ServiceRecord.createWebSocketEndpoint(
            new ServiceOptions("service", "/", "1", false, 8181),
            CommandRegistry.ofTyped(String.class, String.class, Observable::just, new JacksonMapper(json)));

    private final ServiceRecord untypedRecord = ServiceRecord.createWebSocketEndpoint(
            new ServiceOptions("service", "/", "1", false, 8181),
            CommandRegistry.of("foo", command -> Observable.just(Event.create("bar"))));

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
        assertFalse(CouchbaseServiceRegistry.isCompatibleWith(
                Command.create("foo"),
                CouchbaseServiceRegistry.toCouchbaseObject(typedRecord)));

        assertTrue(CouchbaseServiceRegistry.isCompatibleWith(
                Command.create("foo"),
                CouchbaseServiceRegistry.toCouchbaseObject(untypedRecord)));

        assertFalse(CouchbaseServiceRegistry.isCompatibleWith(
                Command.create("bar"),
                CouchbaseServiceRegistry.toCouchbaseObject(untypedRecord)));
    }
}