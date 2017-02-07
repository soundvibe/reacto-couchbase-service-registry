package net.soundvibe.reacto.discovery.couchbase;

import com.couchbase.client.java.document.json.*;
import net.soundvibe.reacto.discovery.types.ServiceRecord;
import net.soundvibe.reacto.mappers.jackson.JacksonMapper;
import net.soundvibe.reacto.server.*;
import net.soundvibe.reacto.types.CommandDescriptor;
import org.junit.Test;
import rx.Observable;

import static net.soundvibe.reacto.discovery.couchbase.CouchbaseServiceRegistry.json;
import static org.junit.Assert.assertEquals;

/**
 * @author Linas Naginionis
 */
public class CouchbaseServiceRegistryTest {

    @Test
    public void shouldMapToCouchbaseObjectAndBack() throws Exception {
        ServiceRecord expectedRecord = ServiceRecord.createWebSocketEndpoint(
                new ServiceOptions("expectedRecord", "/", "1", false, 8181),
                CommandRegistry.ofTyped(String.class, String.class, Observable::just, new JacksonMapper(json)));
        JsonObject actual = CouchbaseServiceRegistry.toCouchbaseObject(expectedRecord);

        assertEquals("expectedRecord", actual.getString("name"));
        JsonArray commands = actual.getObject("metadata").getArray(ServiceRecord.METADATA_COMMANDS);
        assertEquals(1, commands.size());
        assertEquals(String.class.getName(), commands.getObject(0).getString(CommandDescriptor.COMMAND));
        assertEquals(String.class.getName(), commands.getObject(0).getString(CommandDescriptor.EVENT));

        assertEquals("/", actual.getObject("location").getString("root"));

        ServiceRecord actualRecord = CouchbaseServiceRegistry.toRecord(actual);
        assertEquals(expectedRecord, actualRecord);
    }
}