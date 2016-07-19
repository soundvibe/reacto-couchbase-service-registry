package io.vertx.servicediscovery.backend.couchbase;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.ServiceReference;
import io.vertx.servicediscovery.types.HttpEndpoint;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * @author OZY on 2016.07.15.
 */
public class CouchbaseBackendServiceTest {

    @Test
    @Ignore
    public void shouldPublish() throws Exception {
        final Vertx vertx = Vertx.vertx();

        final ServiceDiscovery serviceDiscovery = ServiceDiscovery.create(vertx,
                new ServiceDiscoveryOptions()
                    .setBackendConfiguration(new JsonObject()
                        .put("nodes", new JsonArray().add("**"))
                            .put("bucketName", "***")
                            .put("pwd", "****")
                    )
        );

        //CountDownLatch countDownLatch1 = new CountDownLatch(1);
        serviceDiscovery.getRecords(record -> true, event -> {
            if (event.succeeded()) {
                System.out.println("Records: " + event.result().stream()
                        .peek(record -> serviceDiscovery.unpublish(record.getRegistration(), event1 -> {
                            System.out.println("Unpublished: " + event1.succeeded());
                            if (event1.failed()) {
                                System.out.println("Unpublished error: " + event1.cause().toString());
                            }
                        }))
                        .map(record -> record.toJson().toString())
                        .collect(Collectors.joining(",", "{", "}")));
            }
            //countDownLatch1.countDown();

        });
        //countDownLatch1.await();


        CountDownLatch countDownLatch = new CountDownLatch(1);

        serviceDiscovery.publish(HttpEndpoint.createRecord("test", "localhost", 8181, "services"),
                event -> {
                    System.out.println("Published " + event.succeeded());
                    if (event.failed()) {
                        System.out.println("Publish error: " + event.cause());
                    }
                    countDownLatch.countDown();
                });

        countDownLatch.await();

        AtomicReference<HttpClient> reference = new AtomicReference<>();
        CountDownLatch countDownLatch2 = new CountDownLatch(1);

        HttpEndpoint.getClient(serviceDiscovery, new JsonObject().put("name", "test"),
                event -> {
                    reference.set(event.result());
                    countDownLatch2.countDown();
                });

        countDownLatch2.await();
        final HttpClient actual = reference.get();
        assertNotNull(actual);

        assertEquals(1, serviceDiscovery.bindings().size());


        CountDownLatch countDownLatch3 = new CountDownLatch(serviceDiscovery.bindings().size());

        serviceDiscovery.bindings().stream()
                .peek(serviceReference -> serviceDiscovery.unpublish(serviceReference.record().getRegistration(), event -> {
                    System.out.println("Succeeded with unpublish: " + event.succeeded());
                    countDownLatch3.countDown();
                }))
                .forEach(ServiceReference::release);

        countDownLatch3.await();
        assertEquals(0, serviceDiscovery.bindings().size());



        serviceDiscovery.close();
    }
}