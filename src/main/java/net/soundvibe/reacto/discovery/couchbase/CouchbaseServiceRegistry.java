/*
 * Copyright (c) 2011-2016 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package net.soundvibe.reacto.discovery.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.datastructures.MutationOptionBuilder;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.*;
import com.fasterxml.jackson.databind.*;
import net.soundvibe.reacto.client.events.EventHandlerRegistry;
import net.soundvibe.reacto.discovery.*;
import net.soundvibe.reacto.discovery.types.*;
import net.soundvibe.reacto.mappers.ServiceRegistryMapper;
import net.soundvibe.reacto.mappers.jackson.JacksonMapper;
import net.soundvibe.reacto.types.*;
import net.soundvibe.reacto.utils.Scheduler;
import org.slf4j.*;
import rx.Observable;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static net.soundvibe.reacto.types.CommandDescriptor.*;

/**
 * An implementation of the reacto service registry based on Couchbase.
 *
 * @author Linas Naginionis
 */
public final class CouchbaseServiceRegistry extends AbstractServiceRegistry implements ServiceDiscoveryLifecycle {

    private final static Logger log = LoggerFactory.getLogger(CouchbaseServiceRegistry.class);

    public static final String DEFAULT_RECORDS_KEY = "service-discovery-records";
    public static final ServiceRecord DEFAULT_SERVICE_RECORD =
            ServiceRecord.create("UNKNOWN", Status.UNKNOWN, ServiceType.LOCAL, UUID.randomUUID().toString(),
                    net.soundvibe.reacto.types.json.JsonObject.empty(),
                    net.soundvibe.reacto.types.json.JsonObject.empty());

    public static final int DEFAULT_HEARTBEAT_INTERVAL_IN_SECONDS = 60;

    public static final ObjectMapper json = new ObjectMapper();

    private final Supplier<Bucket> bucketSupplier;
    private final String recordsKey;
    private final ServiceRecord serviceRecord;
    private final AtomicBoolean isOpen = new AtomicBoolean(false);
    private final AtomicReference<Timer> timer = new AtomicReference<>();
    private final JsonObject serviceObject;
    private final int heartBeatIntervalInSeconds;
    private final MutationOptionBuilder builder;

    public CouchbaseServiceRegistry(
            Supplier<Bucket> bucketSupplier,
            EventHandlerRegistry eventHandlerRegistry,
            ServiceRegistryMapper mapper) {
        this(bucketSupplier, DEFAULT_RECORDS_KEY, eventHandlerRegistry, mapper,
                DEFAULT_SERVICE_RECORD,
                DEFAULT_HEARTBEAT_INTERVAL_IN_SECONDS);
    }

    public CouchbaseServiceRegistry(
            Supplier<Bucket> bucketSupplier,
            String recordsKey,
            EventHandlerRegistry eventHandlerRegistry,
            ServiceRegistryMapper mapper,
            ServiceRecord serviceRecord,
            int heartBeatIntervalInSeconds) {
        super(eventHandlerRegistry, mapper);
        requireNonNull(bucketSupplier, "bucketSupplier cannot be null");
        requireNonNull(recordsKey, "recordsKey cannot be null");
        requireNonNull(serviceRecord, "serviceRecord cannot be null");
        this.bucketSupplier = bucketSupplier;
        this.recordsKey = recordsKey;
        this.serviceRecord = serviceRecord;
        this.serviceObject = toCouchbaseObject(serviceRecord);
        this.heartBeatIntervalInSeconds = heartBeatIntervalInSeconds;
        this.builder = MutationOptionBuilder.builder()
                .createDocument(true)
                .expiry((int) (heartBeatIntervalInSeconds * 1.5))
        ;
    }

    static {
        json.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        json.registerModule(JacksonMapper.jsonTypesModule());
    }

    public static JsonObject toCouchbaseObject(ServiceRecord serviceRecord) {
        return JsonObject.create()
                .put("name", serviceRecord.name)
                .put("status", serviceRecord.status.name())
                .put("type", serviceRecord.type.name())
                .put("registration", serviceRecord.registrationId)
                .put("location", JsonObject.fromJson(serviceRecord.location.encode(json::writeValueAsString)))
                .put("metadata", JsonObject.fromJson(serviceRecord.metaData.encode(json::writeValueAsString)));
    }

    @Override
    protected Observable<List<ServiceRecord>> findRecordsOf(Command command) {
        return bucketSupplier.get().async()
                .get(recordsKey)
                .retry(CouchbaseServiceRegistry::RETRY_DEFAULT)
                .map(AbstractDocument::content)
                .flatMap(jsonObject -> Observable.just(jsonObject.getNames().stream()
                        .map(jsonObject::getObject)
                        .filter(o -> isCompatibleWith(command, o))
                        .map(CouchbaseServiceRegistry::toRecord)
                        .collect(toList())))
                .defaultIfEmpty(emptyList());
    }

    private static Boolean RETRY_DEFAULT(Integer retryCount, Throwable error) {
        return (retryCount < 10) && (error instanceof TemporaryFailureException ||
                error instanceof TemporaryLockFailureException ||
                error instanceof CASMismatchException
        );
    }

    public static boolean isCompatibleWith(Command command, JsonObject serviceRecordObject) {
        return getStatus(serviceRecordObject) == Status.UP &&
                ofNullable(serviceRecordObject.getObject("metadata"))
                    .map(metadata -> metadata.getArray(ServiceRecord.METADATA_COMMANDS))
                    .filter(commands -> StreamSupport.stream(commands.spliterator(), false)
                            .filter(o -> o instanceof JsonObject)
                            .map(o -> (JsonObject)o)
                            .anyMatch(jsonObject -> command.name.equals(jsonObject.getString(COMMAND))
                                && command.eventType().equals(jsonObject.getString(EVENT))))
                    .isPresent();
    }

    private static Status getStatus(JsonObject jsonObject) {
        return ofNullable(jsonObject.getString("status"))
                .map(Status::valueOf)
                .orElse(Status.UNKNOWN);
    }

    public static ServiceRecord toRecord(JsonObject jsonObject) {
        return ServiceRecord.create(
                jsonObject.getString("name"),
                getStatus(jsonObject),
                ofNullable(jsonObject.getString("type"))
                        .map(ServiceType::valueOf)
                        .orElse(ServiceType.LOCAL),
                jsonObject.getString("registration"),
                ofNullable(jsonObject.getObject("location"))
                    .map(JsonObject::toMap)
                    .map(net.soundvibe.reacto.types.json.JsonObject::new)
                    .orElseThrow(() -> new IllegalStateException("Location is missing from service record: " + jsonObject.toString())),
                ofNullable(jsonObject.getObject("metadata"))
                    .map(JsonObject::toMap)
                    .map(net.soundvibe.reacto.types.json.JsonObject::new)
                    .orElseGet(net.soundvibe.reacto.types.json.JsonObject::empty)
                );
    }

    @Override
    public Observable<Any> unpublish(ServiceRecord serviceRecord) {
        return bucketSupplier.get().async()
                .mapRemove(recordsKey, serviceRecord.registrationId)
                .retry(CouchbaseServiceRegistry::RETRY_DEFAULT)
                .filter(wasRemoved -> wasRemoved)
                .map(__ -> Any.VOID)
                .switchIfEmpty(Observable.error(new IllegalStateException("Service record was not unpublished")));
    }

    public Observable<Any> publish() {
        return bucketSupplier.get().async()
                .mapAdd(recordsKey, serviceRecord.registrationId, serviceObject, builder)
                .retry(CouchbaseServiceRegistry::RETRY_DEFAULT)
                .filter(wasAdded -> wasAdded)
                .map(__ -> Any.VOID)
                .switchIfEmpty(Observable.error(new IllegalStateException("Service record was not published")))
                ;
    }

    private void startHeartBeat() {
        timer.set(Scheduler.scheduleAtFixedInterval(TimeUnit.SECONDS.toMillis(heartBeatIntervalInSeconds),
                () -> Observable.just(serviceRecord)
                        .filter(rec -> isOpen.get())
                        .flatMap(rec -> publish())
                        .subscribe(any -> log.info("Service was updated successfully"),
                                error -> log.error("Error when updating service registration: " + error, error))
                , "Couchbase service registry heartbeat"));
    }

    @Override
    public Observable<Any> register() {
        return Observable.just(serviceRecord)
                .filter(rec -> !rec.equals(DEFAULT_SERVICE_RECORD))
                .filter(rec -> !isOpen.get())
                .flatMap(rec -> publish())
                .doOnNext(any -> startHeartBeat())
                .doOnNext(any -> isOpen.set(true));
    }

    @Override
    public Observable<Any> unregister() {
        return Observable.just(serviceRecord)
                .filter(rec -> !rec.equals(DEFAULT_SERVICE_RECORD))
                .filter(rec -> isOpen.get())
                .doOnNext(rec -> timer.get().cancel())
                .flatMap(this::unpublish)
                .doOnNext(any -> isOpen.set(false))
                ;
    }
}
