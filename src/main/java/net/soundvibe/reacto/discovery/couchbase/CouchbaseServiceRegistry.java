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

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.*;
import com.couchbase.client.java.view.*;
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
import java.util.stream.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static net.soundvibe.reacto.types.CommandDescriptor.*;

/**
 * An implementation of the reacto service registry based on Couchbase.
 *
 * @author Linas Naginionis
 */
public final class CouchbaseServiceRegistry extends AbstractServiceRegistry implements ServiceDiscoveryLifecycle {

    private final static Logger log = LoggerFactory.getLogger(CouchbaseServiceRegistry.class);

    public static final ViewQuery DEFAULT_VIEW_QUERY = ViewQuery.from("reacto", "services");
    public static final ServiceRecord DEFAULT_SERVICE_RECORD =
            ServiceRecord.create("UNKNOWN", Status.UNKNOWN, ServiceType.LOCAL, UUID.randomUUID().toString(),
                    net.soundvibe.reacto.types.json.JsonObject.empty(),
                    net.soundvibe.reacto.types.json.JsonObject.empty());

    public static final int DEFAULT_HEARTBEAT_INTERVAL_IN_SECONDS = 60;

    public static final ObjectMapper json = new ObjectMapper();
    public static final String METADATA = "metadata";
    public static final String NAME = "name";
    public static final String STATUS = "status";
    public static final String TYPE = "type";
    public static final String REGISTRATION = "registration";
    public static final String LOCATION = "location";
    public static final String OBJECT_TYPE = "objectType";
    public static final String REACTO_SERVICE_REGISTRY = "reacto-service-registry";

    private final Supplier<Bucket> bucketSupplier;
    private final ViewQuery viewQuery;
    private final ServiceRecord serviceRecord;
    private final AtomicBoolean isOpen = new AtomicBoolean(false);
    private final AtomicReference<Timer> timer = new AtomicReference<>();
    private final JsonObject serviceObject;
    private final int heartBeatIntervalInSeconds;

    public CouchbaseServiceRegistry(
            Supplier<Bucket> bucketSupplier,
            EventHandlerRegistry eventHandlerRegistry,
            ServiceRegistryMapper mapper,
            ServiceRecord serviceRecord) {
        this(bucketSupplier,
            DEFAULT_VIEW_QUERY,
            eventHandlerRegistry,
            mapper,
            serviceRecord,
            DEFAULT_HEARTBEAT_INTERVAL_IN_SECONDS);
    }

    public CouchbaseServiceRegistry(
            Supplier<Bucket> bucketSupplier,
            ViewQuery viewQuery,
            EventHandlerRegistry eventHandlerRegistry,
            ServiceRegistryMapper mapper,
            ServiceRecord serviceRecord,
            int heartBeatIntervalInSeconds) {
        super(eventHandlerRegistry, mapper);
        requireNonNull(bucketSupplier, "bucketSupplier cannot be null");
        requireNonNull(viewQuery, "viewQuery cannot be null");
        requireNonNull(serviceRecord, "serviceRecord cannot be null");
        this.bucketSupplier = bucketSupplier;
        this.viewQuery = viewQuery;
        this.serviceRecord = serviceRecord;
        this.serviceObject = toCouchbaseObject(serviceRecord);
        this.heartBeatIntervalInSeconds = heartBeatIntervalInSeconds;
    }

    static {
        json.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        json.registerModule(JacksonMapper.jsonTypesModule());
    }

    public static JsonObject toCouchbaseObject(ServiceRecord serviceRecord) {
        return JsonObject.create()
                .put(NAME, serviceRecord.name)
                .put(STATUS, serviceRecord.status.name())
                .put(TYPE, serviceRecord.type.name())
                .put(REGISTRATION, serviceRecord.registrationId)
                .put(OBJECT_TYPE, REACTO_SERVICE_REGISTRY)
                .put(LOCATION, JsonObject.fromJson(serviceRecord.location.encode(json::writeValueAsString)))
                .put(METADATA, JsonObject.fromJson(serviceRecord.metaData.encode(json::writeValueAsString)));
    }

    private final static String viewMapFunction = "function (doc, meta) {\n" +
            "  if (doc.status && doc.objectType) {\n" +
            "    if (doc.objectType === \"reacto-service-registry\" && doc.status === \"UP\") {\n" +
            "      emit(doc.registration, null);\n" +
            "    }\n" +
            "  }\n" +
            "}";

    public Observable<DesignDocument> updateDefaultView(String designDocument, String viewName) {
        return bucketSupplier.get().bucketManager().async()
                .getDesignDocument(designDocument)
                .doOnNext(doc -> doc.views()
                        .replaceAll(view -> viewName.equals(view.name()) ? DefaultView.create(view.name(), viewMapFunction) : view))
                .flatMap(doc -> bucketSupplier.get().bucketManager().async().upsertDesignDocument(doc))
                .switchIfEmpty(Observable.defer(() -> bucketSupplier.get().bucketManager().async()
                        .insertDesignDocument(DesignDocument.create(designDocument, singletonList(DefaultView.create(viewName, viewMapFunction))))
                ))
                .flatMap(doc -> bucketSupplier.get().bucketManager().async().publishDesignDocument(designDocument, true));
    }

    @Override
    protected Observable<List<ServiceRecord>> findRecordsOf(Command command) {
        return bucketSupplier.get().async()
                .query(viewQuery)
                .retry(CouchbaseServiceRegistry::RETRY_DEFAULT)
                .flatMap(AsyncViewResult::rows)
                .flatMap(AsyncViewRow::document)
                .map(AbstractDocument::content)
                .filter(jsonObject -> isCompatibleWith(command, jsonObject))
                .map(CouchbaseServiceRegistry::toRecord)
                .toList()
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
                ofNullable(serviceRecordObject.getObject(METADATA))
                    .map(metadata -> metadata.getArray(ServiceRecord.METADATA_COMMANDS))
                    .map(commands -> StreamSupport.stream(commands.spliterator(), false)
                            .filter(o -> o instanceof JsonObject)
                            .map(o -> (JsonObject)o)
                            .anyMatch(jsonObject -> command.name.equals(jsonObject.getString(COMMAND))
                                && command.eventType().equals(jsonObject.getString(EVENT))))
                    .orElse(false);
    }

    private static Status getStatus(JsonObject jsonObject) {
        return ofNullable(jsonObject.getString(STATUS))
                .map(Status::valueOf)
                .orElse(Status.UNKNOWN);
    }

    public static ServiceRecord toRecord(JsonObject jsonObject) {
        return ServiceRecord.create(
                jsonObject.getString(NAME),
                getStatus(jsonObject),
                ofNullable(jsonObject.getString(TYPE))
                        .map(ServiceType::valueOf)
                        .orElse(ServiceType.LOCAL),
                jsonObject.getString(REGISTRATION),
                ofNullable(jsonObject.getObject(LOCATION))
                    .map(JsonObject::toMap)
                    .map(net.soundvibe.reacto.types.json.JsonObject::new)
                    .orElseThrow(() -> new IllegalStateException("Location is missing from service record: " + jsonObject.toString())),
                ofNullable(jsonObject.getObject(METADATA))
                    .map(JsonObject::toMap)
                    .map(net.soundvibe.reacto.types.json.JsonObject::new)
                    .orElseGet(net.soundvibe.reacto.types.json.JsonObject::empty)
                );
    }

    public Observable<Any> unpublish(ServiceRecord serviceRecord) {
        return bucketSupplier.get().async()
                .remove(serviceRecord.registrationId, PersistTo.NONE, ReplicateTo.NONE)
                .flatMap(jsonDocument -> bucketSupplier.get().async()
                        .query(ViewQuery.from(viewQuery.getDesign(), viewQuery.getView()).stale(Stale.FALSE).limit(1))
                        .map(AsyncViewResult::success)
                )
                .retry(CouchbaseServiceRegistry::RETRY_DEFAULT)
                .filter(wasRefreshed -> wasRefreshed)
                .map(__ -> Any.VOID)
                .switchIfEmpty(Observable.error(new IllegalStateException("Service record was not unpublished")));
    }

    public Observable<Any> publish() {
        return bucketSupplier.get().async()
                .upsert(JsonDocument.create(serviceRecord.registrationId, ttl(), serviceObject),
                        PersistTo.NONE, ReplicateTo.NONE)
                .flatMap(jsonDocument -> bucketSupplier.get().async()
                        .query(ViewQuery.from(viewQuery.getDesign(), viewQuery.getView()).stale(Stale.FALSE).limit(1))
                        .map(AsyncViewResult::success)
                )
                .retry(CouchbaseServiceRegistry::RETRY_DEFAULT)
                .filter(wasAdded -> wasAdded)
                .map(__ -> Any.VOID)
                .switchIfEmpty(Observable.error(new IllegalStateException("Service record was not published")))
                ;
    }

    public Observable<Any> update() {
        return bucketSupplier.get().async()
                .touch(serviceRecord.registrationId, ttl())
                .retry(CouchbaseServiceRegistry::RETRY_DEFAULT)
                .filter(wasUpdated -> wasUpdated)
                .map(__ -> Any.VOID)
                .switchIfEmpty(Observable.error(new IllegalStateException("Service record was not updated")))
                ;
    }

    private int ttl() {
        return (int) (heartBeatIntervalInSeconds * 1.5);
    }

    private void startHeartBeat() {
        timer.set(Scheduler.scheduleAtFixedInterval(TimeUnit.SECONDS.toMillis(heartBeatIntervalInSeconds),
                () -> Observable.just(serviceRecord)
                        .filter(rec -> isOpen.get())
                        .flatMap(rec -> update())
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
                .doOnNext(any -> Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    log.info("Executing shutdown hook...");
                    unregister()
                            .subscribe(
                                    __ -> log.info("Service was successfully unregistered before shutting down"),
                                    error -> log.error("Service was unable to unregister before shutting down: " + error)
                            );
                })))
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
