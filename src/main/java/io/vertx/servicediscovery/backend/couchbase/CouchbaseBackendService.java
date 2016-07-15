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

package io.vertx.servicediscovery.backend.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.AbstractDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.error.TemporaryLockFailureException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.spi.ServiceDiscoveryBackend;
import rx.*;
import rx.Observable;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * An implementation of the discovery backend based on Redis.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class CouchbaseBackendService implements ServiceDiscoveryBackend {

    private static CouchbaseEnvironment couchbaseEnvironment = DefaultCouchbaseEnvironment.builder()
            .connectTimeout(10000L) // 10000ms = 10s because of virtual machines lags
            .kvTimeout(5000L)
            .build();

    private Bucket couchbase;
    private String key;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Vertx vertx, JsonObject configuration) {
        key = configuration.getString("key", "service-discovery-records");
        final List<String> nodes = Optional.ofNullable(configuration.getJsonArray("nodes"))
                .map(jsonArray -> jsonArray.stream()
                        .filter(o -> o instanceof String)
                        .map(o -> (String) o)
                        .collect(Collectors.toList()))
                .orElseThrow(() -> new IllegalArgumentException("Couchbase 'nodes' (JsonArray) are not present in backend configuration"));
        final CouchbaseCluster cluster = CouchbaseCluster.create(couchbaseEnvironment, nodes);
        couchbase = cluster.openBucket(
                Optional.ofNullable(configuration.getString("bucketName"))
                        .orElseThrow(() -> new IllegalArgumentException("Couchbase 'bucketName' is missing in backend configuration"))
                , Optional.ofNullable(configuration.getString("pwd"))
                        .orElseThrow(() -> new IllegalArgumentException("Couchbase 'pwd' is missing in backend configuration")));
    }

    @Override
    public void store(Record record, Handler<AsyncResult<Record>> resultHandler) {
        if (record.getRegistration() != null) {
            resultHandler.handle(Future.failedFuture("The record has already been registered"));
            return;
        }
        final String uuid = UUID.randomUUID().toString();
        record.setRegistration(uuid);
        getAndLock()
                .onExceptionResumeNext(Observable.timer(100L, TimeUnit.MILLISECONDS).flatMap(aLong -> getAndLock()))
                .retry((count, e) -> (count < 10) && (e instanceof TemporaryFailureException || e instanceof TemporaryLockFailureException))
                .defaultIfEmpty(JsonDocument.create(key, com.couchbase.client.java.document.json.JsonObject.create()))
                .doOnNext(jsonDocument -> jsonDocument.content().put(uuid, com.couchbase.client.java.document.json.JsonObject.fromJson(record.toJson().encode())))
                .flatMap(jsonDocument -> {
                    if (jsonDocument.cas() != 0L) {
                        return couchbase.async().replace(jsonDocument);
                    } else {
                        return couchbase.async().insert(jsonDocument);
                    }
                })
                .subscribe(
                    rawJsonDocument -> resultHandler.handle(Future.succeededFuture(record)),
                    throwable -> resultHandler.handle(Future.failedFuture(throwable))
        );
    }

    private Observable<JsonDocument> getAndLock() {
        return couchbase.async().get(key);
    }

    @Override
    public void remove(Record record, Handler<AsyncResult<Record>> resultHandler) {
        Objects.requireNonNull(record, "No record");
        remove(record.getRegistration(), resultHandler);
    }

    @Override
    public void remove(String uuid, Handler<AsyncResult<Record>> resultHandler) {
        Objects.requireNonNull(uuid, "No registration id in the record");
        getAndLock()
                .onExceptionResumeNext(Observable.timer(100L, TimeUnit.MILLISECONDS).flatMap(aLong -> getAndLock()))
                .retry((count, e) -> (count < 10) && (e instanceof TemporaryFailureException || e instanceof TemporaryLockFailureException))
                .doOnNext(jsonDocument -> jsonDocument.content().removeKey(uuid))
                //.doOnNext(jsonDocument -> couchbase.unlock(jsonDocument.id(), jsonDocument.cas()))
                .map(jsonDocument -> couchbase.replace(jsonDocument))
                .subscribe(doc -> resultHandler.handle(Future.succeededFuture(
                        new Record(new JsonObject(doc.content().toString())))),
                        throwable -> resultHandler.handle(Future.failedFuture(throwable)));

    }

    @Override
    public void update(Record record, Handler<AsyncResult<Void>> resultHandler) {
        Objects.requireNonNull(record.getRegistration(), "No registration id in the record");
        getAndLock()
                .retry((count, e) -> (count < 100) && (e instanceof TemporaryFailureException || e instanceof TemporaryLockFailureException))
                .doOnNext(jsonDocument -> jsonDocument.content()
                        .put(record.getRegistration(), com.couchbase.client.java.document.json.JsonObject.fromJson(record.toJson().encode())))
                //.doOnNext(jsonDocument -> couchbase.unlock(jsonDocument.id(), jsonDocument.cas()))
                .flatMap(jsonDocument -> couchbase.async().replace(jsonDocument))
                .subscribe(jsonDocument -> resultHandler.handle(Future.succeededFuture()),
                        throwable -> resultHandler.handle(Future.failedFuture(throwable)));
    }

    @Override
    public void getRecords(Handler<AsyncResult<List<Record>>> resultHandler) {
        couchbase.async().get(key)
                .retry((count, e) -> (count < 100) && (e instanceof TemporaryFailureException || e instanceof TemporaryLockFailureException))
                .map(AbstractDocument::content)
                .flatMap(jsonObject -> {
                    final Map<String, Object> fields = jsonObject.toMap();
                    return rx.Observable.just(fields.keySet().stream()
                            .map(key -> new Record(new JsonObject(jsonObject.getObject(key).toString())))
                            .collect(Collectors.toList()));

                })
                .subscribe(
                        records -> resultHandler.handle(Future.succeededFuture(records)),
                        throwable -> resultHandler.handle(Future.failedFuture(throwable))
                );
    }

    @Override
    public void getRecord(String uuid, Handler<AsyncResult<Record>> resultHandler) {
        couchbase.async().get(key)
                .retry((count, e) -> (count < 100) && (e instanceof TemporaryFailureException || e instanceof TemporaryLockFailureException))
                .map(AbstractDocument::content)
                .map(jsonObject -> jsonObject.getObject(uuid))
                .subscribe(
                        jsonObject -> {
                            if (jsonObject == null) {
                                resultHandler.handle(Future.succeededFuture(null));
                            } else {
                                resultHandler.handle(Future.succeededFuture(new Record(new JsonObject(jsonObject.toString()))));
                            }
                        },
                        throwable -> resultHandler.handle(Future.failedFuture(throwable))
                );
    }
}
