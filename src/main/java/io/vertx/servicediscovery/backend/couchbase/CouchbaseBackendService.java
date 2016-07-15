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
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.spi.ServiceDiscoveryBackend;

import java.util.*;
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
        key = configuration.getString("key", "service-discovery");
        final List<Object> nodes = configuration.getJsonArray("nodes").getList();
        final CouchbaseCluster cluster = CouchbaseCluster.create(couchbaseEnvironment,
                nodes.stream()
                        .filter(o -> o instanceof String)
                        .map(o -> (String)o)
                        .collect(Collectors.toList())
        );
        couchbase = cluster.openBucket(configuration.getString("bucketName"), configuration.getString("pwd"));
    }

    @Override
    public void store(Record record, Handler<AsyncResult<Record>> resultHandler) {
        if (record.getRegistration() != null) {
            resultHandler.handle(Future.failedFuture("The record has already been registered"));
            return;
        }
        String uuid = UUID.randomUUID().toString();
        record.setRegistration(uuid);

        try {
            final JsonDocument map = getMap().orElseGet(() -> JsonDocument.create(key));
            map.content().put(uuid, com.couchbase.client.java.document.json.JsonObject.fromJson(record.toJson().encode()));

            couchbase.async().upsert(map)
                    .subscribe(
                            rawJsonDocument -> resultHandler.handle(Future.succeededFuture(record)),
                            throwable -> resultHandler.handle(Future.failedFuture(throwable))
                    );
        } catch (Throwable e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private Optional<JsonDocument> getMap() {
        return Optional.ofNullable(couchbase.get(key));
    }


    @Override
    public void remove(Record record, Handler<AsyncResult<Record>> resultHandler) {
        Objects.requireNonNull(record.getRegistration(), "No registration id in the record");
        remove(record.getRegistration(), resultHandler);
    }

    @Override
    public void remove(String uuid, Handler<AsyncResult<Record>> resultHandler) {
        Objects.requireNonNull(uuid, "No registration id in the record");
        try {
            getMap().ifPresent(jsonDocument -> {
                jsonDocument.content().removeKey(uuid);
                couchbase.async().replace(jsonDocument)
                        .subscribe(
                                doc -> resultHandler.handle(Future.succeededFuture(
                                        new Record(new JsonObject(doc.content().toString())))),
                                throwable -> resultHandler.handle(Future.failedFuture(throwable))
                        );
            });
        } catch (Throwable e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public void update(Record record, Handler<AsyncResult<Void>> resultHandler) {
        Objects.requireNonNull(record.getRegistration(), "No registration id in the record");
        try {
            getMap().ifPresent(jsonDocument -> {
                jsonDocument.content().put(record.getRegistration(), com.couchbase.client.java.document.json.JsonObject.fromJson(record.toJson().encode()));
                couchbase.async().replace(jsonDocument)
                        .subscribe(
                                rawJsonDocument -> resultHandler.handle(Future.succeededFuture()),
                                throwable -> resultHandler.handle(Future.failedFuture(throwable))
                        );
            });
        } catch (Throwable e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public void getRecords(Handler<AsyncResult<List<Record>>> resultHandler) {
        try {
            final com.couchbase.client.java.document.json.JsonObject jsonObject = getMap()
                    .map(AbstractDocument::content)
                    .orElseGet(com.couchbase.client.java.document.json.JsonObject::create);
            final Map<String, Object> fields = jsonObject.toMap();

            final List<Record> records = fields.keySet().stream()
                    .map(key -> new Record(new JsonObject(jsonObject.getObject(key).toString())))
                    .collect(Collectors.toList());

            resultHandler.handle(Future.succeededFuture(records));
        } catch (Throwable e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public void getRecord(String uuid, Handler<AsyncResult<Record>> resultHandler) {
        try {
            final Optional<JsonDocument> map = getMap();
            if (!map.isPresent()) {
                resultHandler.handle(Future.failedFuture("Records are not present."));
                return;
            }

            final com.couchbase.client.java.document.json.JsonObject object = map.get().content().getObject(uuid);
            if (object == null) {
                resultHandler.handle(Future.succeededFuture(null));
                return;
            }

            resultHandler.handle(Future.succeededFuture(new Record(new JsonObject(object.toString()))));
        } catch (Throwable e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }
}
