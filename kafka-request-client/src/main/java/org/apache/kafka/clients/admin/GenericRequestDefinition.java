/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.function.Function;

public class GenericRequestDefinition<R extends AbstractResponse> extends RequestDefinition<R, GenericRequestDefinition> {

    private final Function<Long, AbstractRequest.Builder<?>> requestBuilderSupplier;

    public static GenericRequestDefinition<AbstractResponse> create(AbstractRequest.Builder<?> requestBuilder) {
        return new GenericRequestDefinition<>(requestBuilder, AbstractResponse.class);
    }

    public static GenericRequestDefinition<AbstractResponse> create(String requestName, Function<Long, AbstractRequest.Builder<?>> requestBuilderSupplier) {
        return new GenericRequestDefinition<>(requestName, requestBuilderSupplier, AbstractResponse.class);
    }

    public GenericRequestDefinition(AbstractRequest.Builder<?> requestBuilder, Class<R> responseType) {
        this(requestBuilder.apiKey().name, timeoutMs -> requestBuilder, responseType);
    }

    public GenericRequestDefinition(String requestName, Function<Long, AbstractRequest.Builder<?>> requestBuilderSupplier, Class<R> responseType) {
        super(requestName, responseType);
        this.requestBuilderSupplier = requestBuilderSupplier;
    }

    @Override
    public AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        return requestBuilderSupplier.apply(timeoutMs);
    }

}
