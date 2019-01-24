/*
 * Copyright © 2018 Apple Inc. and the ServiceTalk project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.servicetalk.http.api;

import io.servicetalk.concurrent.api.Executor;
import io.servicetalk.concurrent.api.Publisher;
import io.servicetalk.concurrent.api.Single;
import io.servicetalk.transport.api.ExecutionStrategy;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An execution strategy for HTTP client and servers.
 */
public interface HttpExecutionStrategy extends ExecutionStrategy {

    /**
     * Invokes the passed {@link Function} and applies the necessary offloading of request and response for a client.
     *
     * @param fallback {@link Executor} to use as a fallback if this {@link HttpExecutionStrategy} does not define an
     * {@link Executor}.
     * @param request {@link StreamingHttpRequest} for which the offloading is to be applied.
     * @param client A {@link Function} that given flattened stream of {@link HttpRequestMetaData}, payload and
     * trailers, for the passed {@link StreamingHttpRequest} returns a {@link Single}.
     * @return {@link Single} which is offloaded as required.
     */
    Single<StreamingHttpResponse> invokeClient(Executor fallback, StreamingHttpRequest request,
                                               Function<Publisher<Object>, Single<StreamingHttpResponse>> client);

    /**
     * Invokes the passed {@link Function} and applies the necessary offloading of request and response for a server.
     *
     * @param fallback {@link Executor} to use as a fallback if this {@link HttpExecutionStrategy} does not define an
     * {@link Executor}.
     * @param request {@link StreamingHttpRequest} for which the offloading is to be applied.
     * @param service A {@link Function} that executes a {@link StreamingHttpRequest} and returns a
     * {@link Single}
     * @param errorHandler In case there is an error on calling the passed {@code service} or if the returned
     * {@link Single} from the {@code service} terminates with an error. This {@link BiFunction} will be called to
     * generate an error response.
     * @return A flattened {@link Publisher} containing {@link HttpRequestMetaData}, payload and trailers
     * for the {@link StreamingHttpResponse} returned by the passed {@code service}.
     */
    Publisher<Object> invokeService(Executor fallback, StreamingHttpRequest request,
                                    Function<StreamingHttpRequest, Single<StreamingHttpResponse>> service,
                                    BiFunction<Throwable, Executor, Single<StreamingHttpResponse>> errorHandler);

    /**
     * Invokes a service represented by the passed {@link Function}.
     * <p>
     * This method does not apply the strategy on the object returned from the {@link Function}, if that object is an
     * asynchronous source then the caller of this method should take care and offload that source using
     * {@link #offloadSend(Executor, Single)} or {@link #offloadSend(Executor, Publisher)}.
     *
     * @param <T> Type of result of the invocation of the service.
     * @param fallback {@link Executor} to use as a fallback if this {@link HttpExecutionStrategy} does not define an
     * {@link Executor}.
     * @param service {@link Function} representing a service.
     * @return A {@link Single} that invokes the passed {@link Function} and returns the result asynchronously.
     * Invocation of {@link Function} will be offloaded if configured.
     */
    <T> Single<T> invokeService(Executor fallback, Function<Executor, T> service);

    /**
     * Wraps the passed {@link StreamingHttpRequestHandler} to apply this {@link HttpExecutionStrategy}.
     *
     * @param fallback {@link Executor} to use as a fallback if this {@link HttpExecutionStrategy} does not define an
     * {@link Executor}.
     * @param handler {@link StreamingHttpRequestHandler} to wrap.
     * @return Wrapped {@link StreamingHttpService}.
     */
    StreamingHttpService offloadService(Executor fallback, StreamingHttpRequestHandler handler);
}