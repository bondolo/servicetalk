/*
 * Copyright © 2018-2019, 2021 Apple Inc. and the ServiceTalk project authors
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
package io.servicetalk.concurrent.api.publisher;

import io.servicetalk.concurrent.PublisherSource;
import io.servicetalk.concurrent.api.Executor;
import io.servicetalk.concurrent.api.Executors;
import io.servicetalk.concurrent.api.Publisher;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static io.servicetalk.concurrent.api.Executors.immediate;
import static io.servicetalk.concurrent.api.SourceAdapters.toSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

public class PublisherOffloadTest {

    private static final String DEFAULT_ITEM = "success";
    private static final Exception DEFAULT_EXCEPTION = new Exception("failure");

    static class VerifySubscriber implements PublisherSource.Subscriber<String> {

        final AtomicReference<PublisherSource.Subscription> subscription = new AtomicReference<>();
        final AtomicBoolean subscribed = new AtomicBoolean();
        final AtomicBoolean complete = new AtomicBoolean();

        final Predicate<String> itemMatches;
        final Predicate<Throwable> errorMatches;
        final Matcher<? super Thread> subscribeThread;
        final Matcher<? super Thread> publishThread;

        VerifySubscriber(@Nullable Predicate<String> itemMatches, @Nullable Predicate<Throwable> errorMatches,
                        Thread subscribeThread, Thread publishThread) {
            this.itemMatches = itemMatches;
            this.errorMatches = errorMatches;
            this.subscribeThread = CoreMatchers.sameInstance(subscribeThread);
            this.publishThread = CoreMatchers.sameInstance(publishThread);
        }

        @Override
        public void onSubscribe(final PublisherSource.Subscription subscription) {
            boolean first = subscribed.compareAndSet(false, true);
            assertThat("already subscribed", first);
            first = this.subscription.compareAndSet(null, subscription);
            assertThat("Subscription was already set", first);
            assertThat("Unexpected thread " + Thread.currentThread(), Thread.currentThread(), subscribeThread);
            subscription.request(1);
        }

        @Override
        public void onNext(@Nullable final String result) {
            assertThat("not subscribed", subscribed.get());
            boolean first = complete.compareAndSet(false, true);
            assertThat("already completed", first);
            assertThat("Unexpected item " + result, itemMatches, notNullValue());
            assertThat("Item doesn't match", itemMatches.test(result));
            assertThat("Unexpected thread " + Thread.currentThread(), Thread.currentThread(), publishThread);
        }

        @Override
        public void onComplete() {
            assertThat("not subscribed", subscribed.get());
            boolean first = complete.compareAndSet(false, true);
            assertThat("already completed", first);
            assertThat("Unexpected thread " + Thread.currentThread(), Thread.currentThread(), publishThread);
        }

        @Override
        public void onError(final Throwable t) {
            assertThat("not subscribed", subscribed.get());
            boolean first = complete.compareAndSet(false, true);
            assertThat("already completed", first);
            assertThat("Unexpected throwable " + t, errorMatches, notNullValue());
            assertThat("throwable doesn't match", errorMatches.test(t));
            assertThat("Unexpected thread " + Thread.currentThread(), Thread.currentThread(), publishThread);
        }
    }

    final Executor executor = Executors.from(java.util.concurrent.Executors.newSingleThreadExecutor());
    final Thread offloadThread;

    public PublisherOffloadTest() {
        try {
            offloadThread = executor.submitCallable(() -> Thread::currentThread).toFuture().get();
        } catch (InterruptedException | ExecutionException failed) {
            throw new AssertionError("Unexpectedly exception", failed);
        }
    }

    private enum Result {
        SUCCESS(() -> Publisher.from(DEFAULT_ITEM)),
        FAILURE(() -> Publisher.failed(DEFAULT_EXCEPTION));

        private final Supplier<Publisher<String>> supplier;

        Result(Supplier<Publisher<String>> supplier) {
            this.supplier = supplier;
        }
    }

    private enum OffloadSubscribe {
        IMMEDIATE_SUBSCRIBE,
        OFFLOAD_SUBSCRIBE
    }

    private enum OffloadPublish {
        IMMEDIATE_PUBLISH,
        OFFLOAD_PUBLISH
    }

    private static Stream<Arguments> singleOffloadTest() {
        List<Arguments> testCases = new ArrayList<>();
        for (Result result : Result.values()) {
            for (OffloadSubscribe subscribeOffload : OffloadSubscribe.values()) {
                for (OffloadPublish publishOffload : OffloadPublish.values()) {
                    Arguments testCase = Arguments.of(result, subscribeOffload, publishOffload);
                    testCases.add(testCase);
                }
            }
        }
        return testCases.stream();
    }

    @ParameterizedTest
    @MethodSource
    public void singleOffloadTest(final Result result,
                                  final OffloadSubscribe subscribeOffload,
                                  final OffloadPublish publishOffload) throws Exception {
        Publisher<String> publisher = result.supplier.get();
        PublisherSource<String> source = toSource(OffloadSubscribe.OFFLOAD_SUBSCRIBE == subscribeOffload ?
                OffloadPublish.OFFLOAD_PUBLISH == publishOffload ?
                        publisher.publishAndSubscribeOn(executor) : publisher.subscribeOn(executor)
                : OffloadPublish.OFFLOAD_PUBLISH == publishOffload ?
                        publisher.publishOn(executor) : publisher.publishAndSubscribeOn(immediate()));

        VerifySubscriber subscriber = new VerifySubscriber(
                Result.SUCCESS == result ? DEFAULT_ITEM::equals : null,
                Result.SUCCESS == result ? null : e -> DEFAULT_EXCEPTION == e,
                OffloadSubscribe.OFFLOAD_SUBSCRIBE == subscribeOffload ? offloadThread : Thread.currentThread(),
                OffloadPublish.OFFLOAD_PUBLISH == publishOffload ? offloadThread : Thread.currentThread());

        source.subscribe(subscriber);
    }
}
