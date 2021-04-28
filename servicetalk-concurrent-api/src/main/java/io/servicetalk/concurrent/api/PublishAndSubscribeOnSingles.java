/*
 * Copyright © 2018-2019 Apple Inc. and the ServiceTalk project authors
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
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.SingleSource;
import io.servicetalk.concurrent.internal.SignalOffloader;

import static io.servicetalk.concurrent.internal.SubscriberUtils.deliverErrorFromSource;

/**
 * A set of factory methods that provides implementations for the various publish/subscribeOn methods on
 * {@link Single}.
 */
final class PublishAndSubscribeOnSingles {

    private PublishAndSubscribeOnSingles() {
        // No instance.
    }

    static <T> void deliverOnSubscribeAndOnError(SingleSource.Subscriber<? super T> subscriber,
                                                 SignalOffloader signalOffloader, AsyncContextMap contextMap,
                                                 AsyncContextProvider contextProvider, Throwable cause) {
        deliverErrorFromSource(
                signalOffloader.offloadSubscriber(contextProvider.wrapSingleSubscriber(subscriber, contextMap)), cause);
    }

    static <T> Single<T> publishAndSubscribeOn(Single<T> original, Executor executor) {
        return original.executor() == executor ? original : new PublishAndSubscribeOn<>(executor, original);
    }

    static <T> Single<T> publishOn(Single<T> original, Executor executor) {
        return original.executor() == executor ? original : new PublishOn<>(executor, original);
    }

    static <T> Single<T> subscribeOn(Single<T> original, Executor executor) {
        return original.executor() == executor ? original : new SubscribeOn<>(executor, original);
    }

    private static final class PublishAndSubscribeOn<T> extends AbstractNoHandleSubscribeSingle<T> {
        private final Executor executor;
        private final Single<T> original;

        PublishAndSubscribeOn(final Executor executor, final Single<T> original) {
            this.executor = executor;
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Single that is returned
            // by this operator.
            //
            // Here we offload signals from original to subscriber using signalOffloader.
            // We use executor to create the returned Single which means executor will be used
            // to offload handleSubscribe as well as the Subscription that is sent to the subscriber here.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            original.subscribeWithSharedContext(
                    signalOffloader.offloadSubscriber(
                            contextProvider.wrapSingleSubscriber(subscriber, contextMap)), contextProvider);
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private static class PublishOn<T> extends AbstractNoHandleSubscribeSingle<T> {
        private final Executor executor;
        private final Single<T> original;

        PublishOn(final Executor executor, final Single<T> original) {
            this.executor = MergedExecutors.mergeAndOffloadPublish(original.executor(), executor);
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Single that is returned
            // by this operator.
            //
            // Here we offload signals from original to subscriber using signalOffloader.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            original.subscribeWithSharedContext(
                    signalOffloader.offloadSubscriber(
                            contextProvider.wrapSingleSubscriber(subscriber, contextMap)), contextProvider);
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private static final class SubscribeOn<T> extends AbstractNoHandleSubscribeSingle<T> {
        private final Executor executor;
        private final Single<T> original;

        SubscribeOn(final Executor executor, final Single<T> original) {
            this.executor = MergedExecutors.mergeAndOffloadSubscribe(original.executor(), executor);
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            try {
                executor.execute(() -> original.subscribeWithSharedContext(subscriber, contextProvider));
            } catch (Throwable throwable) {
                // We assume that if executor accepted the task, it was run and no exception will be thrown from run.
                deliverErrorFromSource(subscriber, throwable);
            }
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }
}
