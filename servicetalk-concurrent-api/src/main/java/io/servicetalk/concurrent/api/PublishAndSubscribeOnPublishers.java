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

import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.internal.SignalOffloader;
import io.servicetalk.concurrent.internal.SignalOffloaderFactory;

import static io.servicetalk.concurrent.api.MergedExecutors.mergeAndOffloadPublish;
import static io.servicetalk.concurrent.internal.SubscriberUtils.deliverErrorFromSource;

/**
 * A set of factory methods that provides implementations for the various publish/subscribeOn methods on
 * {@link Publisher}.
 */
final class PublishAndSubscribeOnPublishers {

    private PublishAndSubscribeOnPublishers() {
        // No instance.
    }

    static <T> void deliverOnSubscribeAndOnError(Subscriber<? super T> subscriber, SignalOffloader signalOffloader,
                                                 AsyncContextMap contextMap, AsyncContextProvider contextProvider,
                                                 Throwable cause) {
        deliverErrorFromSource(
                signalOffloader.offloadSubscriber(contextProvider.wrapPublisherSubscriber(subscriber, contextMap)),
                cause);
    }

    static <T> Publisher<T> publishAndSubscribeOn(Publisher<T> original, Executor executor) {
        return original.executor() == executor ? original : new PublishAndSubscribeOn<>(executor, original);
    }

    static <T> Publisher<T> publishOn(Publisher<T> original, Executor executor) {
        return original.executor() == executor ? original : new PublishOn<>(executor, original);
    }

    static <T> Publisher<T> subscribeOn(Publisher<T> original, Executor executor) {
        return original.executor() == executor ? original : new SubscribeOn<>(executor, original);
    }

    private static final class PublishAndSubscribeOn<T> extends AbstractNoHandleSubscribePublisher<T> {
        private final Executor executor;
        private final Publisher<T> original;

        PublishAndSubscribeOn(final Executor executor, final Publisher<T> original) {
            this.executor = executor;
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Publisher that is returned
            // by this operator.
            //
            // Here we offload signals from original to subscriber using signalOffloader.
            // We use executor to create the returned Publisher which means executor will be used
            // to offload handleSubscribe as well as the Subscription that is sent to the subscriber here.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            try {
                executor.execute(() -> original.subscribeWithSharedContext(signalOffloader.offloadSubscriber(
                    contextProvider.wrapPublisherSubscriber(subscriber, contextMap))));
            } catch (Throwable throwable) {
                // We assume that if executor accepted the task, it was run and no exception will be thrown from accept.
                deliverErrorFromSource(subscriber, throwable);
            }
        }

        @Override
        Executor executor() {
            return executor;
        }
    }

    private static final class PublishOn<T> extends AbstractNoHandleSubscribePublisher<T> {
        private final Executor executor;
        private final Publisher<T> original;

        PublishOn(final Executor executor, final Publisher<T> original) {
            this.executor = mergeAndOffloadPublish(original.executor(), executor);
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Publisher that is returned
            // by this operator.
            //
            // Here we offload signals from original to subscriber using signalOffloader.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            SignalOffloader offloader = ((SignalOffloaderFactory) executor).newSignalOffloader(executor);
            original.subscribeWithSharedContext(
                    offloader.offloadSubscriber(contextProvider.wrapPublisherSubscriber(subscriber, contextMap)));
        }

        @Override
        Executor executor() {
            return executor;
        }
    }

    private static final class SubscribeOn<T> extends AbstractNoHandleSubscribePublisher<T> {
        private final Executor executor;
        private final Publisher<T> original;

        SubscribeOn(final Executor executor, final Publisher<T> original) {
            this.executor = executor;
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            try {
                executor.execute(() -> original.subscribeWithSharedContext(subscriber));
            } catch (Throwable throwable) {
                // We assume that if executor accepted the task, it was run and no exception will be thrown from accept.
                deliverErrorFromSource(subscriber, throwable);
            }
        }

        @Override
        Executor executor() {
            return executor;
        }
    }
}
