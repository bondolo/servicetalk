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

import io.servicetalk.concurrent.Cancellable;
import io.servicetalk.concurrent.CompletableSource;
import io.servicetalk.concurrent.CompletableSource.Subscriber;
import io.servicetalk.concurrent.internal.SignalOffloader;
import io.servicetalk.concurrent.internal.SignalOffloaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.servicetalk.concurrent.api.Executors.immediate;
import static io.servicetalk.concurrent.api.MergedExecutors.mergeAndOffloadPublish;
import static io.servicetalk.concurrent.api.MergedExecutors.mergeAndOffloadSubscribe;
import static io.servicetalk.concurrent.internal.SubscriberUtils.deliverErrorFromSource;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeCancel;
import static java.util.Objects.requireNonNull;

/**
 * A set of factory methods that provides implementations for the various publish/subscribeOn methods on
 * {@link Completable}.
 */
final class PublishAndSubscribeOnCompletables {

    private static final Logger LOGGER = LoggerFactory.getLogger(PublishAndSubscribeOnSingles.class);

    private PublishAndSubscribeOnCompletables() {
        // No instance.
    }

    static void deliverOnSubscribeAndOnError(Subscriber subscriber, SignalOffloader signalOffloader,
                                             AsyncContextMap contextMap, AsyncContextProvider contextProvider,
                                             Throwable cause) {
        deliverErrorFromSource(
                signalOffloader.offloadSubscriber(contextProvider.wrapCompletableSubscriber(subscriber, contextMap)),
                cause);
    }

    static Completable publishAndSubscribeOn(Completable original, Executor executor) {
        return original.executor() == executor || executor == immediate() ?
                original : new PublishAndSubscribeOn(executor, original);
    }

    static Completable publishOn(Completable original, Executor executor) {
        return original.executor() == executor || executor == immediate() ?
                original : new PublishOn(executor, original);
    }

    static Completable subscribeOn(Completable original, Executor executor) {
        return original.executor() == executor || executor == immediate() ?
                original : new SubscribeOn(executor, original);
    }

    private static final class PublishAndSubscribeOn extends AbstractNoHandleSubscribeCompletable {
        private final Executor executor;
        private final Completable original;

        PublishAndSubscribeOn(final Executor executor, final Completable original) {
            this.executor = executor;
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            try {
                Subscriber offloading = new OffloadedCancellableCompletableSubscriber(subscriber, executor);
                SignalOffloader offloader = SignalOffloaders.newOffloaderFor(executor);
                executor.execute(() ->
                        original.subscribeWithSharedContext(
                                offloader.offloadSubscriber(
                            contextProvider.wrapCompletableSubscriber(offloading, contextMap)), contextProvider));
            } catch (Throwable throwable) {
                // We assume that if executor accepted the task, it was run and no exception will be thrown from accept.
                deliverErrorFromSource(subscriber, throwable);
            }
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private static final class PublishOn extends AbstractNoHandleSubscribeCompletable {
        private final Executor executor;
        private final Completable original;

        PublishOn(final Executor executor, final Completable original) {
            this.executor = mergeAndOffloadPublish(original.executor(), executor);
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Completable that is returned
            // by this operator.
            //
            // Here we offload signals from original to subscriber using signalOffloader.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            SignalOffloader offloader = SignalOffloaders.newOffloaderFor(executor);
            original.subscribeWithSharedContext(
                    offloader.offloadSubscriber(
                            contextProvider.wrapCompletableSubscriber(subscriber, contextMap)), contextProvider);
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private static final class SubscribeOn extends AbstractNoHandleSubscribeCompletable {
        private final Executor executor;
        private final Completable original;

        SubscribeOn(final Executor executor, final Completable original) {
            this.executor = mergeAndOffloadSubscribe(original.executor(), executor);
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber subscriber, final SignalOffloader signalOffloader,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            try {
                Subscriber offloading = new OffloadedCancellableCompletableSubscriber(subscriber, executor);
                executor.execute(() -> original.subscribeWithSharedContext(offloading, contextProvider));
            } catch (Throwable throwable) {
                // We assume that if executor accepted the task, it was run and no exception will be thrown from accept.
                deliverErrorFromSource(subscriber, throwable);
            }
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private static final class OffloadedCancellableCompletableSubscriber implements CompletableSource.Subscriber {
        private final CompletableSource.Subscriber subscriber;
        private final Executor executor;

        OffloadedCancellableCompletableSubscriber(final CompletableSource.Subscriber subscriber,
                                                  final Executor executor) {
            this.subscriber = requireNonNull(subscriber);
            this.executor = executor;
        }

        @Override
        public void onSubscribe(final Cancellable cancellable) {
            subscriber.onSubscribe(new OffloadedCancellable(cancellable, executor));
        }

        @Override
        public void onComplete() {
            subscriber.onComplete();
        }

        @Override
        public void onError(final Throwable t) {
            subscriber.onError(t);
        }
    }

    private static final class OffloadedCancellable implements Cancellable {
        private final Cancellable cancellable;
        private final Executor executor;

        OffloadedCancellable(final Cancellable cancellable, final Executor executor) {
            this.cancellable = requireNonNull(cancellable);
            this.executor = executor;
        }

        @Override
        public void cancel() {
            try {
                executor.execute(() -> safeCancel(cancellable));
            } catch (Throwable t) {
                LOGGER.error("Failed to execute task on the executor {}. " +
                                "Invoking Cancellable (cancel()) in the caller thread. Cancellable {}. ",
                        executor, cancellable, t);
                // As a policy, we call the target in the calling thread when the executor is inadequately
                // provisioned. In the future we could make this configurable.
                cancellable.cancel();
                // We swallow the error here as we are forwarding the actual call and throwing from here will
                // interrupt the control flow.
            }
        }
    }
}
