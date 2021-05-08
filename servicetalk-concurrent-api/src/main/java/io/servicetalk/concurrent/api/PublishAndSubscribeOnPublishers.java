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

import io.servicetalk.concurrent.PublisherSource;
import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.internal.FlowControlUtils;
import io.servicetalk.concurrent.internal.SignalOffloader;
import io.servicetalk.concurrent.internal.SignalOffloaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static io.servicetalk.concurrent.api.Executors.immediate;
import static io.servicetalk.concurrent.api.MergedExecutors.mergeAndOffloadPublish;
import static io.servicetalk.concurrent.internal.SubscriberUtils.deliverErrorFromSource;
import static io.servicetalk.concurrent.internal.SubscriberUtils.isRequestNValid;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeCancel;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

/**
 * A set of factory methods that provides implementations for the various publish/subscribeOn methods on
 * {@link Publisher}.
 */
final class PublishAndSubscribeOnPublishers {

    private static final Logger LOGGER = LoggerFactory.getLogger(PublishAndSubscribeOnPublishers.class);

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
        return original.executor() == executor || executor == immediate() ?
                original : new PublishAndSubscribeOn<>(executor, original);
    }

    static <T> Publisher<T> publishOn(Publisher<T> original, Executor executor) {
        return original.executor() == executor || executor == immediate() ?
                original : new PublishOn<>(executor, original);
    }

    static <T> Publisher<T> subscribeOn(Publisher<T> original, Executor executor) {
        return original.executor() == executor || executor == immediate() ?
                original : new SubscribeOn<>(executor, original);
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
                Subscriber<? super T> offloading = new OffloadedSubscriptionSubscriber(subscriber, executor);
                SignalOffloader offloader = SignalOffloaders.newOffloaderFor(executor);
                executor.execute(() -> original.subscribeWithSharedContext(offloader.offloadSubscriber(
                    contextProvider.wrapPublisherSubscriber(offloading, contextMap))));
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
            SignalOffloader offloader = SignalOffloaders.newOffloaderFor(executor);
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
                PublisherSource.Subscriber<? super T> offloading =
                        new OffloadedSubscriptionSubscriber(subscriber, executor);
                executor.execute(() -> original.subscribeWithSharedContext(offloading));
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

    private static final class OffloadedSubscriptionSubscriber<T> implements Subscriber<T> {
        private final Subscriber<T> subscriber;
        private final Executor executor;

        OffloadedSubscriptionSubscriber(final Subscriber<T> subscriber, final Executor executor) {
            this.subscriber = requireNonNull(subscriber);
            this.executor = executor;
        }

        @Override
        public void onSubscribe(final PublisherSource.Subscription s) {
            subscriber.onSubscribe(new OffloadedSubscription(executor, s));
        }

        @Override
        public void onNext(final T t) {
            subscriber.onNext(t);
        }

        @Override
        public void onError(final Throwable t) {
            subscriber.onError(t);
        }

        @Override
        public void onComplete() {
            subscriber.onComplete();
        }
    }

    private static final class OffloadedSubscription implements PublisherSource.Subscription, Runnable {
        private static final int STATE_IDLE = 0;
        private static final int STATE_ENQUEUED = 1;
        private static final int STATE_EXECUTING = 2;

        public static final int CANCELLED = -1;
        public static final int TERMINATED = -2;

        private static final AtomicIntegerFieldUpdater<OffloadedSubscription> stateUpdater =
                newUpdater(OffloadedSubscription.class, "state");
        private static final AtomicLongFieldUpdater<OffloadedSubscription> requestedUpdater =
                AtomicLongFieldUpdater.newUpdater(OffloadedSubscription.class, "requested");

        private final Executor executor;
        private final PublisherSource.Subscription target;
        private volatile int state = STATE_IDLE;
        private volatile long requested;

        OffloadedSubscription(final Executor executor, final PublisherSource.Subscription target) {
            this.executor = executor;
            this.target = requireNonNull(target);
        }

        @Override
        public void request(final long n) {
            if ((!isRequestNValid(n) &&
                    requestedUpdater.getAndSet(this, n < TERMINATED ? n : Long.MIN_VALUE) >= 0) ||
                    requestedUpdater.accumulateAndGet(this, n,
                            FlowControlUtils::addWithOverflowProtectionIfNotNegative) > 0) {
                enqueueTaskIfRequired(true);
            }
        }

        @Override
        public void cancel() {
            long oldVal = requestedUpdater.getAndSet(this, CANCELLED);
            if (oldVal != CANCELLED) {
                enqueueTaskIfRequired(false);
            }
            // duplicate cancel.
        }

        private void enqueueTaskIfRequired(boolean forRequestN) {
            final int oldState = stateUpdater.getAndSet(this, STATE_ENQUEUED);
            if (oldState == STATE_IDLE) {
                try {
                    executor.execute(this);
                } catch (Throwable t) {
                    // Ideally, we should send an error to the related Subscriber but that would mean we make sure
                    // we do not concurrently invoke the Subscriber with the original source which would mean we
                    // add some "lock" in the data path.
                    // This is an optimistic approach assuming executor rejections are occasional and hence adding
                    // Subscription -> Subscriber dependency for all paths is too costly.
                    // As we do for other cases, we simply invoke the target in the calling thread.
                    if (forRequestN) {
                        LOGGER.error("Failed to execute task on the executor {}. " +
                                        "Invoking Subscription (request()) in the caller thread. Subscription {}.",
                                executor, target, t);
                        target.request(requestedUpdater.getAndSet(this, 0));
                    } else {
                        requested = TERMINATED;
                        LOGGER.error("Failed to execute task on the executor {}. " +
                                        "Invoking Subscription (cancel()) in the caller thread. Subscription {}.",
                                executor, target, t);
                        target.cancel();
                    }
                    // We swallow the error here as we are forwarding the actual call and throwing from here will
                    // interrupt the control flow.
                }
            }
        }

        @Override
        public void run() {
            state = STATE_EXECUTING;
            for (;;) {
                long r = requestedUpdater.getAndSet(this, 0);
                if (r > 0) {
                    try {
                        target.request(r);
                        continue;
                    } catch (Throwable t) {
                        // Cancel since request-n threw.
                        requested = r = CANCELLED;
                        LOGGER.error("Unexpected exception from request(). Subscription {}.", target, t);
                    }
                }

                if (r == CANCELLED) {
                    requested = TERMINATED;
                    safeCancel(target);
                    return; // No more signals are required to be sent.
                } else if (r == TERMINATED) {
                    return; // we want to hard return to avoid resetting state.
                } else if (r != 0) {
                    // Invalid request-n
                    //
                    // As per spec (Rule 3.9) a request-n with n <= 0 MUST signal an onError hence terminating the
                    // Subscription. Since, we can not store negative values in requested and keep going without
                    // requesting more invalid values, we assume spec compliance (no more data can be requested) and
                    // terminate.
                    requested = TERMINATED;
                    try {
                        target.request(r);
                    } catch (IllegalArgumentException iae) {
                        // Expected
                    } catch (Throwable t) {
                        LOGGER.error("Ignoring unexpected exception from request(). Subscription {}.", target, t);
                    }
                    return;
                }
                // We store a request(0) as Long.MIN_VALUE so if we see r == 0 here, it means we are re-entering
                // the loop because we saw the STATE_ENQUEUED but we have already read from requested.

                for (;;) {
                    final int cState = state;
                    if (cState == STATE_EXECUTING) {
                        if (stateUpdater.compareAndSet(this, STATE_EXECUTING, STATE_IDLE)) {
                            return;
                        }
                    } else if (cState == STATE_ENQUEUED) {
                        if (stateUpdater.compareAndSet(this, STATE_ENQUEUED, STATE_EXECUTING)) {
                            break;
                        }
                    } else {
                        return;
                    }
                }
            }
        }
    }
}
