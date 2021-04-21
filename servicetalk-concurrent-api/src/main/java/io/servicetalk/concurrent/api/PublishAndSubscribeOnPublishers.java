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
import io.servicetalk.concurrent.internal.QueueFullException;
import io.servicetalk.concurrent.internal.TerminalNotification;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.Nullable;

import static io.servicetalk.concurrent.internal.EmptySubscriptions.EMPTY_SUBSCRIPTION;
import static io.servicetalk.concurrent.internal.SubscriberUtils.deliverErrorFromSource;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeCancel;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeOnComplete;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeOnError;
import static io.servicetalk.utils.internal.PlatformDependent.newUnboundedSpscQueue;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

/**
 * A set of factory methods that provides implementations for the various publish/subscribeOn methods on
 * {@link Publisher}.
 */
final class PublishAndSubscribeOnPublishers {
    private static final Object NULL_WRAPPER = new Object();

    private PublishAndSubscribeOnPublishers() {
        // No instance.
    }

    static <T> void deliverOnSubscribeAndOnError(Subscriber<? super T> subscriber,
                                                 AsyncContextMap contextMap, AsyncContextProvider contextProvider,
                                                 Throwable cause) {
        deliverErrorFromSource(contextProvider.wrapPublisherSubscriber(subscriber, contextMap), cause);
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
        private final Publisher<T> original;
        private final Executor executor;

        PublishAndSubscribeOn(final Executor executor, final Publisher<T> original) {
            this.executor = executor;
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Publisher that is returned
            // by this operator.
            //
            // We use executor to create the returned Publisher which means executor will be used
            // to offload handleSubscribe as well as the Subscription that is sent to the subscriber here.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            original.subscribeWithSharedContext(contextProvider.wrapPublisherSubscriber(subscriber, contextMap));
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private static final class PublishOn<T> extends AbstractNoHandleSubscribePublisher<T> {
        private final Publisher<T> original;
        final Executor executor;

        PublishOn(final Executor executor, final Publisher<T> original) {
            this.original = original;
            this.executor = executor;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Publisher that is returned
            // by this operator.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            Subscriber<? super T> offloaded = new OffloadedSubscriber<>(subscriber, executor);
            original.subscribeWithSharedContext(contextProvider.wrapPublisherSubscriber(offloaded, contextMap));
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private static final class OffloadedSubscriber<T> implements Subscriber<T> {
        private static final int STATE_IDLE = 0;
        private static final int STATE_ENQUEUED = 1;
        private static final int STATE_EXECUTING = 2;
        private static final int STATE_TERMINATING = 3;
        private static final int STATE_TERMINATED = 4;
        @SuppressWarnings("rawtypes")
        private static final AtomicIntegerFieldUpdater<OffloadedSubscriber> stateUpdater =
                newUpdater(OffloadedSubscriber.class, "state");

        private volatile int state = STATE_IDLE;

        private final Subscriber<? super T> target;
        private final io.servicetalk.concurrent.Executor executor;
        private final Queue<Object> signals;
        // Set in onSubscribe before we enqueue the task which provides memory visibility inside the task.
        // Since any further action happens after onSubscribe, we always guarantee visibility of this field inside
        // run()
        @Nullable
        private PublisherSource.Subscription subscription;

        OffloadedSubscriber(final Subscriber<? super T> target, final io.servicetalk.concurrent.Executor executor) {
            this(target, executor, 2);
        }

        OffloadedSubscriber(final Subscriber<? super T> target, final io.servicetalk.concurrent.Executor executor,
                            final int publisherSignalQueueInitialCapacity) {
            this.target = target;
            this.executor = executor;
            // Queue is bounded by request-n
            signals = newUnboundedSpscQueue(publisherSignalQueueInitialCapacity);
        }

        @Override
        public void onSubscribe(final PublisherSource.Subscription s) {
            subscription = s;
            offerSignal(s);
        }

        @Override
        public void onNext(final T t) {
            offerSignal(t == null ? NULL_WRAPPER : t);
        }

        @Override
        public void onError(final Throwable t) {
            offerSignal(TerminalNotification.error(t));
        }

        @Override
        public void onComplete() {
            offerSignal(TerminalNotification.complete());
        }

        public void signal() {
            state = STATE_EXECUTING;
            for (;;) {
                Object signal;
                while ((signal = signals.poll()) != null) {
                    if (signal instanceof PublisherSource.Subscription) {
                        PublisherSource.Subscription subscription = (PublisherSource.Subscription) signal;
                        try {
                            target.onSubscribe(subscription);
                        } catch (Throwable t) {
                            clearSignalsFromExecutorThread();
                            safeOnError(target, t);
                            safeCancel(subscription);
                            return; // We can't interact with the queue any more because we terminated, so bail.
                        }
                    } else if (signal instanceof TerminalNotification) {
                        state = STATE_TERMINATED;
                        Throwable cause = ((TerminalNotification) signal).cause();
                        if (cause != null) {
                            safeOnError(target, cause);
                        } else {
                            safeOnComplete(target);
                        }
                        return; // We can't interact with the queue any more because we terminated, so bail.
                    } else {
                        @SuppressWarnings("unchecked")
                        T t = signal == NULL_WRAPPER ? null : (T) signal;
                        try {
                            target.onNext(t);
                        } catch (Throwable th) {
                            clearSignalsFromExecutorThread();
                            safeOnError(target, th);
                            assert subscription != null;
                            safeCancel(subscription);
                            return; // We can't interact with the queue any more because we terminated, so bail.
                        }
                    }
                }
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

        private void clearSignalsFromExecutorThread() {
            do {
                state = STATE_TERMINATING;
                signals.clear();
                // if we fail to go from draining to terminated, that means the state was set to interrupted by the
                // producer thread, and we need to try to drain from the queue again.
            } while (!stateUpdater.compareAndSet(this, STATE_TERMINATING, STATE_TERMINATED));
        }

        private void offerSignal(Object signal) {
            // We optimistically insert into the queue, and then clear elements from the queue later if there is an
            // error detected in the consumer thread.
            if (!signals.offer(signal)) {
                throw new QueueFullException("signals");
            }

            for (;;) {
                final int cState = state;
                if (cState == STATE_TERMINATED) {
                    // Once we have terminated, we are sure no other thread will be consuming from the queue and
                    // therefore we can consume (aka clear) the queue in this thread without violating the single
                    // consumer constraint.
                    signals.clear();
                    return;
                } else if (cState == STATE_TERMINATING) {
                    if (stateUpdater.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
                        // If another thread was draining the queue, and is no longer training the queue then the only
                        // state we can be in is STATE_TERMINATED. This means no other thread is consuming from the
                        // queue and we are safe to consume/clear it.
                        signals.clear();
                    }
                    return;
                } else if (stateUpdater.compareAndSet(this, cState, STATE_ENQUEUED)) {
                    if (cState == STATE_IDLE) {
                        break;
                    } else {
                        return;
                    }
                }
            }

            try {
                executor.execute(this::signal);
            } catch (Throwable t) {
                state = STATE_TERMINATED;
                try {
                    // As a policy, we call the target in the calling thread when the executor is inadequately
                    // provisioned. In the future we could make this configurable.
                    if (signal instanceof PublisherSource.Subscription) {
                        // Offloading of onSubscribe was rejected.
                        // If target throws here, we do not attempt to do anything else as spec has been violated.
                        target.onSubscribe(EMPTY_SUBSCRIPTION);
                    }
                } finally {
                    safeOnError(target, t);
                }
                // This is an SPSC queue; at this point we are sure that there is no other consumer of the queue
                // because:
                //  - We were in STATE_IDLE and hence the task isn't running.
                //  - The Executor threw from execute(), so we assume it will not run the task.
                signals.clear();
                assert subscription != null;
                safeCancel(subscription);
            }
        }
    }

    private static final class SubscribeOn<T> extends AbstractNoHandleSubscribePublisher<T> {
        private final Publisher<T> original;
        final Executor executor;

        SubscribeOn(final Executor executor, final Publisher<T> original) {
            this.original = original;
            this.executor = executor;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Publisher that is returned
            // by this operator.
            //
            // Subscription and handleSubscribe are offloaded at subscribe so we do not need to do anything specific
            // here.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            original.subscribeWithSharedContext(subscriber);
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }
}
