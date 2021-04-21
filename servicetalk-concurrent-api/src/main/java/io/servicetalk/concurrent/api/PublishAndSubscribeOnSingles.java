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
import io.servicetalk.concurrent.SingleSource;
import io.servicetalk.concurrent.internal.TerminalNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import javax.annotation.Nullable;

import static io.servicetalk.concurrent.Cancellable.IGNORE_CANCEL;
import static io.servicetalk.concurrent.internal.SubscriberUtils.deliverErrorFromSource;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeCancel;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeOnError;
import static io.servicetalk.concurrent.internal.SubscriberUtils.safeOnSuccess;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

/**
 * A set of factory methods that provides implementations for the various publish/subscribeOn methods on
 * {@link Single}.
 */
final class PublishAndSubscribeOnSingles {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublishAndSubscribeOnSingles.class);
    private static final Object NULL_WRAPPER = new Object();

    private PublishAndSubscribeOnSingles() {
        // No instance.
    }

    static <T> void deliverOnSubscribeAndOnError(SingleSource.Subscriber<? super T> subscriber,
                                                 AsyncContextMap contextMap,
                                                 AsyncContextProvider contextProvider, Throwable cause) {
        deliverErrorFromSource(
                contextProvider.wrapSingleSubscriber(subscriber, contextMap), cause);
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
        void handleSubscribe(final Subscriber<? super T> subscriber,
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
                    contextProvider.wrapSingleSubscriber(subscriber, contextMap), contextProvider);
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
            this.executor = executor;
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Single that is returned
            // by this operator.
            //
            // Here we offload signals from original to subscriber using signalOffloader.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            Subscriber<? super T> offloaded = new OffloadedSingleSubscriber<>(executor, subscriber);
            original.subscribeWithSharedContext(
                    contextProvider.wrapSingleSubscriber(offloaded, contextMap), contextProvider);
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }

    private abstract static class AbstractOffloadedSingleValueSubscriber implements Runnable {
        private static final int ON_SUBSCRIBE_RECEIVED_MASK = 8;
        private static final int EXECUTING_MASK = 16;
        private static final int RECEIVED_TERMINAL_MASK = 32;
        private static final int EXECUTING_SUBSCRIBED_RECEIVED_MASK = EXECUTING_MASK | ON_SUBSCRIBE_RECEIVED_MASK;

        private static final int STATE_INIT = 0;
        private static final int STATE_AWAITING_TERMINAL = 1;
        private static final int STATE_TERMINATED = 2;
        private static final AtomicIntegerFieldUpdater<AbstractOffloadedSingleValueSubscriber> stateUpdater =
                newUpdater(AbstractOffloadedSingleValueSubscriber.class, "state");

        final io.servicetalk.concurrent.Executor executor;
        @Nullable
        // Visibility: Task submitted to executor happens-before task execution.
        private Cancellable cancellable;
        @Nullable
        private Object terminal;
        private volatile int state = STATE_INIT;

        AbstractOffloadedSingleValueSubscriber(final io.servicetalk.concurrent.Executor executor) {
            this.executor = executor;
        }

        public final void onSubscribe(final Cancellable cancellable) {
            this.cancellable = cancellable;
            state = ON_SUBSCRIBE_RECEIVED_MASK;
            try {
                executor.execute(this);
            } catch (Throwable t) {
                // As a policy, we call the target in the calling thread when the executor is inadequately
                // provisioned. In the future we could make this configurable.
                state = STATE_TERMINATED;
                sendOnSubscribe(IGNORE_CANCEL);
                terminateOnEnqueueFailure(t);
            }
        }

        @Override
        public final void run() {
            for (;;) {
                int cState = state;
                if (cState == STATE_TERMINATED) {
                    return;
                }
                if (!casAppend(cState, EXECUTING_MASK)) {
                    continue;
                }
                cState |= EXECUTING_MASK;
                if (has(cState, ON_SUBSCRIBE_RECEIVED_MASK)) {
                    while (!stateUpdater.compareAndSet(this, cState, (cState & ~ON_SUBSCRIBE_RECEIVED_MASK))) {
                        cState = state;
                    }
                    assert cancellable != null;
                    sendOnSubscribe(cancellable);
                    // Re-read state to see if we terminated from onSubscribe
                    cState = state;
                }
                if (has(cState, RECEIVED_TERMINAL_MASK)) {
                    if (stateUpdater.compareAndSet(this, cState, STATE_TERMINATED)) {
                        assert terminal != null;
                        deliverTerminalToSubscriber(terminal);
                        return;
                    }
                } else if (stateUpdater.compareAndSet(this, cState, STATE_AWAITING_TERMINAL)) {
                    return;
                }
            }
        }

        final void terminal(final Object terminal) {
            this.terminal = terminal;
            for (;;) {
                int cState = state;
                if (// Duplicate terminal event
                        has(cState, RECEIVED_TERMINAL_MASK) || cState == STATE_TERMINATED ||
                                // Already executing or enqueued for executing, append the state.
                                hasAny(cState, EXECUTING_SUBSCRIBED_RECEIVED_MASK) &&
                                        casAppend(cState, RECEIVED_TERMINAL_MASK)) {
                    return;
                } else if ((cState == STATE_AWAITING_TERMINAL || cState == STATE_INIT) &&
                        stateUpdater.compareAndSet(this, cState, RECEIVED_TERMINAL_MASK)) {
                    // Either we have seen onSubscribe and the Runnable is no longer executing, or we have not
                    // seen onSubscribe and there is a sequencing issue on the Subscriber. Either way we avoid looping
                    // and deliver the terminal event.
                    try {
                        executor.execute(this);
                    } catch (Throwable t) {
                        state = STATE_TERMINATED;
                        // As a policy, we call the target in the calling thread when the executor is inadequately
                        // provisioned. In the future we could make this configurable.
                        terminateOnEnqueueFailure(t);
                    }
                    return;
                }
            }
        }

        final void onSubscribeFailed() {
            state = STATE_TERMINATED;
        }

        abstract void terminateOnEnqueueFailure(Throwable cause);

        abstract void deliverTerminalToSubscriber(Object terminal);

        abstract void sendOnSubscribe(Cancellable cancellable);

        private static boolean has(int state, int flag) {
            return (state & flag) == flag;
        }

        private static boolean hasAny(int state, int flag) {
            return (state & flag) != 0;
        }

        private boolean casAppend(int cState, int toAppend) {
            return stateUpdater.compareAndSet(this, cState, (cState | toAppend));
        }
    }

    private static final class OffloadedSingleSubscriber<T> extends AbstractOffloadedSingleValueSubscriber
            implements SingleSource.Subscriber<T> {
        private final SingleSource.Subscriber<T> target;

        OffloadedSingleSubscriber(final io.servicetalk.concurrent.Executor executor,
                                  final SingleSource.Subscriber<T> target) {
            super(executor);
            this.target = requireNonNull(target);
        }

        @Override
        public void onSuccess(@Nullable final T result) {
            terminal(result == null ? NULL_WRAPPER : result);
        }

        @Override
        public void onError(final Throwable t) {
            terminal(TerminalNotification.error(t));
        }

        @Override
        void terminateOnEnqueueFailure(final Throwable cause) {
            LOGGER.error("Failed to execute task on the executor {}. " +
                    "Invoking Subscriber (onError()) in the caller thread. Subscriber {}.", executor, target, cause);
            target.onError(cause);
        }

        @Override
        void deliverTerminalToSubscriber(final Object terminal) {
            if (terminal instanceof TerminalNotification) {
                final Throwable error = ((TerminalNotification) terminal).cause();
                assert error != null;
                safeOnError(target, error);
            } else {
                safeOnSuccess(target, uncheckCast(terminal));
            }
        }

        @Override
        void sendOnSubscribe(final Cancellable cancellable) {
            try {
                target.onSubscribe(cancellable);
            } catch (Throwable t) {
                onSubscribeFailed();
                safeOnError(target, t);
                safeCancel(cancellable);
            }
        }

        @Nullable
        @SuppressWarnings("unchecked")
        private T uncheckCast(final Object signal) {
            return signal == NULL_WRAPPER ? null : (T) signal;
        }
    }

    private static final class SubscribeOn<T> extends AbstractNoHandleSubscribeSingle<T> {
        private final Executor executor;
        private final Single<T> original;

        SubscribeOn(final Executor executor, final Single<T> original) {
            this.executor = executor;
            this.original = original;
        }

        @Override
        void handleSubscribe(final Subscriber<? super T> subscriber,
                             final AsyncContextMap contextMap, final AsyncContextProvider contextProvider) {
            // This operator is to make sure that we use the executor to subscribe to the Single that is returned
            // by this operator.
            //
            // Subscription and handleSubscribe are offloaded at subscribe so we do not need to do anything specific
            // here.
            //
            // This operator acts as a boundary that changes the Executor from original to the rest of the execution
            // chain. If there is already an Executor defined for original, it will be used to offload signals until
            // they hit this operator.
            original.subscribeWithSharedContext(subscriber, contextProvider);
        }

        @Override
        public Executor executor() {
            return executor;
        }
    }
}
