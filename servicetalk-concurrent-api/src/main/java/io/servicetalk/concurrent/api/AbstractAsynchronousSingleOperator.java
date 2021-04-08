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

import static java.util.Objects.requireNonNull;

/**
 * Base class for operators on a {@link Single} that process signals asynchronously hence in order to guarantee safe
 * downstream invocations require to wrap their {@link Subscriber}s with a {@link SignalOffloader}.
 * Operators that process signals synchronously can use {@link AbstractSynchronousSingleOperator} to avoid wrapping
 * their {@link Subscriber}s and hence reduce object allocation.
 *
 * @param <T> Type of original {@link Single}.
 * @param <R> Type of {@link Single} returned by the operator.
 *
 * @see AbstractSynchronousSingleOperator
 */
abstract class AbstractAsynchronousSingleOperator<T, R> extends AbstractNoHandleSubscribeSingle<R>
        implements SingleOperator<T, R> {

    private final Single<T> original;

    AbstractAsynchronousSingleOperator(Single<T> original) {
        this.original = requireNonNull(original);
    }

    @Override
    final void handleSubscribe(Subscriber<? super R> subscriber,
                               AsyncContextMap contextMap, AsyncContextProvider contextProvider) {
        // Offload signals to the passed Subscriber making sure they are not invoked in the thread that
        // asynchronously processes signals. This is because the thread that processes the signals may have different
        // thread safety characteristics than the typical thread interacting with the execution chain.
        //
        // The AsyncContext needs to be preserved when ever we interact with the original Subscriber, so we wrap it here
        // with the original contextMap. Otherwise some other context may leak into this subscriber chain from the other
        // side of the asynchronous boundary.
        final Subscriber<? super R> operatorSubscriber =
                contextProvider.wrapSingleSubscriberAndCancellable(subscriber, contextMap);
        // Subscriber to use to subscribe to the original source. Since this is an asynchronous operator, it may call
        // Cancellable method from EventLoop (if the asynchronous source created/obtained inside this operator uses
        // EventLoop) which may execute blocking code on EventLoop, eg: beforeCancel(). So, we should offload
        // Cancellable method here.
        //
        // We are introducing offloading on the Subscription, which means the AsyncContext may leak if we don't save
        // and restore the AsyncContext before/after the asynchronous boundary.
        final Subscriber<? super T> upstreamSubscriber = apply(operatorSubscriber);
        original.delegateSubscribe(upstreamSubscriber, contextMap, contextProvider);
    }
}
