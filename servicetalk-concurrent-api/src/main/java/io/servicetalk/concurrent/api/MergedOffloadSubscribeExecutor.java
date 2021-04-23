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
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.CompletableSource;
import io.servicetalk.concurrent.PublisherSource.Subscriber;
import io.servicetalk.concurrent.SingleSource;
import io.servicetalk.concurrent.internal.SignalOffloader;
import io.servicetalk.concurrent.internal.SignalOffloaderFactory;
import io.servicetalk.concurrent.internal.SignalOffloaders;

import java.util.function.Consumer;

import static io.servicetalk.concurrent.internal.SignalOffloaders.newOffloaderFor;

final class MergedOffloadSubscribeExecutor extends DelegatingExecutor implements SignalOffloaderFactory {

    private final Executor downstream;

    MergedOffloadSubscribeExecutor(final Executor upstream, final Executor downstream) {
        super(upstream);
        this.downstream = downstream;
    }

    @Override
    public SignalOffloader newSignalOffloader(final io.servicetalk.concurrent.Executor executor) {
        // This method is weird since we want to keep SignalOffloader internal but it has to be associated with the
        // the Executor. In practice, the Executor passed here should always be self when the SignalOffloaderFactory is
        // an Executor itself.
        assert executor == this;
        return new SubscribeOnlySignalOffloader(delegate(), downstream);
    }

    @Override
    public boolean hasThreadAffinity() {
        return SignalOffloaders.hasThreadAffinity(delegate()) && SignalOffloaders.hasThreadAffinity(downstream);
    }

    private static final class SubscribeOnlySignalOffloader implements SignalOffloader {

        private final SignalOffloader offloaded;
        private final SignalOffloader immediate;

        SubscribeOnlySignalOffloader(final Executor subscribeOnExecutor, final Executor fallbackExecutor) {
            offloaded = newOffloaderFor(subscribeOnExecutor);
            immediate = newOffloaderFor(fallbackExecutor);
        }

        @Override
        public <T> Subscriber<? super T> offloadSubscriber(final Subscriber<? super T> subscriber) {
            return immediate.offloadSubscriber(subscriber);
        }

        @Override
        public <T> SingleSource.Subscriber<? super T> offloadSubscriber(
                final SingleSource.Subscriber<? super T> subscriber) {
            return immediate.offloadSubscriber(subscriber);
        }

        @Override
        public CompletableSource.Subscriber offloadSubscriber(
                final CompletableSource.Subscriber subscriber) {
            return immediate.offloadSubscriber(subscriber);
        }

        @Override
        public <T> Subscriber<? super T> offloadSubscription(final Subscriber<? super T> subscriber) {
            return offloaded.offloadSubscription(subscriber);
        }

        @Override
        public <T> SingleSource.Subscriber<? super T> offloadCancellable(
                final SingleSource.Subscriber<? super T> subscriber) {
            return offloaded.offloadCancellable(subscriber);
        }

        @Override
        public CompletableSource.Subscriber offloadCancellable(
                final CompletableSource.Subscriber subscriber) {
            return offloaded.offloadCancellable(subscriber);
        }

        @Override
        public <T> void offloadSubscribe(final Subscriber<? super T> subscriber,
                                         final Consumer<Subscriber<? super T>> handleSubscribe) {
            offloaded.offloadSubscribe(subscriber, handleSubscribe);
        }

        @Override
        public <T> void offloadSubscribe(final SingleSource.Subscriber<? super T> subscriber,
                                         final Consumer<SingleSource.Subscriber<? super T>> handleSubscribe) {
            offloaded.offloadSubscribe(subscriber, handleSubscribe);
        }

        @Override
        public void offloadSubscribe(final CompletableSource.Subscriber subscriber,
                                     final Consumer<CompletableSource.Subscriber> handleSubscribe) {
            offloaded.offloadSubscribe(subscriber, handleSubscribe);
        }

        @Override
        public <T> void offloadSignal(final T signal, final Consumer<T> signalConsumer) {
            offloaded.offloadSignal(signal, signalConsumer);
        }
    }
}
