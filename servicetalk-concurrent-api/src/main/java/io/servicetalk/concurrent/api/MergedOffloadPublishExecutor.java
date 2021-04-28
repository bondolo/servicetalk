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

/**
 * For scenarios where we need an {@link Executor} for selectively offloading publish signals, we need to merge
 * {@link Executor}s such that the publish signals are offloaded and other signals are executed on
 * the immediate executor */
final class MergedOffloadPublishExecutor extends DelegatingExecutor implements SignalOffloaderFactory {

    private final Executor immediateExecutor;

    /**
     * For scenarios where we need an {@link Executor} for selectively offloading some signals, we need to merge
     * {@link Executor}s so that we can use an appropriate {@link Executor} for offloading specific signals. This method
     * does such merging when the {@code publishOnExecutor} {@link Executor} only needs to offload publish signals.
     *
     * @param upstream {@link Executor} that we need to merge {@code upstream} with.
     * @param downstream {@link Executor} that is to be merged with {@code upstream}.
     * @return {@link Executor} which will use {@code fallback} for all signals that are not offloaded by
     * {@code publishOnExecutor}.
     */
    MergedOffloadPublishExecutor(final Executor downstream, final Executor upstream) {
        super(downstream);
        this.immediateExecutor = upstream;
    }

    @Override
    public SignalOffloader newSignalOffloader(final io.servicetalk.concurrent.Executor executor) {
        // This method is weird since we want to keep SignalOffloader internal but it has to be associated with the
        // the Executor. In practice, the Executor passed here should always be self when the SignalOffloaderFactory is
        // an Executor itself.
        assert executor == this;
        return new PublishOnlySignalOffloader(delegate(), immediateExecutor);
    }

    @Override
    public boolean hasThreadAffinity() {
        return SignalOffloaders.hasThreadAffinity(delegate()) && SignalOffloaders.hasThreadAffinity(immediateExecutor);
    }

    private static final class PublishOnlySignalOffloader implements SignalOffloader {

        private final SignalOffloader offloaded;
        private final SignalOffloader immediate;

        PublishOnlySignalOffloader(final Executor offloadExecutor, final Executor immediateExecutor) {
            offloaded = newOffloaderFor(offloadExecutor);
            this.immediate = newOffloaderFor(immediateExecutor);
        }

        @Override
        public <T> Subscriber<? super T> offloadSubscriber(final Subscriber<? super T> subscriber) {
            return offloaded.offloadSubscriber(subscriber);
        }

        @Override
        public <T> SingleSource.Subscriber<? super T> offloadSubscriber(
                final SingleSource.Subscriber<? super T> subscriber) {
            return offloaded.offloadSubscriber(subscriber);
        }

        @Override
        public CompletableSource.Subscriber offloadSubscriber(final CompletableSource.Subscriber subscriber) {
            return offloaded.offloadSubscriber(subscriber);
        }

        @Override
        public <T> Subscriber<? super T> offloadSubscription(final Subscriber<? super T> subscriber) {
            return immediate.offloadSubscription(subscriber);
        }

        @Override
        public <T> SingleSource.Subscriber<? super T> offloadCancellable(
                final SingleSource.Subscriber<? super T> subscriber) {
            return immediate.offloadCancellable(subscriber);
        }

        @Override
        public CompletableSource.Subscriber offloadCancellable(final CompletableSource.Subscriber subscriber) {
            return immediate.offloadCancellable(subscriber);
        }

        @Override
        public <T> void offloadSignal(final T signal, final Consumer<T> signalConsumer) {
            immediate.offloadSignal(signal, signalConsumer);
        }
    }
}
