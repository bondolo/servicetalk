/*
 * Copyright © 2019 Apple Inc. and the ServiceTalk project authors
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
package io.servicetalk.concurrent.api.internal;

import io.servicetalk.concurrent.Cancellable;
import io.servicetalk.concurrent.api.Completable;
import io.servicetalk.concurrent.api.Executor;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * An {@link Executor}.
 */
public final class DelegateExecutor implements Executor {

    private final Executor delegate;

    /**
     * New instance.
     *
     * @param delegate Actual {@link Executor} to use.
     */
    public DelegateExecutor(final Executor delegate) {
        this.delegate = requireNonNull(delegate);
    }

    @Override
    public Cancellable execute(final Runnable task) throws RejectedExecutionException {
        return delegate.execute(task);
    }

    @Override
    public Cancellable schedule(final Runnable task, final long delay, final TimeUnit unit)
            throws RejectedExecutionException {
        return delegate.schedule(task, delay, unit);
    }

    @Override
    public Completable onClose() {
        return delegate.onClose();
    }

    @Override
    public Completable closeAsync() {
        return delegate.closeAsync();
    }
}
