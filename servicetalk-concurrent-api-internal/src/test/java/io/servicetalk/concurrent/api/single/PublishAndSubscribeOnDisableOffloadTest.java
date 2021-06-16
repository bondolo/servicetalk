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
package io.servicetalk.concurrent.api.single;

import io.servicetalk.concurrent.api.Single;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReferenceArray;

import static io.servicetalk.concurrent.api.Executors.immediate;
import static org.hamcrest.MatcherAssert.assertThat;

public class PublishAndSubscribeOnDisableOffloadTest extends AbstractSinglePublishAndSubscribeOnTest {

    @Test
    public void testDefault() throws InterruptedException {
        Thread[] capturedThreads = setupAndSubscribe((s, e) -> s, immediate());
        assertThat("Unexpected threads for original source.",
                capturedThreads[ORIGINAL_SUBSCRIBER_THREAD], APP_EXECUTOR);
        assertThat("Unexpected threads for offloaded source.",
                capturedThreads[TERMINAL_SIGNAL_THREAD], APP_EXECUTOR);
    }

    @Test
    public void testPublishOnDisable() throws InterruptedException {
        Thread[] capturedThreads = setupAndSubscribe(Single::publishOnOverride, immediate());
        assertThat("Unexpected threads for original and offloaded source.",
                capturedThreads[ORIGINAL_SUBSCRIBER_THREAD], APP_EXECUTOR);
    }

    @Test
    public void testSubscribeOnDisable() throws InterruptedException {
        Thread[] capturedThreads = setupAndSubscribe(Single::subscribeOnOverride, immediate());
        assertThat("Unexpected threads for original and offloaded source.",
                capturedThreads[ORIGINAL_SUBSCRIBER_THREAD], APP_EXECUTOR);
    }

    @Test
    public void testPublishAndSubscribeOnDisable() throws InterruptedException {
        Thread[] capturedThreads = setupAndSubscribe(Single::publishAndSubscribeOnOverride, immediate());
        assertThat("Unexpected threads for original and offloaded source.",
                capturedThreads[ORIGINAL_SUBSCRIBER_THREAD], APP_EXECUTOR);
    }

    @Test
    public void testPublishAndSubscribeOnDisableWithCancel() throws InterruptedException {
        Thread[] capturedThreads = setupForCancelAndSubscribe(Single::publishAndSubscribeOnOverride, immediate());
        assertThat("Unexpected threads for original and offloaded source.",
                capturedThreads[ORIGINAL_SUBSCRIBER_THREAD], APP_EXECUTOR);
    }
}
