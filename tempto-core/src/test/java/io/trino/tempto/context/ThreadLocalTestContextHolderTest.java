/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.tempto.context;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static io.trino.tempto.context.TestContextDsl.withChildTestContext;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.ThreadLocalTestContextException;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.assertTestContextNotSet;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.popAllTestContexts;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.popTestContext;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.pushTestContext;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThreadLocalTestContextHolderTest
{
    @AfterEach
    public void cleanup()
    {
        if (testContextIfSet().isPresent()) {
            popAllTestContexts();
        }
    }

    @Test
    public void assertNotSetDoesNotThrowIfUnset()
    {
        assertThatCode(() -> assertTestContextNotSet()).doesNotThrowAnyException();
    }

    @Test
    public void assertNotSetThrowsIfSet()
    {
        pushTestContext(mock(TestContext.class));

        assertThatThrownBy(() -> assertTestContextNotSet())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void gettingTestContextThrowsIfNotSet()
    {
        assertThatThrownBy(() -> testContext())
                .isInstanceOf(ThreadLocalTestContextException.class);
    }

    @Test
    public void gettingTestContextReturnsWhatWasSet()
    {
        TestContext mockTestContext = mock(TestContext.class);
        pushTestContext(mockTestContext);

        assertThat(testContext()).isEqualTo(mockTestContext);
    }

    @Test
    public void emptyTestContextShouldNotPropagateFromParentToChild()
            throws Throwable
    {
        TestContext mockTestContext = mock(TestContext.class);

        pushTestContext(mockTestContext);
        popTestContext();

        runAndJoin(() -> {
            assertTestContextNotSet();
            pushTestContext(mockTestContext);
        });

        assertTestContextNotSet();
    }

    @Test
    public void parentTestContextAfterChildStartShouldNotPropagateToChild()
            throws Throwable
    {
        TestContext mockTestContext = mock(TestContext.class);

        CountDownLatch latch = new CountDownLatch(1);
        Pair<Thread, List<Throwable>> threadAndThrowables = run(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            assertTestContextNotSet();
        });

        pushTestContext(mockTestContext);
        latch.countDown();

        join(threadAndThrowables);
    }

    @Test
    public void testContextShouldPropagateFromParentToChild()
            throws Throwable
    {
        TestContext mockTestContext = mock(TestContext.class);
        TestContext childTestContext = mock(TestContext.class);
        when(mockTestContext.createChildContext()).thenReturn(childTestContext);

        pushTestContext(mockTestContext);

        runAndJoin(() -> {
            assertThat(testContext()).isEqualTo(mockTestContext);
            popTestContext();
        });

        runAndJoin(() -> {
            assertThat(testContext()).isEqualTo(mockTestContext);
            popTestContext();
        });

        runAndJoin(withChildTestContext(() -> {
            assertThat(testContext()).isEqualTo(childTestContext);
            popTestContext();
        }));

        assertThat(testContext()).isEqualTo(mockTestContext);
    }

    private Pair<Thread, List<Throwable>> run(Runnable runnable)
    {
        List<Throwable> throwables = new ArrayList<>();
        Thread thread = new Thread(() -> {
            try {
                runnable.run();
            }
            catch (Throwable e) {
                throwables.add(e);
            }
        });

        thread.start();

        return Pair.of(thread, throwables);
    }

    private void join(Pair<Thread, List<Throwable>> threadAndThrowables)
            throws Throwable
    {
        threadAndThrowables.getLeft().join();

        if (!threadAndThrowables.getRight().isEmpty()) {
            throw threadAndThrowables.getRight().get(0);
        }
    }

    private void runAndJoin(Runnable runnable)
            throws Throwable
    {
        join(run(runnable));
    }
}
