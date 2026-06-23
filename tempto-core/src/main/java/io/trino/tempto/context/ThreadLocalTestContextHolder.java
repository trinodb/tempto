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

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;

/**
 * Static helper for holding the current {@link TestContext} stack in a thread local variable. The
 * stack is a {@link Deque} whose <em>tail</em> is the top of the stack ({@link Deque#addLast}/
 * {@link Deque#removeLast}/{@link Deque#getLast}), so iterating it yields contexts bottom-to-top.
 * <p>
 * Justification for existence:
 * <p>
 * Using thread local for holding current TestContext is less explicit
 * and a bit hacky. But allows for having less verbose test building blocks.
 * <p>
 * We also do not require users to subclass a common context-aware test class
 * when they write tests.
 */
public final class ThreadLocalTestContextHolder
{
    private static final ThreadLocal<Deque<TestContext>> testContextStackThreadLocal = new InheritableThreadLocal<>()
    {
        @Override
        protected Deque<TestContext> childValue(Deque<TestContext> parentTestContextStack)
        {
            if (parentTestContextStack != null) {
                checkState(!parentTestContextStack.isEmpty());
                Deque<TestContext> childTestContextStack = new ArrayDeque<>();
                childTestContextStack.addLast(parentTestContextStack.getLast());
                return childTestContextStack;
            }

            return null;
        }
    };

    public static TestContext testContext()
    {
        assertTestContextSet();
        return testContextStackThreadLocal.get().getLast();
    }

    public static Optional<TestContext> testContextIfSet()
    {
        if (testContextStackThreadLocal.get() == null) {
            return Optional.empty();
        }

        return Optional.of(testContext());
    }

    public static void pushTestContext(TestContext testContext)
    {
        ensureTestContextStack();
        testContextStackThreadLocal.get().addLast(testContext);
    }

    public static TestContext popTestContext()
    {
        assertTestContextSet();

        Deque<TestContext> testContextStack = testContextStackThreadLocal.get();
        TestContext testContext = testContextStack.removeLast();
        if (testContextStack.isEmpty()) {
            testContextStackThreadLocal.remove();
        }

        return testContext;
    }

    public static void pushAllTestContexts(Deque<? extends TestContext> testContextStack)
    {
        testContextStack.forEach(ThreadLocalTestContextHolder::pushTestContext);
    }

    public static Deque<TestContext> popAllTestContexts()
    {
        Deque<TestContext> testContextStack = testContextStackThreadLocal.get();
        testContextStackThreadLocal.remove();
        return testContextStack;
    }

    /**
     * Detaches and returns whatever context stack is currently bound to this thread (if any),
     * leaving the thread with no context. Pair with {@link #restoreTestContexts}.
     * <p>
     * Needed because JUnit's parallel executor uses a work-stealing {@code ForkJoinPool}: a worker
     * thread can begin running a test while it is already in the middle of another one (during a
     * join). Saving and restoring the previously bound context lets nested tests coexist on the same
     * thread without clobbering each other.
     */
    public static Optional<Deque<TestContext>> saveAndClearTestContexts()
    {
        Deque<TestContext> testContextStack = testContextStackThreadLocal.get();
        testContextStackThreadLocal.remove();
        return Optional.ofNullable(testContextStack);
    }

    public static void restoreTestContexts(Optional<Deque<TestContext>> testContexts)
    {
        if (testContexts.isPresent()) {
            testContextStackThreadLocal.set(testContexts.get());
        }
        else {
            testContextStackThreadLocal.remove();
        }
    }

    public static void assertTestContextNotSet()
    {
        checkState(testContextStackThreadLocal.get() == null, "test context should not be set for current thread");
    }

    public static void assertTestContextSet()
    {
        if (testContextStackThreadLocal.get() == null || testContextStackThreadLocal.get().isEmpty()) {
            throw new ThreadLocalTestContextException("test context not set for current thread");
        }
    }

    private static void ensureTestContextStack()
    {
        if (testContextStackThreadLocal.get() == null) {
            testContextStackThreadLocal.set(new ArrayDeque<>());
        }
    }

    private ThreadLocalTestContextHolder() {}

    @VisibleForTesting
    static class ThreadLocalTestContextException
            extends RuntimeException
    {
        private static final AtomicLong instanceCount = new AtomicLong();

        ThreadLocalTestContextException(String message)
        {
            super(message,
                    null,
                    true,
                    // Suppress stacktrace for all but first 10 exceptions. It is not useful when printed for every test.
                    instanceCount.getAndIncrement() < 10);
        }
    }
}
