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
package io.trino.tempto.internal.context;

import io.trino.tempto.context.State;
import io.trino.tempto.context.TestContextCloseCallback;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GuiceTestContextTest
{
    private static final String A = "A";
    private static final String B = "B";

    @Test
    public void testGetDependency()
    {
        GuiceTestContext context = new GuiceTestContext();
        DummyState state = new DummyState();

        assertThat(context.createChildContext(state).getDependency(DummyState.class)).isEqualTo(state);
    }

    @Test
    public void testOverride()
    {
        DummyState state1 = new DummyState(A);
        DummyState state2 = new DummyState(B);
        GuiceTestContext context1 = new GuiceTestContext(binder -> binder.bind(DummyState.class).toInstance(state1));

        GuiceTestContext context2 = context1.createChildContext(
                List.of(),
                List.of(binder -> binder.bind(DummyState.class).toInstance(state2)));

        assertThat(context1.getDependency(DummyState.class)).isEqualTo(state1);
        assertThat(context2.getDependency(DummyState.class)).isEqualTo(state2);
    }

    @Test
    public void testSpawningNoNaming()
    {
        GuiceTestContext context = new GuiceTestContext();
        DummyState state = new DummyState();

        assertThat(context.createChildContext(state).getDependency(DummyState.class)).isEqualTo(state);
    }

    @Test
    public void testSpawningExternalNaming()
    {
        GuiceTestContext context = new GuiceTestContext();
        DummyState state = new DummyState(A);

        assertThat(context.createChildContext(state).getDependency(DummyState.class, A)).isEqualTo(state);
    }

    @Test
    public void testContextClose()
    {
        GuiceTestContext context1 = new GuiceTestContext();
        GuiceTestContext context2 = context1.createChildContext(List.of());

        TestContextCloseCallback callback1 = mock(TestContextCloseCallback.class);
        TestContextCloseCallback callback2 = mock(TestContextCloseCallback.class);

        context1.registerCloseCallback(callback1);
        context2.registerCloseCallback(callback2);

        // closing the parent cascades to the child, then fires the parent's callback
        context1.close();

        verify(callback2, times(1)).testContextClosed(context2);
        verify(callback1, times(1)).testContextClosed(context1);

        // child detached itself from parent on its first close, so each fires once more
        context2.close();
        context1.close();

        verify(callback2, times(2)).testContextClosed(context2);
        verify(callback1, times(2)).testContextClosed(context1);

        context2.close();

        verify(callback2, times(3)).testContextClosed(context2);
    }

    private static class DummyState
            implements State
    {
        private final Optional<String> name;

        DummyState()
        {
            this(null);
        }

        DummyState(String name)
        {
            this.name = Optional.ofNullable(name);
        }

        @Override
        public Optional<String> getName()
        {
            return name;
        }
    }
}
