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
package io.prestosql.tempto.internal.initialization;

import io.prestosql.tempto.context.State;
import io.prestosql.tempto.context.TestContext;
import io.prestosql.tempto.context.TestContextCloseCallback;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ForwardingTestContext
        implements TestContext
{
    private final TestContext delegate;

    public ForwardingTestContext(TestContext delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public <T> T getDependency(Class<T> dependencyClass)
    {
        return delegate.getDependency(dependencyClass);
    }

    @Override
    public <T> T getDependency(Class<T> dependencyClass, String dependencyName)
    {
        return delegate.getDependency(dependencyClass, dependencyName);
    }

    @Override
    public <T> Optional<T> getOptionalDependency(Class<T> dependencyClass)
    {
        return delegate.getOptionalDependency(dependencyClass);
    }

    @Override
    public <T> Optional<T> getOptionalDependency(Class<T> dependencyClass, String dependencyName)
    {
        return delegate.getOptionalDependency(dependencyClass, dependencyName);
    }

    @Override
    public TestContext createChildContext(Iterable<State> states)
    {
        return delegate.createChildContext(states);
    }

    @Override
    public void registerCloseCallback(TestContextCloseCallback callback)
    {
        delegate.registerCloseCallback(callback);
    }

    @Override
    public void injectMembers(Object instance)
    {
        delegate.injectMembers(instance);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
