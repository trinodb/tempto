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
package io.trino.tempto.initialization;

import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Describes a single test method about to be executed. This is the Tempto-owned replacement for
 * TestNG's {@code ITestResult} that used to be passed to {@link TestMethodModuleProvider}.
 */
public record TestMethodInfo(
        String testName,
        Set<String> testGroups,
        Class<?> testClass,
        Optional<Method> testMethod,
        Object testInstance)
{
    public TestMethodInfo
    {
        requireNonNull(testName, "testName is null");
        testGroups = ImmutableSet.copyOf(requireNonNull(testGroups, "testGroups is null"));
        requireNonNull(testClass, "testClass is null");
        requireNonNull(testMethod, "testMethod is null");
        requireNonNull(testInstance, "testInstance is null");
    }
}
