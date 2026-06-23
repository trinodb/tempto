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
package io.trino.tempto.internal.initialization;

import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.internal.TestSpecificRequirementsResolver;
import io.trino.tempto.internal.convention.ConventionBasedTest;
import io.trino.tempto.internal.convention.ConventionBasedTestFactory;
import io.trino.tempto.internal.convention.ConventionBasedTests;
import io.trino.tempto.internal.listeners.TestNameGroupNameFilter;
import org.junit.platform.commons.support.ReflectionSupport;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Resolves the union of all requirements across every test in a {@link TestPlan} (regular product
 * tests plus convention-based tests). This is what suite-level fulfillment is performed against.
 */
final class AllTestsRequirementsResolver
{
    private static final Logger LOGGER = getLogger(AllTestsRequirementsResolver.class);

    private AllTestsRequirementsResolver() {}

    static boolean containsTemptoTests(TestPlan testPlan)
    {
        return testPlan.getRoots().stream()
                .flatMap(root -> testPlan.getDescendants(root).stream())
                .anyMatch(identifier -> sourceClass(identifier).map(AllTestsRequirementsResolver::isTemptoTestClass).orElse(false));
    }

    static Set<Requirement> resolveAllRequirements(TestPlan testPlan, Configuration configuration)
    {
        TestSpecificRequirementsResolver resolver = new TestSpecificRequirementsResolver(configuration);
        Set<Requirement> allRequirements = new HashSet<>();
        boolean hasConventionTests = false;

        for (TestIdentifier root : testPlan.getRoots()) {
            for (TestIdentifier identifier : testPlan.getDescendants(root)) {
                Optional<Class<?>> sourceClass = sourceClass(identifier);
                if (sourceClass.isEmpty()) {
                    continue;
                }
                Class<?> clazz = sourceClass.get();
                if (clazz.equals(ConventionBasedTests.class)) {
                    hasConventionTests = true;
                    continue;
                }
                if (!ProductTest.class.isAssignableFrom(clazz)) {
                    continue;
                }
                sourceMethod(identifier).ifPresent(method ->
                        addAll(allRequirements, resolver, method, instantiateIfRequirementsProvider(clazz)));
            }
        }

        if (hasConventionTests) {
            addConventionTestRequirements(allRequirements, resolver);
        }

        return allRequirements;
    }

    private static boolean isTemptoTestClass(Class<?> clazz)
    {
        return ProductTest.class.isAssignableFrom(clazz) || clazz.equals(ConventionBasedTests.class);
    }

    private static Optional<Class<?>> sourceClass(TestIdentifier identifier)
    {
        TestSource source = identifier.getSource().orElse(null);
        try {
            if (source instanceof MethodSource methodSource) {
                return Optional.of(methodSource.getJavaClass());
            }
            if (source instanceof ClassSource classSource) {
                return Optional.of(classSource.getJavaClass());
            }
        }
        catch (Throwable e) {
            LOGGER.debug("Could not load test class for {}", source, e);
        }
        return Optional.empty();
    }

    private static Optional<Method> sourceMethod(TestIdentifier identifier)
    {
        if (!(identifier.getSource().orElse(null) instanceof MethodSource methodSource)) {
            return Optional.empty();
        }
        try {
            return Optional.of(methodSource.getJavaMethod());
        }
        catch (Throwable e) {
            LOGGER.debug("Could not load test method for {}", methodSource, e);
            return Optional.empty();
        }
    }

    private static Optional<Object> instantiateIfRequirementsProvider(Class<?> clazz)
    {
        if (!RequirementsProvider.class.isAssignableFrom(clazz)) {
            return Optional.empty();
        }
        try {
            return Optional.of(ReflectionSupport.newInstance(clazz));
        }
        catch (Throwable e) {
            LOGGER.debug("Could not instantiate {} to resolve suite-level requirements", clazz, e);
            return Optional.empty();
        }
    }

    private static void addConventionTestRequirements(Set<Requirement> allRequirements, TestSpecificRequirementsResolver resolver)
    {
        try {
            // Convention tests are not part of the statically discovered TestPlan (they become
            // @TestTemplate invocations at execution time), so the launcher's name/group filter has
            // not been applied to them yet. Use the shared, already-filtered discovery so suite-level
            // fulfillment matches exactly the convention tests that will run.
            Method testMethod = ConventionBasedTest.class.getMethod("test");
            for (ConventionBasedTest conventionTest : ConventionBasedTestFactory.selectedConventionTests(TestNameGroupNameFilter.fromSystemProperties())) {
                addAll(allRequirements, resolver, testMethod, Optional.of(conventionTest));
            }
        }
        catch (RuntimeException | NoSuchMethodException e) {
            LOGGER.warn("Could not resolve convention test requirements for suite-level fulfillment", e);
        }
    }

    private static void addAll(Set<Requirement> allRequirements, TestSpecificRequirementsResolver resolver, Method method, Optional<Object> instance)
    {
        for (Set<Requirement> requirementsSet : resolver.resolve(method, instance)) {
            allRequirements.addAll(requirementsSet);
        }
    }
}
