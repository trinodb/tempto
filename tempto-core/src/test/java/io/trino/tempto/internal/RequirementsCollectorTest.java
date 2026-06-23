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

package io.trino.tempto.internal;

import com.google.common.collect.ImmutableSet;
import io.trino.tempto.CompositeRequirement;
import io.trino.tempto.Requirement;
import io.trino.tempto.Requirements;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.internal.configuration.YamlConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.stream.Stream;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.fulfillment.command.SuiteCommandRequirement.suiteCommand;
import static io.trino.tempto.fulfillment.command.TestCommandRequirement.testCommand;
import static io.trino.tempto.internal.configuration.EmptyConfiguration.emptyConfiguration;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class RequirementsCollectorTest
{
    private static final Requirement A = req("a");
    private static final Requirement B = req("b");
    private static final Requirement C = req("c");
    private static final Requirement D = req("d");

    private final DefaultRequirementsCollector requirementsCollector = new DefaultRequirementsCollector(emptyConfiguration());

    @ParameterizedTest
    @MethodSource("listMethodRequirementsDataProvider")
    public void shouldListMethodRequirements(Method method, Set<Set<Requirement>> expectedRequirementSets)
    {
        assertThat(requirementsCollector.collect(method).getRequirementsSets()).isEqualTo(expectedRequirementSets);
    }

    static Stream<Arguments> listMethodRequirementsDataProvider()
            throws NoSuchMethodException
    {
        return Stream.of(
                Arguments.of(MethodRequirement.class.getMethod("method"), setOf(setOf(A))),
                Arguments.of(ClassRequirement.class.getMethod("method"), setOf(setOf(B))),
                Arguments.of(MethodAndClassRequirement.class.getMethod("method"), setOf(setOf(A, B))));
    }

    @ParameterizedTest
    @MethodSource("composeRequirementsDataProvider")
    public void shouldComposeRequirements(Requirement requirement, Set<Set<Requirement>> expectedRequirementSets)
    {
        assertThat(((CompositeRequirement) requirement).getRequirementsSets()).isEqualTo(expectedRequirementSets);
    }

    static Stream<Arguments> composeRequirementsDataProvider()
    {
        return Stream.of(
                Arguments.of(compose(), setOf(setOf())),
                Arguments.of(compose(A, B), setOf(setOf(A, B))),
                Arguments.of(Requirements.allOf(A, B), setOf(setOf(A), setOf(B))),
                Arguments.of(compose(C, Requirements.allOf(A, B)), setOf(setOf(A, C), setOf(B, C))),
                Arguments.of(compose(Requirements.allOf(C, D), Requirements.allOf(A, B)), setOf(setOf(A, C), setOf(B, C), setOf(D, A), setOf(D, B))),
                Arguments.of(compose(Requirements.allOf(C, Requirements.allOf(A, B)), D), setOf(setOf(C, D), setOf(A, D), setOf(B, D))),
                Arguments.of(compose(A, Requirements.allOf(compose(B, C), D)), setOf(setOf(A, B, C), setOf(A, D))));
    }

    @Test
    public void shouldProvideCommandRequirementsFromConfiguration()
            throws NoSuchMethodException
    {
        DefaultRequirementsCollector requirementsCollector = new DefaultRequirementsCollector(new YamlConfiguration("""
                command:
                  test:
                    - test command
                  suite:
                    - suite command
                """));
        Method method = MethodRequirement.class.getMethod("method");
        Set<Set<Requirement>> expectedRequirementSets = setOf(setOf(A, testCommand("test command"), suiteCommand("suite command")));

        assertThat(requirementsCollector.collect(method).getRequirementsSets()).isEqualTo(expectedRequirementSets);
    }

    private static Requirement req(String name)
    {
        return new DummyTestRequirement(name);
    }

    private static <E> Set<E> setOf(E e)
    {
        return ImmutableSet.of(e);
    }

    @SafeVarargs
    private static <E> Set<E> setOf(E... elems)
    {
        return ImmutableSet.<E>builder().addAll(asList(elems)).build();
    }

    private static class MethodRequirement
    {
        @Requires(ProviderA.class)
        public void method()
        {
        }
    }

    @Requires(ProviderB.class)
    private static class ClassRequirement
    {
        public void method()
        {
        }
    }

    @Requires(ProviderA.class)
    private static class MethodAndClassRequirement
    {
        @Requires(ProviderB.class)
        public void method()
        {
        }
    }

    private static class ProviderA
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return A;
        }
    }

    private static class ProviderB
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return B;
        }
    }
}
