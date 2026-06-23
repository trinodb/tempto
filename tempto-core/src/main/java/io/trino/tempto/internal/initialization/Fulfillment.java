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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import io.trino.tempto.Requirement;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.fulfillment.RequirementFulfiller;
import io.trino.tempto.fulfillment.TestStatus;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.TableManagerDispatcher;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.reverse;
import static com.google.inject.util.Modules.combine;
import static io.trino.tempto.context.TestContextDsl.runWithTestContext;
import static io.trino.tempto.fulfillment.TestStatus.FAILURE;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Shared requirement fulfillment/cleanup logic used both at suite level
 * ({@link SuiteState}) and at test-method level ({@link TemptoTestRunner}).
 */
final class Fulfillment
{
    private static final Logger LOGGER = getLogger(Fulfillment.class);

    private Fulfillment() {}

    static void fulfill(
            Deque<TestContext> testContextStack,
            List<Class<? extends RequirementFulfiller>> fulfillerClasses,
            Set<Requirement> requirements)
    {
        List<Class<? extends RequirementFulfiller>> successfulFulfillerClasses = new ArrayList<>();

        try {
            for (Class<? extends RequirementFulfiller> fulfillerClass : fulfillerClasses) {
                LOGGER.debug("Fulfilling using {}", fulfillerClass);
                TestContext testContext = testContextStack.getLast();
                runWithTestContext(testContext, () -> {
                    RequirementFulfiller fulfiller = testContext.getDependency(fulfillerClass);
                    TestContext testContextWithNewStates = testContext.createChildContext(fulfiller.fulfill(requirements));
                    successfulFulfillerClasses.add(fulfillerClass);
                    testContextStack.addLast(testContextWithNewStates);
                });
            }
        }
        catch (RuntimeException e) {
            LOGGER.debug("error during fulfillment", e);
            try {
                cleanup(testContextStack, successfulFulfillerClasses, FAILURE);
            }
            catch (RuntimeException cleanupException) {
                e.addSuppressed(cleanupException);
            }
            throw e;
        }
    }

    static void cleanup(
            Deque<TestContext> testContextStack,
            List<Class<? extends RequirementFulfiller>> fulfillerClasses,
            TestStatus testStatus)
    {
        // one base test context plus one test context for each fulfiller
        checkState(testContextStack.size() == fulfillerClasses.size() + 1);

        for (Class<? extends RequirementFulfiller> fulfillerClass : reverse(fulfillerClasses)) {
            LOGGER.debug("Cleaning for fulfiller {}", fulfillerClass);
            TestContext testContext = testContextStack.removeLast();
            testContext.close();
            runWithTestContext(testContext, () -> testContextStack.getLast().getDependency(fulfillerClass).cleanup(testStatus));
        }

        if (testContextStack.size() == 1) {
            // we are going to close last context, so we need to close TableManager's first
            testContextStack.getLast().getOptionalDependency(TableManagerDispatcher.class)
                    .ifPresent(dispatcher -> dispatcher.getAllTableManagers().forEach(TableManager::close));
        }

        // remove close init test context too
        testContextStack.getLast().close();
    }

    static Module bindFulfillers(List<Class<? extends RequirementFulfiller>> classes)
    {
        Function<Class<? extends RequirementFulfiller>, Module> bindToModule =
                clazz -> (Binder binder) -> binder.bind(clazz).in(Singleton.class);
        List<Module> modules = classes.stream()
                .map(bindToModule)
                .collect(toImmutableList());
        return combine(modules);
    }
}
