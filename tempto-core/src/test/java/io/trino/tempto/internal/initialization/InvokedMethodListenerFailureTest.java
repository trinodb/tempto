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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.TestNG;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards against the TestNG &gt;= 7.9 regression where throwing from a listener during parallel
 * execution hangs the whole suite (GraphOrchestrator NPE about a null worker), instead of failing
 * the test. {@link io.trino.tempto.internal.initialization.TestInitializationListener} signals
 * setup failures from {@code IInvokedMethodListener.beforeInvocation} precisely because that path
 * is caught by the invoker and reported as a test failure. This test pins that behaviour.
 */
public class InvokedMethodListenerFailureTest
{
    static class ThrowingBeforeInvocationListener
            implements IInvokedMethodListener
    {
        @Override
        public void beforeInvocation(IInvokedMethod method, ITestResult testResult)
        {
            if (method.isTestMethod() && testResult.getStatus() == ITestResult.STARTED) {
                throw new RuntimeException("simulated setup failure");
            }
        }
    }

    static class CountingTestListener
            implements ITestListener
    {
        static final List<String> FAILURES = Collections.synchronizedList(new ArrayList<String>());

        @Override
        public void onTestFailure(ITestResult result)
        {
            FAILURES.add(result.getMethod().getMethodName());
        }
    }

    public static class SampleParallelTest
    {
        @org.testng.annotations.Test
        public void testOne() {}

        @org.testng.annotations.Test
        public void testTwo() {}

        @org.testng.annotations.Test
        public void testThree() {}
    }

    @Test
    @Timeout(60)
    public void throwingFromBeforeInvocationFailsTestsCleanlyInsteadOfHangingTheSuite()
    {
        CountingTestListener.FAILURES.clear();

        XmlSuite suite = new XmlSuite();
        suite.setName("repro-suite");
        XmlTest test = new XmlTest(suite);
        test.setName("repro-test");
        test.setParallel(XmlSuite.ParallelMode.METHODS);
        test.setThreadCount(3);
        test.setXmlClasses(List.of(new XmlClass(SampleParallelTest.class.getName())));

        TestNG testNG = new TestNG();
        testNG.setUseDefaultListeners(false);
        testNG.setXmlSuites(List.of(suite));
        testNG.addListener(new ThrowingBeforeInvocationListener());
        testNG.addListener(new CountingTestListener());

        // Would hang forever before the fix; the @Timeout turns a regression into a failure.
        testNG.run();

        assertThat(testNG.hasFailure()).isTrue();
        List<String> failures = new ArrayList<>(CountingTestListener.FAILURES);
        Collections.sort(failures);
        assertThat(failures).isEqualTo(List.of("testOne", "testThree", "testTwo"));
    }
}
