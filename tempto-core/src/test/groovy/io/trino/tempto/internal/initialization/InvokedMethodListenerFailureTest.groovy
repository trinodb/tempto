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
package io.trino.tempto.internal.initialization

import org.testng.IInvokedMethod
import org.testng.IInvokedMethodListener
import org.testng.ITestListener
import org.testng.ITestResult
import org.testng.TestNG
import org.testng.annotations.Test
import org.testng.xml.XmlSuite
import org.testng.xml.XmlTest
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.TimeUnit

/**
 * Guards against the TestNG >= 7.9 regression where throwing from a listener during parallel
 * execution hangs the whole suite (GraphOrchestrator NPE about a null worker), instead of failing
 * the test. {@link io.trino.tempto.internal.initialization.TestInitializationListener} signals
 * setup failures from {@code IInvokedMethodListener.beforeInvocation} precisely because that path
 * is caught by the invoker and reported as a test failure. This test pins that behaviour.
 */
class InvokedMethodListenerFailureTest
        extends Specification
{
    static class ThrowingBeforeInvocationListener
            implements IInvokedMethodListener
    {
        @Override
        void beforeInvocation(IInvokedMethod method, ITestResult testResult)
        {
            if (method.isTestMethod() && testResult.getStatus() == ITestResult.STARTED) {
                throw new RuntimeException("simulated setup failure")
            }
        }
    }

    static class CountingTestListener
            implements ITestListener
    {
        static final List<String> FAILURES = Collections.synchronizedList(new ArrayList<String>())

        @Override
        void onTestFailure(ITestResult result)
        {
            FAILURES.add(result.getMethod().getMethodName())
        }
    }

    static class SampleParallelTest
    {
        @Test
        void testOne() {}

        @Test
        void testTwo() {}

        @Test
        void testThree() {}
    }

    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    def 'throwing from beforeInvocation fails tests cleanly instead of hanging the suite'()
    {
        setup:
        CountingTestListener.FAILURES.clear()

        XmlSuite suite = new XmlSuite()
        suite.setName("repro-suite")
        XmlTest test = new XmlTest(suite)
        test.setName("repro-test")
        test.setParallel(XmlSuite.ParallelMode.METHODS)
        test.setThreadCount(3)
        test.setXmlClasses([new org.testng.xml.XmlClass(SampleParallelTest.getName())])

        TestNG testNG = new TestNG()
        testNG.setUseDefaultListeners(false)
        testNG.setXmlSuites([suite])
        testNG.addListener(new ThrowingBeforeInvocationListener())
        testNG.addListener(new CountingTestListener())

        when:
        // Would hang forever before the fix; the @Timeout turns a regression into a failure.
        testNG.run()

        then:
        testNG.hasFailure()
        CountingTestListener.FAILURES.sort() == ["testOne", "testThree", "testTwo"]
    }
}
