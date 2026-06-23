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

import org.junit.jupiter.api.Test;
import org.testng.ITestNGMethod;
import org.testng.annotations.DataProvider;
import org.testng.internal.ConstructorOrMethod;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataProvidersTest
{
    public static final Object[][] EXTERNAL_DATA_PROVIDER_PARAMS = {{"ex1"}, {"ex2"}};
    public static final Object[][] INTERNAL_DATA_PROVIDER_PARAMS = {{"int1"}, {"int2"}};

    private static class ExternalDataProviderClass
    {
        @DataProvider(name = "external_data_provider")
        public static Object[][] externalDataProvider()
        {
            return EXTERNAL_DATA_PROVIDER_PARAMS;
        }
    }

    private static class TestClass
    {
        @DataProvider(name = "internal_data_provider")
        public static Object[][] internalDataProvider()
        {
            return INTERNAL_DATA_PROVIDER_PARAMS;
        }

        @org.testng.annotations.Test
        void testMethodWithoutDataProvider() {}

        @org.testng.annotations.Test(dataProvider = "internal_data_provider")
        void testMethodWithInternalDataProvider() {}

        @org.testng.annotations.Test(dataProvider = "external_data_provider", dataProviderClass = ExternalDataProviderClass.class)
        void testMethodWithExternalDataProvider() {}
    }

    @Test
    public void shouldReturnAbsentForMethodWithoutDataProvider()
    {
        ITestNGMethod mockTestNGMethod = mockTestNGMethod("testMethodWithoutDataProvider");
        Optional<Object[][]> parameters = DataProviders.getParametersForMethod(mockTestNGMethod);

        assertThat(parameters).isNotPresent();
    }

    @Test
    public void shouldReturnParametersForMethodWithInternalDataProvider()
    {
        ITestNGMethod mockTestNGMethod = mockTestNGMethod("testMethodWithInternalDataProvider");
        Optional<Object[][]> parameters = DataProviders.getParametersForMethod(mockTestNGMethod);

        assertThat(parameters).isPresent();
        assertThat(parameters.get()).isEqualTo(INTERNAL_DATA_PROVIDER_PARAMS);
    }

    @Test
    public void shouldReturnParametersForMethodWithExternalDataProvider()
    {
        ITestNGMethod mockTestNGMethod = mockTestNGMethod("testMethodWithExternalDataProvider");
        Optional<Object[][]> parameters = DataProviders.getParametersForMethod(mockTestNGMethod);

        assertThat(parameters).isPresent();
        assertThat(parameters.get()).isEqualTo(EXTERNAL_DATA_PROVIDER_PARAMS);
    }

    private ITestNGMethod mockTestNGMethod(String testMethodName)
    {
        try {
            ITestNGMethod mockedTestNGMethod = mock(ITestNGMethod.class);
            when(mockedTestNGMethod.getInstance()).thenReturn(new TestClass());
            when(mockedTestNGMethod.getRealClass()).thenReturn((Class) TestClass.class);
            ConstructorOrMethod mockedConstructorOrMethod = new ConstructorOrMethod(TestClass.class.getDeclaredMethod(testMethodName));
            when(mockedTestNGMethod.getConstructorOrMethod()).thenReturn(mockedConstructorOrMethod);
            return mockedTestNGMethod;
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
