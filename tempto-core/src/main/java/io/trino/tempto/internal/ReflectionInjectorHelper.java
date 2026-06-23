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

import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import io.trino.tempto.context.TestContext;
import io.trino.tempto.internal.context.GuiceTestContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Optional;

import static java.util.Arrays.stream;

public class ReflectionInjectorHelper
{
    public Object[] getMethodArguments(TestContext testContext, Method method)
    {
        if (!isAnnotatedWithInject(method)) {
            return new Object[] {};
        }
        GuiceTestContext guiceTestContext = (GuiceTestContext) testContext;
        return stream(method.getParameters())
                .map(parameter -> guiceTestContext.getDependency(keyFor(parameter)))
                .toArray();
    }

    private static Key<?> keyFor(Parameter parameter)
    {
        Optional<Annotation> bindingAnnotation = bindingAnnotation(parameter);
        return bindingAnnotation.isPresent()
                ? Key.get(parameter.getType(), bindingAnnotation.get())
                : Key.get(parameter.getType());
    }

    private static Optional<Annotation> bindingAnnotation(Parameter parameter)
    {
        return stream(parameter.getAnnotations())
                .filter(ReflectionInjectorHelper::isBindingAnnotation)
                .findFirst();
    }

    private static boolean isBindingAnnotation(Annotation annotation)
    {
        Class<? extends Annotation> annotationType = annotation.annotationType();
        return annotationType.isAnnotationPresent(BindingAnnotation.class)
                || annotationType.isAnnotationPresent(jakarta.inject.Qualifier.class);
    }

    private static boolean isAnnotatedWithInject(Method method)
    {
        return stream(method.getAnnotations())
                .anyMatch(annotation -> annotation.annotationType().getSimpleName().equals("Inject"));
    }
}
