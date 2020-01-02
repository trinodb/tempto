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

package io.prestosql.tempto.internal;

import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public final class ReflectionHelper
{
    public static <T> List<? extends T> instantiate(Collection<Class<? extends T>> classes)
    {
        return classes
                .stream()
                .map(ReflectionHelper::instantiate)
                .collect(toList());
    }

    public static <T> T instantiate(String className)
    {
        try {
            return instantiate((Class<T>) Class.forName(className));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to find specified class: " + className, e);
        }
    }

    public static <T> T instantiate(Class<? extends T> clazz)
    {
        try {
            return clazz.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private ReflectionHelper()
    {
    }
}
