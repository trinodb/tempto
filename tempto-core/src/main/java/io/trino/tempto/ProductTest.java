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

package io.trino.tempto;

import io.trino.tempto.internal.initialization.TemptoTestExtension;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for all Tempto (product) tests.
 * <p>
 * Extending this class registers the {@link TemptoTestExtension} JUnit 5 extension which is
 * responsible for creating the per-test {@link io.trino.tempto.context.TestContext}, fulfilling
 * the test's {@link Requirement}s, injecting members into the test instance and invoking methods
 * annotated with {@link BeforeMethodWithContext} / {@link AfterMethodWithContext}.
 */
@ExtendWith(TemptoTestExtension.class)
public class ProductTest {}
