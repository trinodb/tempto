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
package io.trino.tempto.threads;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.tempto.threads.ParallelExecution.parallelExecution;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ParallelExecutionTest
{
    @Test
    public void shouldExecuteRunnablesInParallel()
            throws InterruptedException
    {
        ParallelExecution.ParallelExecutionBuilder parallelExecutionBuilder = ParallelExecution.builder();
        AtomicInteger executionCount = new AtomicInteger();

        parallelExecutionBuilder.addRunnable(() -> executionCount.incrementAndGet());

        for (int i = 0; i < 10; i++) {
            final int expectedThreadIndex = i;
            parallelExecutionBuilder.addRunnable((int threadIndex) -> {
                assertThat(threadIndex).isEqualTo(expectedThreadIndex);
                executionCount.incrementAndGet();
            });
        }

        ParallelExecution parallelExecution = parallelExecutionBuilder.build();

        parallelExecution.start();
        parallelExecution.join();

        assertThat(executionCount.get()).isEqualTo(11);
    }

    @Test
    public void shouldPropagateAssertionsFromRunnable()
    {
        ParallelExecution.ParallelExecutionBuilder parallelExecutionBuilder = ParallelExecution.builder();

        for (int i = 0; i < 2; i++) {
            parallelExecutionBuilder.addRunnable(() -> assertThat(false).isTrue());
        }

        ParallelExecution parallelExecution = parallelExecutionBuilder.build();

        parallelExecution.start();

        assertThatThrownBy(() -> parallelExecution.joinAndRethrow())
                .isInstanceOf(ParallelExecutionException.class);
    }

    @Test
    public void shouldFailTimeout()
            throws InterruptedException
    {
        ParallelExecution parallelExecution = parallelExecution(1, (int _) -> Thread.sleep(500000));

        parallelExecution.start();

        assertThat(parallelExecution.join(100)).isFalse();
    }
}
