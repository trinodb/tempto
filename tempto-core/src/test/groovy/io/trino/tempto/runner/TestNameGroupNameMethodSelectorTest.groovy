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

package io.trino.tempto.runner

import io.trino.tempto.internal.listeners.TestMetadata
import io.trino.tempto.internal.listeners.TestMetadataReader
import io.trino.tempto.internal.listeners.TestNameGroupNameMethodSelector
import org.testng.IMethodSelectorContext
import org.testng.ITestNGMethod
import spock.lang.Specification
import spock.lang.Unroll

class TestNameGroupNameMethodSelectorTest
        extends Specification
{
    private TestMetadataReader metadataReader = Mock()

    @Unroll
    def 'test selector match #testName/#testGroups for #allowedTestNames/#allowedTestGroups/#excludedTestGroups'()
    {
        setup:
        metadataReader.readTestMetadata(_) >> new TestMetadata(testGroups as Set, testName)
        def testSelector = new TestNameGroupNameMethodSelector(
                asSetOptional(allowedTestNames),
                asSet(excludedTestNames),
                asSetOptional(allowedTestGroups),
                asSet(excludedTestGroups),
                metadataReader)

        expect:
        testSelector.includeMethod(Mock(IMethodSelectorContext), Mock(ITestNGMethod), true) == expected

        where:
        testName    | testGroups   | allowedTestNames | excludedTestNames | allowedTestGroups | excludedTestGroups | expected
        'abc'       | ['g1', 'g2'] | null             | null              | null              | null               | true
        'abc'       | ['g1', 'g2'] | ['abc']          | null              | null              | null               | true
        'abc'       | ['g1', 'g2'] | ['xyz', 'abc']   | null              | null              | null               | true
        'abc'       | ['g1', 'g2'] | null             | null              | ['g1']            | null               | true
        'abc'       | ['g1', 'g2'] | null             | null              | ['g1', 'g3']      | null               | true
        'abc'       | ['g1', 'g2'] | ['xyz', 'abc']   | null              | ['g1', 'g3']      | null               | true
        'p.q.r.abc' | []           | ['abc']          | null              | null              | null               | true
        'p.q.r.abc' | []           | ['r.abc']        | null              | null              | null               | true
        'p.q.r.abc' | []           | ['p.q.r.abc']    | null              | null              | null               | true
        'p.q.r.abc' | []           | ['bc']           | null              | null              | null               | true
        'p.q.r.abc' | []           | ['xbc']          | null              | null              | null               | false
        'abc'       | ['g1', 'g2'] | ['xyz', 'abc']   | null              | ['g1', 'g3']      | ['g1']             | false
        'abc'       | ['g1', 'g2'] | ['xyz', 'abc']   | null              | ['g1', 'g3']      | ['g2']             | false
        'abc'       | ['g1', 'g2'] | ['xyz', 'abc']   | ['qwe']           | ['g1', 'g3']      | ['g5']             | true
        'abc'       | ['g1', 'g2'] | ['xyz', 'abc']   | ['ab']            | ['g1', 'g3']      | ['g5']             | true
        'abc'       | ['g1', 'g2'] | null             | ['abc']           | ['g1', 'g3']      | ['g5']             | false
        'abc'       | ['g1', 'g2'] | ['xyz', 'abc']   | ['abc']           | ['g1', 'g3']      | ['g5']             | false
        'p.q.r.abc' | ['g1', 'g2'] | null             | ['p.q.r.xyz']     | ['g1', 'g3']      | ['g5']             | true
        'p.q.r.abc' | ['g1', 'g2'] | null             | ['p.q.r.ab']      | ['g1', 'g3']      | ['g5']             | true
        'p.q.r.abc' | ['g1', 'g2'] | null             | ['p.q.r.abc']     | ['g1', 'g3']      | ['g5']             | false
        'p.q.r.abc' | ['g1', 'g2'] | null             | ['p.q.r']         | ['g1', 'g3']      | ['g5']             | false
    }

    private Optional<Set<String>> asSetOptional(List<String> strings)
    {
        if (strings == null) {
            return Optional.empty();
        }
        else {
            return Optional.of(strings as Set);
        }
    }

    private static Set<String> asSet(List<String> strings)
    {
        if (strings == null) {
            return []
        }
        else {
            return strings as Set
        }
    }
}
