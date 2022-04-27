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

package io.trino.tempto.internal.configuration

import spock.lang.Specification

class YamlConfigurationTest
        extends Specification
{
    def create()
    {
        setup:
        def configuration = new YamlConfiguration("""\
a:
   b:
      d: ela\${foo}
x:
   y: 10
list:
    - element1
    - element2   
""")
        expect:
        configuration.listKeys() == ['a.b.d', 'x.y', 'list'] as Set
        configuration.getString('missing') == Optional.empty()
        configuration.getStringList('missing') == []
        configuration.getStringOrList('missing') == []
        configuration.getInt('x.y') == Optional.of(10)
        configuration.getString('a.b.d') == Optional.of('ela${foo}')
        !configuration.isList('x.y')
        configuration.getString('x.y') == Optional.of('10')
        configuration.getStringOrList('x.y') == ['10']
        configuration.isList('list')
        configuration.getStringList('list') == ['element1', 'element2']
        configuration.getStringOrList('list') == ['element1', 'element2']
    }

    def createMultiple()
    {
        setup:
        def configurations = YamlConfiguration.loadAll("""\
a: 1
list:
    - element1
    - element2   
---
b: 2
""")
        expect:
        configurations[0].listKeys() == ['a', 'list'] as Set
        configurations[0].getInt('a') == Optional.of(1)
        configurations[0].isList('list')
        configurations[0].getStringList('list') == ['element1', 'element2']
        configurations[0].getStringOrList('list') == ['element1', 'element2']
        configurations[1].listKeys() == ['b'] as Set
        configurations[1].getInt('b') == Optional.of(2)
    }

    def createEmpty()
    {
        setup:
        def configuration = new YamlConfiguration("");

        expect:
        configuration.listKeys() == [] as Set
        configuration.getString('missing') == Optional.empty()
    }

    def createFromAllComments()
    {
        setup:
        def configuration = new YamlConfiguration("# comment");

        expect:
        configuration.listKeys() == [] as Set
        configuration.getString('missing') == Optional.empty()
    }
}
