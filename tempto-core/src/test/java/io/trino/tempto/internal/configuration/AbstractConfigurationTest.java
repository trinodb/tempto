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

package io.trino.tempto.internal.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractConfigurationTest
{
    public static final String KEY = "a.b.c";

    private AbstractConfiguration configuration;

    @BeforeEach
    public void setup()
    {
        configuration = mock(AbstractConfiguration.class, CALLS_REAL_METHODS);
    }

    @Test
    public void getPresentIntegerValue()
    {
        setupGetObject(KEY, 10);

        assertThat(configuration.getInt(KEY)).isEqualTo(Optional.of(10));
        assertThat(configuration.getIntMandatory(KEY)).isEqualTo(10);
    }

    @Test
    public void getPresentIntegerValueFromString()
    {
        setupGetObject(KEY, "10");

        assertThat(configuration.getInt(KEY)).isEqualTo(Optional.of(10));
        assertThat(configuration.getIntMandatory(KEY)).isEqualTo(10);
    }

    @Test
    public void nonMandatoryGetIntegerNotPresentValue()
    {
        setupGetObject(KEY, null);

        assertThat(configuration.getInt(KEY)).isEqualTo(Optional.empty());
    }

    @Test
    public void mandatoryGetIntegerNotPresentValue()
    {
        setupGetObject(KEY, null);

        assertThatThrownBy(() -> configuration.getIntMandatory(KEY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("could not find value for key a.b.c");
    }

    @Test
    public void mandatoryGetIntegerNotPresentValueWithMessage()
    {
        setupGetObject(KEY, null);

        assertThatThrownBy(() -> configuration.getIntMandatory(KEY, "damn, no key"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("damn, no key");
    }

    @Test
    public void getIntegerForNonMatchingType()
    {
        setupGetObject(KEY, new ArrayList<>());

        assertThatThrownBy(() -> configuration.getInt(KEY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("expected java.lang.Integer value for key a.b.c but got java.util.ArrayList");
    }

    @Test
    public void getPresentStringValue()
    {
        setupGetObject(KEY, "ala");

        assertThat(configuration.getString(KEY)).isEqualTo(Optional.of("ala"));
        assertThat(configuration.getStringMandatory(KEY)).isEqualTo("ala");
    }

    @Test
    public void getPresentStringValueForIntegerObject()
    {
        setupGetObject(KEY, 10);

        assertThat(configuration.getString(KEY)).isEqualTo(Optional.of("10"));
        assertThat(configuration.getStringMandatory(KEY)).isEqualTo("10");
    }

    @Test
    public void nonMandatoryGetStringNotPresentValue()
    {
        setupGetObject(KEY, null);

        assertThat(configuration.getString(KEY)).isEqualTo(Optional.empty());
    }

    @Test
    public void mandatoryGetStringNotPresentValue()
    {
        setupGetObject(KEY, null);

        assertThatThrownBy(() -> configuration.getStringMandatory(KEY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("could not find value for key a.b.c");
    }

    @Test
    public void mandatoryGetStringNotPresentValueWithMessage()
    {
        setupGetObject(KEY, null);

        assertThatThrownBy(() -> configuration.getStringMandatory(KEY, "damn, no key"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("damn, no key");
    }

    @Test
    public void getStringList()
    {
        setupGetObject(KEY, Arrays.asList("a", "b"));

        assertThat(configuration.getStringList(KEY)).isEqualTo(Arrays.asList("a", "b"));
    }

    @Test
    public void mandatoryGetStringListWithNoValueWithMessage()
    {
        setupGetObject(KEY, null);

        assertThatThrownBy(() -> configuration.getStringListMandatory(KEY, "damn, no key"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("damn, no key");
    }

    @Test
    public void mandatoryGetStringListWithNoValue()
    {
        setupGetObject(KEY, null);

        assertThatThrownBy(() -> configuration.getStringListMandatory(KEY))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("could not find value for key a.b.c");
    }

    private void setupGetObject(String key, Object value)
    {
        when(configuration.get(key)).thenReturn(Optional.ofNullable(value));
    }
}
