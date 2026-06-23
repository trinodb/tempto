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

package io.trino.tempto.internal.fulfillment.table;

import io.trino.tempto.Requirement;
import io.trino.tempto.context.State;
import io.trino.tempto.fulfillment.TestStatus;
import io.trino.tempto.fulfillment.table.ImmutableTableRequirement;
import io.trino.tempto.fulfillment.table.ImmutableTablesState;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.MutableTablesState;
import io.trino.tempto.fulfillment.table.TableDefinition;
import io.trino.tempto.fulfillment.table.TableHandle;
import io.trino.tempto.fulfillment.table.TableInstance;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.TableManagerDispatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.LOADED;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TableRequirementFulfillerTest
{
    private static final String DATABASE_NAME = "database_name";
    private static final String OTHER_DATABASE_NAME = "other_database_name";
    private static final String OTHER_DATABASE_NAME_2 = "other_database_name_2";

    private TableManager tableManager;
    private TableManager otherTableManager;
    private TableManager otherTableManager2;
    private TableManagerDispatcher tableManagerDispatcher;

    @BeforeEach
    public void setup()
    {
        tableManager = mock(TableManager.class);
        otherTableManager = mock(TableManager.class);
        otherTableManager2 = mock(TableManager.class);

        lenient().when(tableManager.getDatabaseName()).thenReturn(DATABASE_NAME);
        lenient().when(tableManager.getTableDefinitionClass()).thenReturn(TestTableDefinition.class);
        lenient().when(otherTableManager.getDatabaseName()).thenReturn(OTHER_DATABASE_NAME);
        lenient().when(otherTableManager.getTableDefinitionClass()).thenReturn(OtherTestTableDefinition.class);
        lenient().when(otherTableManager2.getDatabaseName()).thenReturn(OTHER_DATABASE_NAME_2);
        lenient().when(otherTableManager2.getTableDefinitionClass()).thenReturn(OtherTestTableDefinition.class);

        Map<String, TableManager> tableManagers = new HashMap<>();
        tableManagers.put(DATABASE_NAME, tableManager);
        tableManagers.put(OTHER_DATABASE_NAME, otherTableManager);
        tableManagers.put(OTHER_DATABASE_NAME_2, otherTableManager2);
        tableManagerDispatcher = new DefaultTableManagerDispatcher(tableManagers);
    }

    @Test
    public void testMutableTableFulfillCleanup()
    {
        TestTableDefinition tableDefinition = getTableDefinition("nation");

        TableInstance mutableTableInstanceLoaded = new TestTableInstance(new TableName(DATABASE_NAME, Optional.empty(), "nation", "nation_mutable"), tableDefinition);
        MutableTableRequirement mutableTableRequirementLoaded = MutableTableRequirement.builder(tableDefinition).build();

        when(tableManager.createMutable(eq(tableDefinition), eq(LOADED), any())).thenReturn(mutableTableInstanceLoaded);

        MutableTablesFulfiller fulfiller = new MutableTablesFulfiller(tableManagerDispatcher);

        Set<State> states = fulfiller.fulfill(Set.<Requirement>of(mutableTableRequirementLoaded));

        assertThat(states).hasSize(1);
        MutableTablesState state = (MutableTablesState) getOnlyElement(states);
        assertThat(state.get("nation")).isNotNull();
        assertThat(state.get("nation")).isEqualTo(mutableTableInstanceLoaded);
        assertThat(state.get(tableHandle("nation").inDatabase(DATABASE_NAME))).isEqualTo(mutableTableInstanceLoaded);

        verify(tableManager).createMutable(eq(tableDefinition), eq(LOADED), any());

        clearInvocations(tableManager, otherTableManager, otherTableManager2);

        fulfiller.cleanup(TestStatus.SUCCESS);

        verify(tableManager).dropTable(any());
    }

    @Test
    public void testMutableNamedAndCreatedTableFulfillCleanup()
    {
        TestTableDefinition tableDefinition = getTableDefinition("nation");

        String tableInstanceName = "table_instance_name";
        TableInstance mutableTableInstanceNamedCreated = new TestTableInstance(new TableName(DATABASE_NAME, Optional.empty(), tableInstanceName, tableInstanceName), tableDefinition);
        MutableTableRequirement mutableTableRequirementNamedCreated = MutableTableRequirement.builder(tableDefinition)
                .withName(tableInstanceName)
                .withState(CREATED)
                .build();

        when(tableManager.createMutable(eq(tableDefinition), eq(CREATED), any())).thenReturn(mutableTableInstanceNamedCreated);

        MutableTablesFulfiller fulfiller = new MutableTablesFulfiller(tableManagerDispatcher);

        Set<State> states = fulfiller.fulfill(Set.<Requirement>of(mutableTableRequirementNamedCreated));

        assertThat(states).hasSize(1);
        MutableTablesState state = (MutableTablesState) getOnlyElement(states);
        assertThat(state.get(tableInstanceName)).isNotNull();
        assertThat(state.get(tableInstanceName)).isEqualTo(mutableTableInstanceNamedCreated);
        assertThat(state.get(tableHandle(tableInstanceName).inDatabase(DATABASE_NAME))).isEqualTo(mutableTableInstanceNamedCreated);

        verify(tableManager).createMutable(eq(tableDefinition), eq(CREATED), any());

        clearInvocations(tableManager, otherTableManager, otherTableManager2);

        fulfiller.cleanup(TestStatus.FAILURE);

        verifyNoInteractions(tableManager, otherTableManager, otherTableManager2);
    }

    @Test
    public void testImmutableTableFulfillCleanup()
    {
        TestTableDefinition tableDefinition = getTableDefinition("nation");
        TableInstance tableInstance = new TestTableInstance(new TableName(DATABASE_NAME, Optional.empty(), "nation", "nation"), tableDefinition);
        ImmutableTableRequirement requirement = new ImmutableTableRequirement(tableDefinition);

        when(tableManager.createImmutable(tableDefinition)).thenReturn(tableInstance);

        ImmutableTablesFulfiller fulfiller = new ImmutableTablesFulfiller(tableManagerDispatcher);

        Set<State> states = fulfiller.fulfill(Set.<Requirement>of(requirement));

        assertThat(states).hasSize(1);
        ImmutableTablesState state = (ImmutableTablesState) getOnlyElement(states);
        assertThat(state.get("nation")).isEqualTo(tableInstance);
        assertThat(state.get(tableHandle("nation").inDatabase(DATABASE_NAME))).isEqualTo(tableInstance);

        verify(tableManager).createImmutable(tableDefinition);

        clearInvocations(tableManager, otherTableManager, otherTableManager2);

        fulfiller.cleanup(TestStatus.SUCCESS);

        verifyNoInteractions(tableManager, otherTableManager, otherTableManager2);
    }

    @Test
    public void testSameImmutableTablesOnDifferentDatabases()
    {
        OtherTestTableDefinition tableDefinition = getOtherTableDefinition("nation");
        TableInstance tableInstance = new TestTableInstance(new TableName(OTHER_DATABASE_NAME, Optional.empty(), "nation", "nation"), tableDefinition);
        TableInstance tableInstance2 = new TestTableInstance(new TableName(OTHER_DATABASE_NAME_2, Optional.empty(), "nation", "nation"), tableDefinition);

        TableHandle tableHandle = TableHandle.tableHandle("nation").inDatabase(OTHER_DATABASE_NAME);
        ImmutableTableRequirement requirement = new ImmutableTableRequirement(tableDefinition, tableHandle);

        TableHandle tableHandle2 = TableHandle.tableHandle("nation").inDatabase(OTHER_DATABASE_NAME_2);
        ImmutableTableRequirement requirement2 = new ImmutableTableRequirement(tableDefinition, tableHandle2);

        when(otherTableManager.createImmutable(tableDefinition)).thenReturn(tableInstance);
        when(otherTableManager2.createImmutable(tableDefinition)).thenReturn(tableInstance2);

        ImmutableTablesFulfiller fulfiller = new ImmutableTablesFulfiller(tableManagerDispatcher);

        Set<State> states = fulfiller.fulfill(Set.<Requirement>of(requirement, requirement2));

        assertThat(states).hasSize(1);
        ImmutableTablesState state = (ImmutableTablesState) getOnlyElement(states);
        assertThat(state.get(tableHandle)).isEqualTo(tableInstance);
        assertThat(state.get(tableHandle2)).isEqualTo(tableInstance2);
        assertThatThrownBy(() -> state.get("nation"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("please use more detailed table handle");

        verify(otherTableManager).createImmutable(tableDefinition);
        verify(otherTableManager2).createImmutable(tableDefinition);

        clearInvocations(tableManager, otherTableManager, otherTableManager2);

        fulfiller.cleanup(TestStatus.SUCCESS);

        verifyNoInteractions(tableManager, otherTableManager, otherTableManager2);
    }

    @Test
    public void testSameImmutableTablesOnSameDatabasesWithDifferentDatabaseAliasesAreFiltered()
    {
        TestTableDefinition tableDefinition = getTableDefinition("nation");
        TableInstance tableInstance = new TestTableInstance(new TableName(DATABASE_NAME, Optional.empty(), "nation", "nation"), tableDefinition);
        ImmutableTableRequirement requirement = new ImmutableTableRequirement(tableDefinition, tableHandle("nation").inDatabase(DATABASE_NAME));
        ImmutableTableRequirement requirementOnDefault = new ImmutableTableRequirement(tableDefinition);

        when(tableManager.createImmutable(tableDefinition)).thenReturn(tableInstance);

        ImmutableTablesFulfiller fulfiller = new ImmutableTablesFulfiller(tableManagerDispatcher);

        Set<State> states = fulfiller.fulfill(Set.<Requirement>of(requirement, requirementOnDefault));

        assertThat(states).hasSize(1);
        ImmutableTablesState state = (ImmutableTablesState) getOnlyElement(states);
        assertThat(state.get(tableHandle("nation").inDatabase(DATABASE_NAME))).isEqualTo(tableInstance);
        assertThat(state.get("nation")).isEqualTo(tableInstance);

        verify(tableManager).createImmutable(tableDefinition);

        clearInvocations(tableManager, otherTableManager, otherTableManager2);

        fulfiller.cleanup(TestStatus.SUCCESS);

        verifyNoInteractions(tableManager, otherTableManager, otherTableManager2);
    }

    private TestTableDefinition getTableDefinition(String tableName)
    {
        return new TestTableDefinition(tableHandle(tableName));
    }

    private OtherTestTableDefinition getOtherTableDefinition(String tableName)
    {
        return new OtherTestTableDefinition(tableHandle(tableName));
    }

    static class TestTableInstance
            extends TableInstance
    {
        TestTableInstance(TableName name, TableDefinition tableDefinition)
        {
            super(name, tableDefinition);
        }
    }

    static class TestTableDefinition
            extends TableDefinition
    {
        TestTableDefinition(TableHandle handle)
        {
            super(handle);
        }
    }

    static class OtherTestTableDefinition
            extends TableDefinition
    {
        OtherTestTableDefinition(TableHandle handle)
        {
            super(handle);
        }
    }
}
