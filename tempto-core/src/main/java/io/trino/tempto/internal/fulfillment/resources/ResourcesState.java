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
package io.trino.tempto.internal.fulfillment.resources;

import com.google.common.io.Closer;
import io.trino.tempto.context.State;

import java.io.IOException;
import java.io.UncheckedIOException;

public abstract class ResourcesState
        implements State, AutoCloseable
{
    private final Closer closer = Closer.create();

    public <T extends AutoCloseable> T register(T resource)
    {
        closer.register(() -> {
            try {
                resource.close();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return resource;
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
