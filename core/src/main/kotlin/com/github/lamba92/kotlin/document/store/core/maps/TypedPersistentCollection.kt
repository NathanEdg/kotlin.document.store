package com.github.lamba92.kotlin.document.store.core.maps

import com.github.lamba92.kotlin.document.store.core.PersistentMap
import com.github.lamba92.kotlin.document.store.core.SerializableEntry
import com.github.lamba92.kotlin.document.store.core.UpdateResult
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

/**
 * Converts a [PersistentMap] instance into a [TypedPersistentCollection] with strongly-typed keys.
 *
 * @param keySerializer The serializer for the key type.
 * @param json The Json instance for serialization.
 * @return A [TypedPersistentCollection] that wraps the current [PersistentMap].
 */
public fun <K : Any> PersistentMap<String, String>.asTypedCollectionMap(
    keySerializer: KSerializer<K>,
    json: Json,
): TypedPersistentCollection<K> = TypedPersistentCollection(this, keySerializer, json)

/**
 * TypedPersistentCollection is an implementation of the PersistentMap interface with strongly-typed keys.
 * It adapts a PersistentMap<String, String> as a PersistentMap<K, String>.
 *
 * This class leverages an underlying PersistentMap<String, String> for its operations,
 * and ensures compatibility by serializing keys to their String representation.
 *
 * @param K The type of keys used in this collection.
 * @constructor Creates a TypedPersistentCollection instance.
 * @param delegate The underlying PersistentMap<String, String> used to perform operations.
 * @param keySerializer The serializer for the key type.
 * @param json The Json instance for serialization.
 */
public class TypedPersistentCollection<K : Any>(
    private val delegate: PersistentMap<String, String>,
    private val keySerializer: KSerializer<K>,
    private val json: Json,
) : PersistentMap<K, String> {

    private fun serializeKey(key: K): String = json.encodeToString(keySerializer, key)

    private fun deserializeKey(key: String): K = json.decodeFromString(keySerializer, key)

    override suspend fun clear(): Unit = delegate.clear()

    override suspend fun size(): Long = delegate.size()

    override suspend fun isEmpty(): Boolean = delegate.isEmpty()

    override fun close() {
        delegate.close()
    }

    override suspend fun get(key: K): String? = delegate.get(serializeKey(key))

    override suspend fun put(
        key: K,
        value: String,
    ): String? = delegate.put(serializeKey(key), value)

    override suspend fun remove(key: K): String? = delegate.remove(serializeKey(key))

    override suspend fun containsKey(key: K): Boolean = delegate.containsKey(serializeKey(key))

    override suspend fun update(
        key: K,
        value: String,
        updater: (String) -> String,
    ): UpdateResult<String> =
        delegate.update(
            key = serializeKey(key),
            value = value,
            updater = updater,
        )

    override suspend fun getOrPut(
        key: K,
        defaultValue: () -> String,
    ): String =
        delegate.getOrPut(
            key = serializeKey(key),
            defaultValue = defaultValue,
        )

    override fun entries(): Flow<Map.Entry<K, String>> =
        delegate.entries()
            .map { SerializableEntry(deserializeKey(it.key), it.value) }
}

