package com.github.lamba92.kotlin.document.store.core.maps

import com.github.lamba92.kotlin.document.store.core.PersistentMap
import com.github.lamba92.kotlin.document.store.core.SerializableEntry
import com.github.lamba92.kotlin.document.store.core.UpdateResult
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json

/**
 * Converts the current `PersistentMap` instance to a generic typed `TypedIdGenerator`.
 *
 * This transformation allows the key-value pairs in the `PersistentMap<String, String>`
 * to be used as a persistent key-value store with key type `String` and value type `T`,
 * providing seamless serialization and deserialization of the values using the provided serializer.
 *
 * @param serializer The serializer used to convert between type T and String representation.
 * @param json The Json instance used for serialization/deserialization.
 * @return A `TypedIdGenerator<T>` instance backed by the current `PersistentMap`.
 */
public fun <T : Any> PersistentMap<String, String>.asTypedIdGenerator(
    serializer: KSerializer<T>,
    json: Json,
): TypedIdGenerator<T> = TypedIdGenerator(this, serializer, json)

/**
 * Converts the current `PersistentMap` instance to an instance of `IdGenerator`.
 *
 * This transformation allows the key-value pairs in the `PersistentMap<String, String>`
 * to be used as a persistent key-value store with key type `String` and value type `Long`,
 * providing seamless serialization and deserialization of the values.
 *
 * @return An `IdGenerator` instance backed by the current `PersistentMap`.
 */
public fun PersistentMap<String, String>.asIdGenerator(): IdGenerator = IdGenerator(this)

/**
 * A generic implementation of the `PersistentMap` interface where the values are
 * automatically converted between `String` and type `T` for storage and retrieval.
 *
 * This class delegates the actual storage and persistence operations to another
 * `PersistentMap` instance, while providing a type-safe interface with `T`
 * values for the users. String-to-T and T-to-String conversions are handled
 * using Kotlinx Serialization.
 *
 * @param T The type of values stored in this map. Must be serializable.
 * @constructor Creates a `TypedIdGenerator` with the specified delegate map and serializer.
 * @param delegate The underlying `PersistentMap` that performs the actual storage operations.
 * @param serializer The serializer used to convert between type T and String.
 * @param json The Json instance used for serialization/deserialization.
 */
public class TypedIdGenerator<T : Any>(
    private val delegate: PersistentMap<String, String>,
    private val serializer: KSerializer<T>,
    private val json: Json,
) : PersistentMap<String, T> {

    private fun serialize(value: T): String = json.encodeToString(serializer, value)

    private fun deserialize(value: String): T = json.decodeFromString(serializer, value)

    override suspend fun clear(): Unit = delegate.clear()

    override suspend fun size(): Long = delegate.size()

    override suspend fun isEmpty(): Boolean = delegate.isEmpty()

    override fun close() {
        delegate.close()
    }

    override suspend fun get(key: String): T? = delegate.get(key)?.let { deserialize(it) }

    override suspend fun put(
        key: String,
        value: T,
    ): T? = delegate.put(key, serialize(value))?.let { deserialize(it) }

    override suspend fun remove(key: String): T? = delegate.remove(key)?.let { deserialize(it) }

    override suspend fun containsKey(key: String): Boolean = delegate.containsKey(key)

    override suspend fun update(
        key: String,
        value: T,
        updater: (T) -> T,
    ): UpdateResult<T> =
        delegate.update(
            key = key,
            value = serialize(value),
            updater = { serialize(updater(deserialize(it))) },
        ).let { UpdateResult(it.oldValue?.let { deserialize(it) }, deserialize(it.newValue)) }

    override suspend fun getOrPut(
        key: String,
        defaultValue: () -> T,
    ): T =
        delegate.getOrPut(
            key = key,
            defaultValue = { serialize(defaultValue()) },
        ).let { deserialize(it) }

    override fun entries(): Flow<Map.Entry<String, T>> =
        delegate.entries()
            .map { SerializableEntry(it.key, deserialize(it.value)) }
}

/**
 * An implementation of the `PersistentMap` interface where the values are
 * automatically converted between `String` and `Long` for storage and retrieval.
 *
 * This class delegates the actual storage and persistence operations to another
 * `PersistentMap` instance, while providing a type-safe interface with `Long`
 * values for the users. String-to-Long and Long-to-String conversions are handled
 * implicitly during operations.
 *
 * This is the legacy implementation maintained for backwards compatibility.
 *
 * @constructor Creates an `IdGenerator` with the specified delegate map.
 * @param delegate The underlying `PersistentMap` that performs the actual
 * storage operations. This map stores values as `String`.
 */
public class IdGenerator(private val delegate: PersistentMap<String, String>) : PersistentMap<String, Long> {
    override suspend fun clear(): Unit = delegate.clear()

    override suspend fun size(): Long = delegate.size()

    override suspend fun isEmpty(): Boolean = delegate.isEmpty()

    override fun close() {
        delegate.close()
    }

    override suspend fun get(key: String): Long? = delegate.get(key)?.toLong()

    override suspend fun put(
        key: String,
        value: Long,
    ): Long? = delegate.put(key, value.toString())?.toLong()

    override suspend fun remove(key: String): Long? = delegate.remove(key)?.toLong()

    override suspend fun containsKey(key: String): Boolean = delegate.containsKey(key)

    override suspend fun update(
        key: String,
        value: Long,
        updater: (Long) -> Long,
    ): UpdateResult<Long> =
        delegate.update(
            key = key,
            value = value.toString(),
            updater = { updater(it.toLong()).toString() },
        ).let { UpdateResult(it.oldValue?.toLong(), it.newValue.toLong()) }

    override suspend fun getOrPut(
        key: String,
        defaultValue: () -> Long,
    ): Long =
        delegate.getOrPut(
            key = key,
            defaultValue = { defaultValue().toString() },
        ).toLong()

    override fun entries(): Flow<Map.Entry<String, Long>> =
        delegate.entries()
            .map { SerializableEntry(it.key, it.value.toLong()) }
}
