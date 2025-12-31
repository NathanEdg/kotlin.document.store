package com.github.lamba92.kotlin.document.store.core

import com.github.lamba92.kotlin.document.store.core.maps.IndexOfIndexes
import com.github.lamba92.kotlin.document.store.core.maps.TypedIdGenerator
import com.github.lamba92.kotlin.document.store.core.maps.TypedPersistentCollection
import com.github.lamba92.kotlin.document.store.core.maps.asIndex
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

/**
 * A collection of [JsonElement] with strongly-typed IDs saved in a [DataStore].
 *
 * This class provides the same functionality as [JsonCollection] but with strongly-typed
 * identifiers instead of Long IDs. The ID type must be serializable and is managed through
 * a [TypedIdGenerator].
 *
 * @param ID The type of identifiers used in this collection. Must be serializable.
 * @property name The name of the collection in the [DataStore].
 * @property json The [Json] instance used to serialize and deserialize JSON data.
 * @property mutex [Mutex] used to synchronize access to the collection.
 * @property store The [DataStore] where the collection is stored.
 * @property indexMap A [PersistentMap] of indexes for the collection.
 * @property genIdMap A typed ID generator for this collection.
 * @property persistentCollection The [PersistentMap] of JSON strings stored in the [DataStore].
 * @property idSerializer The serializer for the ID type.
 */
public class TypedJsonCollection<ID : Any>(
    override val name: String,
    override val json: Json,
    private val mutex: Mutex,
    private val store: DataStore,
    private val indexMap: IndexOfIndexes,
    private val genIdMap: TypedIdGenerator<ID>,
    private val persistentCollection: TypedPersistentCollection<ID>,
    private val idSerializer: KSerializer<ID>,
) : KotlinDocumentStoreCollection {

    /**
     * Generates a new unique identifier by updating the generator state.
     *
     * @param initialValue The initial value to use when no previous ID exists.
     * @param generator A function that takes the current ID and produces the next ID.
     * @return The newly generated unique identifier.
     */
    private suspend fun generateId(initialValue: ID, generator: (ID) -> ID): ID =
        genIdMap.update(name, generator(initialValue)) { generator(it) }.newValue

    /**
     * Retrieves the index map for the specified field if it exists; otherwise, returns `null`.
     *
     * @param selector The selector for the field for which the index map is to be retrieved.
     * @return The index map for the specified field, or `null` if the field does not have an associated index.
     */
    private suspend fun getIndexOrNull(selector: String) =
        when {
            !hasIndex(selector) -> null
            else -> store.getMap("index:$name:$selector").asIndex()
        }

    /**
     * Checks whether a specific field selector has an associated index in the collection.
     *
     * @param selector The field selector for which to check the existence of an index.
     * @return `true` if an index exists for the given selector; otherwise, `false`.
     */
    private suspend fun hasIndex(selector: String): Boolean =
        indexMap.get(name)?.contains(selector) == true

    /**
     * Iterates through all entries in the persistent collection.
     *
     * @return A [Flow]<[JsonObject]> of all entries in the collection.
     */
    public fun iterateAll(): Flow<JsonObject> =
        persistentCollection
            .entries()
            .map { json.parseToJsonElement(it.value).jsonObject }

    /**
     * Creates an index for optimizing query performance on the collection.
     *
     * @param selector A dot separated string where each string specifies a key for a JsonObject or
     *                 an index (prefixed with '$') for a JsonArray to navigate through the JSON structure.
     */
    override suspend fun createIndex(selector: String): Unit =
        mutex.withLock {
            if (hasIndex(selector)) return

            val index = store.getMap("index:$name:$selector").asIndex()
            val query = selector.split(".")

            iterateAll()
                .mapNotNull {
                    val id = it.getTypedId(idSerializer, json) ?: return@mapNotNull null
                    val idAsLong = id.hashCode().toLong() // Convert ID to Long for index storage
                    val selectResult = it.select(query) ?: return@mapNotNull null
                    selectResult to idAsLong
                }
                .chunked(100)
                .map {
                    it.groupBy(
                        keySelector = { it.first },
                        valueTransform = { it.second },
                    ).mapValues { it.value.toSet() }
                }
                .flatMapConcat { it.entries.asFlow() }
                .collect { (fieldValue, ids) ->
                    index.update(fieldValue, ids) { it + ids }
                }
            indexMap.update(name, listOf(selector)) { it + selector }
        }

    /**
     * Retrieves a JSON object from the persistent collection by its unique identifier.
     *
     * @param id The unique identifier of the JSON object to be retrieved.
     * @return The decoded [JsonObject] if found, or `null` if no entry exists with the given `id`.
     */
    public suspend fun findById(id: ID): JsonObject? {
        return persistentCollection.get(id)?.let { json.decodeFromString<JsonObject>(it) }
    }

    /**
     * Searches for JSON objects in the collection that match the specified selector and value.
     *
     * @param selector A dot-separated string representing the field to search within the JSON objects.
     * @param value The value of the field to match in the JSON objects.
     * @return A [Flow] of [JsonObject] representing the JSON objects that meet the search criteria.
     */
    public fun find(
        selector: String,
        value: JsonElement,
    ): Flow<JsonObject> {
        // For typed IDs, we can't efficiently use hash-based indexes since we can't reconstruct IDs
        // So we fall back to full scan for now
        return iterateAll().filter { it.select(selector) == value }
    }

    /**
     * Removes a JSON object from the collection by its unique identifier.
     *
     * @param id The unique identifier of the JSON object to be removed.
     * @return The removed JSON object as a [JsonObject], or `null` if no object
     *         with the specified ID was found.
     */
    public suspend fun removeById(id: ID): JsonObject? =
        mutex.withLock { removeByIdUnsafe(id) }

    private suspend fun removeByIdUnsafe(id: ID): JsonObject? {
        val jsonString = persistentCollection.remove(id) ?: return null
        val jsonObject = json.parseToJsonElement(jsonString).jsonObject

        indexMap.get(name)
            ?.forEach { fieldSelector ->
                getIndexOrNull(fieldSelector)
                    ?.update(
                        key = jsonObject.select(fieldSelector) ?: return@forEach,
                        default = emptySet(),
                        updater = { it - id.hashCode().toLong() },
                    )
            }
        return jsonObject
    }

    /**
     * Removes documents that match the specified field selector and value.
     *
     * @param fieldSelector The field selector to match.
     * @param fieldValue The value to match.
     * @return `true` if any documents were removed, `false` otherwise.
     */
    override suspend fun removeWhere(fieldSelector: String, fieldValue: JsonElement): Boolean =
        mutex.withLock {
            var removed = false
            find(fieldSelector, fieldValue)
                .collect { doc ->
                    val id = doc.getTypedId(idSerializer, json)
                    if (id != null) {
                        removeByIdUnsafe(id)
                        removed = true
                    }
                }
            removed
        }

    /**
     * Inserts the given JSON object into the collection.
     *
     * @param value The JSON object to be inserted into the collection.
     * @param initialId The initial ID value to use when generating an ID for the first time.
     * @param idGenerator A function that generates the next ID. If null, an ID must be present in the object.
     * @return The JSON object after insertion, enriched with an `_id` field.
     */
    public suspend fun insert(
        value: JsonObject,
        initialId: ID? = null,
        idGenerator: ((ID) -> ID)? = null,
    ): JsonObject = mutex.withLock { insertUnsafe(value, initialId, idGenerator) }

    private suspend fun insertUnsafe(
        value: JsonObject,
        initialId: ID? = null,
        idGenerator: ((ID) -> ID)? = null,
    ): JsonObject {
        val id = value.getTypedId(idSerializer, json)
            ?: (if (idGenerator != null && initialId != null) generateId(initialId, idGenerator)
                else error("No ID found in object and no ID generator (with initial value) provided"))

        val jsonObjectWithId = value.copyWithTypedId(id, idSerializer, json)
        val jsonString = json.encodeToString<JsonObject>(jsonObjectWithId)

        persistentCollection.put(id, jsonString)
        indexMap.get(name)
            ?.forEach { fieldSelector ->
                getIndexOrNull(fieldSelector)
                    ?.update(
                        key = jsonObjectWithId.select(fieldSelector) ?: return@forEach,
                        default = setOf(id.hashCode().toLong()),
                        updater = { it + id.hashCode().toLong() },
                    )
            }

        return jsonObjectWithId
    }

    /**
     * Retrieves the total number of elements in the collection.
     *
     * @return The total count of elements in the collection as a [Long].
     */
    override suspend fun size(): Long = persistentCollection.size()

    /**
     * Removes all elements from the collection, leaving it empty.
     */
    override suspend fun clear(): Unit = persistentCollection.clear()

    /**
     * Retrieves a list of all index selectors present in the collection.
     *
     * @return A list of strings where each string represents an index selector.
     */
    override suspend fun getAllIndexNames(): List<String> = indexMap.get(name) ?: emptyList()

    /**
     * Retrieves an index mapping for the specified JSON selector.
     *
     * @param selector A dot-separated string used to navigate through the JSON structure.
     * @return A map where keys are [JsonElement] values and values are sets of document identifiers.
     */
    override suspend fun getIndex(selector: String): Map<JsonElement, Set<Long>>? =
        getIndexOrNull(selector)
            ?.entries()
            ?.let { flow ->
                buildMap {
                    flow.collect { entry ->
                        put(entry.key, entry.value)
                    }
                }
            }

    /**
     * Drops an index from the collection.
     *
     * @param selector A dot-separated string used to navigate through the JSON structure.
     */
    override suspend fun dropIndex(selector: String): Unit =
        mutex.withLock {
            store.deleteMap("index:$name:$selector")
            indexMap.update(name, emptyList()) { it - selector }
        }

    /**
     * Retrieves detailed information about the collection.
     *
     * @return A [CollectionDetails] object containing metadata.
     */
    override suspend fun details(): CollectionDetails =
        CollectionDetails(
            idGeneratorState = 0L, // Typed IDs don't have a numeric state
            indexes =
                indexMap.get(name)?.mapNotNull { selector ->
                    getIndex(selector)
                        ?.let { selector to it }
                }
                    ?.toMap()
                    ?: emptyMap(),
        )

    /**
     * Updates a JSON object in the collection by its unique identifier.
     *
     * @param id The unique identifier of the entity to update.
     * @param update A suspend function that takes the current state and returns the updated state.
     * @return `true` if the object was updated successfully, `false` otherwise.
     */
    public suspend fun updateById(
        id: ID,
        update: suspend (JsonObject) -> JsonObject?,
    ): Boolean = mutex.withLock { updateByIdUnsafe(id, update) }

    private suspend fun updateByIdUnsafe(
        id: ID,
        update: suspend (JsonObject) -> JsonObject?,
    ): Boolean {
        val item = persistentCollection.get(id) ?: return false
        val jsonObject = json.decodeFromString<JsonObject>(item)
        val newItem = update(jsonObject)?.copyWithTypedId(id, idSerializer, json) ?: return false
        insertUnsafe(newItem, null)
        return true
    }
}

/**
 * Extension function to extract a typed ID from a JsonObject.
 */
internal fun <ID : Any> JsonObject.getTypedId(
    serializer: KSerializer<ID>,
    json: Json,
): ID? {
    val idElement = this[KotlinDocumentStore.ID_PROPERTY_NAME] ?: return null
    return try {
        json.decodeFromJsonElement(serializer, idElement)
    } catch (_: Exception) {
        null
    }
}

/**
 * Extension function to copy a JsonObject with a typed ID.
 */
internal fun <ID : Any> JsonObject.copyWithTypedId(
    id: ID,
    serializer: KSerializer<ID>,
    json: Json,
): JsonObject {
    val idElement = json.encodeToJsonElement(serializer, id)
    return JsonObject(this.toMutableMap().apply {
        put(KotlinDocumentStore.ID_PROPERTY_NAME, idElement)
    })
}
