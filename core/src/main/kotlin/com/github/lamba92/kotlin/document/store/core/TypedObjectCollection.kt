package com.github.lamba92.kotlin.document.store.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * `TypedObjectCollection` provides an abstraction for managing a collection of @[Serializable] objects
 * with strongly-typed IDs using [TypedJsonCollection] as a backing data store.
 *
 * @param T The type of objects stored in the collection. Must be a non-null type.
 * @param ID The type of identifiers used in this collection.
 * @param serializer The serializer used for encoding and decoding objects of type `T`.
 * @param typedJsonCollection The underlying typed JSON-based collection for storing serialized objects.
 */
public class TypedObjectCollection<T : Any, ID : Any>(
    private val serializer: KSerializer<T>,
    public val typedJsonCollection: TypedJsonCollection<ID>,
) : KotlinDocumentStoreCollection by typedJsonCollection {

    /**
     * Searches for objects in the collection that match the specified selector and value.
     *
     * @param selector A dot-separated string representing the field to search within the JSON objects.
     * @param value The value of the field to match in the JSON objects.
     * @param valueSerializer The serializer for the value type [K].
     * @return A [Flow]<[T]> representing the objects that meet the search criteria.
     */
    public suspend fun <K> find(
        selector: String,
        value: K,
        valueSerializer: KSerializer<K>,
    ): Flow<T> =
        typedJsonCollection.find(selector, json.encodeToJsonElement(valueSerializer, value))
            .map { json.decodeFromJsonElement(serializer, it) }

    /**
     * Inserts the given [value] into the collection.
     *
     * If the object does not have an `_id` field, an ID must be generated using the provided generator.
     * The `_id` field type matches the collection's ID type.
     *
     * When deserializing the object, if the `_id` field is not present in the class
     * definition, it is ignored.
     *
     * The annotation @[SerialName] can be used to map the `_id` to a different name in the class definition.
     *
     * @param value The object to be inserted into the collection.
     * @param idGenerator A function that generates the next ID from the previous ID.
     *                    The function receives the initial ID (for first insert) or the previous ID.
     * @return The object after insertion, enriched with an `_id` field.
     */
    public suspend fun insert(
        value: T,
        idGenerator: ((ID) -> ID)? = null,
    ): T {
        val jsonObject = json.encodeToJsonElement(serializer, value)
        if (jsonObject !is JsonObject) {
            val s =
                when (jsonObject) {
                    is JsonArray -> "an array-like object"
                    is JsonPrimitive -> "a primitive"
                    JsonNull -> "null"
                    else -> "an unknown type"
                }
            error("Expected an object but got $s")
        }
        return json.decodeFromJsonElement(serializer, typedJsonCollection.insert(jsonObject, null, idGenerator))
    }

    /**
     * Inserts the given [value] into the collection with a custom initial ID for the generator.
     *
     * If the object does not have an `_id` field, an ID will be generated using the provided
     * initial ID and generator function.
     *
     * @param value The object to be inserted into the collection.
     * @param initialId The initial ID value to use when generating an ID for the first time.
     * @param idGenerator A function that generates the next ID from the previous ID.
     * @return The object after insertion, enriched with an `_id` field.
     */
    public suspend fun insert(
        value: T,
        initialId: ID,
        idGenerator: (ID) -> ID,
    ): T {
        val jsonObject = json.encodeToJsonElement(serializer, value)
        if (jsonObject !is JsonObject) {
            val s =
                when (jsonObject) {
                    is JsonArray -> "an array-like object"
                    is JsonPrimitive -> "a primitive"
                    JsonNull -> "null"
                    else -> "an unknown type"
                }
            error("Expected an object but got $s")
        }
        return json.decodeFromJsonElement(serializer, typedJsonCollection.insert(jsonObject, initialId, idGenerator))
    }

    /**
     * Removes an object from the collection by its unique identifier.
     *
     * @param id The unique identifier of the object to be removed.
     * @return The removed object as a [T], or `null` if no object with the specified ID was found.
     */
    public suspend fun removeById(id: ID): T? =
        typedJsonCollection.removeById(id)
            ?.let { json.decodeFromJsonElement(serializer, it) }

    /**
     * Retrieves an object from the persistent collection by its unique identifier.
     *
     * @param id The unique identifier of the object to be retrieved.
     * @return The object if found, or `null` if no entry exists with the given `id`.
     */
    public suspend fun findById(id: ID): T? =
        typedJsonCollection.findById(id)
            ?.let { json.decodeFromJsonElement(serializer, it) }

    /**
     * Updates an object in the collection by its unique identifier.
     *
     * @param id The unique identifier of the entity to update.
     * @param update A suspend function that takes the current state of the entity as [T]
     * and returns the updated state. The function is executed within the collection's lock.
     * @return `true` if the object was updated successfully, `false` otherwise.
     */
    public suspend fun updateById(
        id: ID,
        update: suspend (T) -> T?,
    ): Boolean =
        typedJsonCollection.updateById(id) { jsonObject ->
            val obj = json.decodeFromJsonElement(serializer, jsonObject)
            val updated = update(obj) ?: return@updateById null
            val updatedJson = json.encodeToJsonElement(serializer, updated)
            if (updatedJson is JsonObject) updatedJson else null
        }
}
