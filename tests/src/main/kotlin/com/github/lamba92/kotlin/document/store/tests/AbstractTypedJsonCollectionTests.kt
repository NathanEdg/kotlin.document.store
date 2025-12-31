@file:Suppress("FunctionName")

package com.github.lamba92.kotlin.document.store.tests

import com.github.lamba92.kotlin.document.store.core.getTypedJsonCollection
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestResult
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Abstract base class for testing TypedJsonCollection functionality.
 *
 * Tests the strongly-typed JSON collection with custom ID types:
 * - Insert, retrieve, update, delete operations on JSON objects
 * - Index creation and usage with typed IDs
 * - removeWhere functionality
 * - Collection size and clearing
 */
public abstract class AbstractTypedJsonCollectionTests(store: DataStoreProvider) : BaseTest(store) {
    public companion object {
        public const val TEST_NAME_1: String = "typed_json_collection_inserts_and_retrieves"
        public const val TEST_NAME_2: String = "typed_json_collection_with_index"
        public const val TEST_NAME_3: String = "typed_json_collection_updates"
        public const val TEST_NAME_4: String = "typed_json_collection_removes"
        public const val TEST_NAME_5: String = "typed_json_collection_find"
        public const val TEST_NAME_6: String = "typed_json_collection_remove_where"
        public const val TEST_NAME_7: String = "typed_json_collection_iterate_all"
        public const val TEST_NAME_8: String = "typed_json_collection_size_and_clear"
    }

    @Test
    public fun typedJsonCollectionInsertsAndRetrieves(): TestResult =
        runDatabaseTest(TEST_NAME_1) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            val doc = buildJsonObject {
                put("name", JsonPrimitive("alice"))
                put("age", JsonPrimitive(25))
                put("_id", JsonPrimitive("user-001"))
            }

            val inserted = collection.insert(doc)

            assertEquals("user-001", inserted["_id"]?.toString()?.trim('"'))

            val retrieved = collection.findById("user-001")
            assertNotNull(retrieved)
            assertEquals("alice", retrieved["name"]?.toString()?.trim('"'))
        }

    @Test
    public fun typedJsonCollectionWithIndex(): TestResult =
        runDatabaseTest(TEST_NAME_2) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            collection.createIndex("name")

            val doc1 = buildJsonObject {
                put("name", JsonPrimitive("bob"))
                put("age", JsonPrimitive(30))
                put("_id", JsonPrimitive("user-001"))
            }

            val doc2 = buildJsonObject {
                put("name", JsonPrimitive("charlie"))
                put("age", JsonPrimitive(35))
                put("_id", JsonPrimitive("user-002"))
            }

            collection.insert(doc1)
            collection.insert(doc2)

            val indexes = collection.getAllIndexNames()
            assertTrue(indexes.contains("name"), "Should have name index")

            val indexData = collection.getIndex("name")
            assertNotNull(indexData)
            assertTrue(indexData.isNotEmpty())
        }

    @Test
    public fun typedJsonCollectionUpdates(): TestResult =
        runDatabaseTest(TEST_NAME_3) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            val doc = buildJsonObject {
                put("name", JsonPrimitive("dave"))
                put("age", JsonPrimitive(40))
                put("_id", JsonPrimitive("user-dave"))
            }

            collection.insert(doc)

            val updated = collection.updateById("user-dave") { current ->
                buildJsonObject {
                    put("name", current["name"]!!)
                    put("age", JsonPrimitive(41))
                    put("_id", current["_id"]!!)
                }
            }

            assertTrue(updated)

            val retrieved = collection.findById("user-dave")
            assertNotNull(retrieved)
            assertEquals(41, retrieved["age"]?.toString()?.toIntOrNull())
        }

    @Test
    public fun typedJsonCollectionRemoves(): TestResult =
        runDatabaseTest(TEST_NAME_4) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            val doc = buildJsonObject {
                put("name", JsonPrimitive("eve"))
                put("_id", JsonPrimitive("user-eve"))
            }

            collection.insert(doc)

            val removed = collection.removeById("user-eve")
            assertNotNull(removed)

            val retrieved = collection.findById("user-eve")
            assertNull(retrieved)
        }

    @Test
    public fun typedJsonCollectionFind(): TestResult =
        runDatabaseTest(TEST_NAME_5) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            val doc1 = buildJsonObject {
                put("status", JsonPrimitive("active"))
                put("_id", JsonPrimitive("doc-1"))
            }

            val doc2 = buildJsonObject {
                put("status", JsonPrimitive("active"))
                put("_id", JsonPrimitive("doc-2"))
            }

            val doc3 = buildJsonObject {
                put("status", JsonPrimitive("inactive"))
                put("_id", JsonPrimitive("doc-3"))
            }

            collection.insert(doc1)
            collection.insert(doc2)
            collection.insert(doc3)

            val activeDocuments = collection.find("status", JsonPrimitive("active")).toList()

            assertEquals(2, activeDocuments.size)
        }

    @Test
    public fun typedJsonCollectionRemoveWhere(): TestResult =
        runDatabaseTest(TEST_NAME_6) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            collection.insert(buildJsonObject {
                put("category", JsonPrimitive("temp"))
                put("_id", JsonPrimitive("doc-1"))
            })

            collection.insert(buildJsonObject {
                put("category", JsonPrimitive("temp"))
                put("_id", JsonPrimitive("doc-2"))
            })

            collection.insert(buildJsonObject {
                put("category", JsonPrimitive("keep"))
                put("_id", JsonPrimitive("doc-3"))
            })

            val removed = collection.removeWhere("category", JsonPrimitive("temp"))
            assertTrue(removed, "Should have removed documents")

            assertEquals(1L, collection.size(), "Should have 1 document remaining")

            val remaining = collection.findById("doc-3")
            assertNotNull(remaining)
        }

    @Test
    public fun typedJsonCollectionIterateAll(): TestResult =
        runDatabaseTest(TEST_NAME_7) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            (1..5).forEach { i ->
                collection.insert(buildJsonObject {
                    put("number", JsonPrimitive(i))
                    put("_id", JsonPrimitive("doc-$i"))
                })
            }

            val allDocs = collection.iterateAll().toList()
            assertEquals(5, allDocs.size)

            val numbers = allDocs.mapNotNull { it["number"]?.toString()?.toIntOrNull() }.sorted()
            assertEquals(listOf(1, 2, 3, 4, 5), numbers)
        }

    @Test
    public fun typedJsonCollectionSizeAndClear(): TestResult =
        runDatabaseTest(TEST_NAME_8) { db ->
            val collection = db.getTypedJsonCollection<String>("test")

            assertEquals(0L, collection.size())

            (1..10).forEach { i ->
                collection.insert(buildJsonObject {
                    put("value", JsonPrimitive(i))
                    put("_id", JsonPrimitive("doc-$i"))
                })
            }

            assertEquals(10L, collection.size())

            collection.clear()

            assertEquals(0L, collection.size())
        }
}

