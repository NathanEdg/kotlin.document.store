@file:Suppress("FunctionName")

package com.github.lamba92.kotlin.document.store.tests

import com.github.lamba92.kotlin.document.store.core.maps.asTypedIdGenerator
import com.github.lamba92.kotlin.document.store.core.maps.asTypedCollectionMap
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestResult
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Abstract base class for testing the TypedIdGenerator functionality.
 *
 * Tests the core typed ID generator components:
 * - TypedIdGenerator with String serialization
 * - TypedIdGenerator with Int serialization
 * - TypedPersistentCollection with typed keys
 * - Proper serialization and deserialization of typed IDs
 */
public abstract class AbstractTypedIdGeneratorTests(store: DataStoreProvider) : BaseTest(store) {
    public companion object {
        public const val TEST_NAME_1: String = "typed_id_generator_stores_and_retrieves_strings"
        public const val TEST_NAME_2: String = "typed_id_generator_stores_and_retrieves_ints"
        public const val TEST_NAME_3: String = "typed_id_generator_updates_correctly"
        public const val TEST_NAME_4: String = "typed_persistent_collection_with_string_keys"
        public const val TEST_NAME_5: String = "typed_persistent_collection_with_int_keys"
        public const val TEST_NAME_6: String = "typed_id_generator_handles_custom_serializable_types"
        public const val TEST_NAME_7: String = "typed_persistent_collection_entries_iteration"
    }

    @Test
    public fun typedIdGeneratorStoresAndRetrievesStrings(): TestResult =
        runDatabaseTest(TEST_NAME_1) { db ->
            val map = db.store.getMap("test-gen")
            val json = Json { }
            val generator = map.asTypedIdGenerator(serializer<String>(), json)

            // Put some values
            generator.put("collection1", "id-001")
            generator.put("collection2", "id-002")

            // Retrieve values
            val value1 = generator.get("collection1")
            val value2 = generator.get("collection2")

            assertEquals("id-001", value1)
            assertEquals("id-002", value2)
        }

    @Test
    public fun typedIdGeneratorStoresAndRetrievesInts(): TestResult =
        runDatabaseTest(TEST_NAME_2) { db ->
            val map = db.store.getMap("test-gen")
            val json = Json { }
            val generator = map.asTypedIdGenerator(serializer<Int>(), json)

            generator.put("collection1", 100)
            generator.put("collection2", 200)

            assertEquals(100, generator.get("collection1"))
            assertEquals(200, generator.get("collection2"))
        }

    @Test
    public fun typedIdGeneratorUpdatesCorrectly(): TestResult =
        runDatabaseTest(TEST_NAME_3) { db ->
            val map = db.store.getMap("test-gen")
            val json = Json { }
            val generator = map.asTypedIdGenerator(serializer<Int>(), json)

            // Test update with incrementing
            // When key doesn't exist, the default value (first argument) is stored directly
            // The updater is only applied when the key already exists
            val result1 = generator.update("counter", 1) { it + 1 }
            assertNull(result1.oldValue) // First time, no prior value exists
            assertEquals(1, result1.newValue) // Default value is stored

            val result2 = generator.update("counter", 1) { it + 1 }
            assertEquals(1, result2.oldValue)
            assertEquals(2, result2.newValue) // Updater applied to existing value

            val result3 = generator.update("counter", 1) { it + 1 }
            assertEquals(2, result3.oldValue)
            assertEquals(3, result3.newValue)
        }

    @Test
    public fun typedPersistentCollectionWithStringKeys(): TestResult =
        runDatabaseTest(TEST_NAME_4) { db ->
            val map = db.store.getMap("test-collection")
            val json = Json { }
            val collection = map.asTypedCollectionMap(serializer<String>(), json)

            // Store documents with string keys
            collection.put("user-001", """{"name":"alice","age":25}""")
            collection.put("user-002", """{"name":"bob","age":30}""")

            val doc1 = collection.get("user-001")
            val doc2 = collection.get("user-002")

            assertNotNull(doc1)
            assertNotNull(doc2)
            assertTrue(doc1.contains("alice"))
            assertTrue(doc2.contains("bob"))

            assertEquals(2L, collection.size())
        }

    @Test
    public fun typedPersistentCollectionWithIntKeys(): TestResult =
        runDatabaseTest(TEST_NAME_5) { db ->
            val map = db.store.getMap("test-collection")
            val json = Json { }
            val collection = map.asTypedCollectionMap(serializer<Int>(), json)

            collection.put(1, """{"name":"user1"}""")
            collection.put(2, """{"name":"user2"}""")
            collection.put(42, """{"name":"answer"}""")

            assertEquals("""{"name":"user1"}""", collection.get(1))
            assertEquals("""{"name":"user2"}""", collection.get(2))
            assertEquals("""{"name":"answer"}""", collection.get(42))

            val removed = collection.remove(2)
            assertEquals("""{"name":"user2"}""", removed)
            assertNull(collection.get(2))
        }

    @Test
    public fun typedIdGeneratorHandlesCustomSerializableTypes(): TestResult =
        runDatabaseTest(TEST_NAME_6) { db ->
            val map = db.store.getMap("test-gen")
            val json = Json { }
            val generator = map.asTypedIdGenerator(serializer<CustomId>(), json)

            val id1 = CustomId("prefix", 123)
            val id2 = CustomId("prefix", 456)

            generator.put("doc1", id1)
            generator.put("doc2", id2)

            val retrieved1 = generator.get("doc1")
            val retrieved2 = generator.get("doc2")

            assertEquals(id1, retrieved1)
            assertEquals(id2, retrieved2)
        }

    @Test
    public fun typedPersistentCollectionEntriesIteration(): TestResult =
        runDatabaseTest(TEST_NAME_7) { db ->
            val map = db.store.getMap("test-collection")
            val json = Json { }
            val collection = map.asTypedCollectionMap(serializer<String>(), json)

            // Insert multiple documents
            val documents = mapOf(
                "id-1" to """{"data":"value1"}""",
                "id-2" to """{"data":"value2"}""",
                "id-3" to """{"data":"value3"}"""
            )

            documents.forEach { (key, value) ->
                collection.put(key, value)
            }

            // Iterate and verify
            val entries = collection.entries().toList()
            assertEquals(3, entries.size)

            val keys = entries.map { it.key }.toSet()
            assertTrue(keys.contains("id-1"))
            assertTrue(keys.contains("id-2"))
            assertTrue(keys.contains("id-3"))

            val values = entries.map { it.value }
            assertTrue(values.any { it.contains("value1") })
            assertTrue(values.any { it.contains("value2") })
            assertTrue(values.any { it.contains("value3") })
        }
}

/**
 * Custom serializable ID type for testing.
 */
@Serializable
public data class CustomId(
    val prefix: String,
    val number: Int,
)

