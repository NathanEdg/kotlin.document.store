@file:Suppress("FunctionName")

package com.github.lamba92.kotlin.document.store.tests

import com.github.lamba92.kotlin.document.store.core.getTypedObjectCollection
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Abstract base class for testing typed ID functionality within a document store.
 *
 * Tests the new strongly-typed ID system with String IDs, ensuring that:
 * - Documents can be inserted with custom String IDs
 * - Documents can be retrieved by String ID
 * - Documents can be updated by String ID
 * - Documents can be removed by String ID
 * - Collections work correctly with typed IDs
 */
public abstract class AbstractTypedIdStringTests(store: DataStoreProvider) : BaseTest(store) {
    public companion object {
        public const val TEST_NAME_1: String = "inserts_and_retrieves_document_with_string_id"
        public const val TEST_NAME_2: String = "inserts_document_with_custom_string_id"
        public const val TEST_NAME_3: String = "updates_document_with_string_id"
        public const val TEST_NAME_4: String = "removes_document_with_string_id"
        public const val TEST_NAME_5: String = "finds_documents_with_string_id"
        public const val TEST_NAME_6: String = "counts_documents_correctly_with_string_id"
        public const val TEST_NAME_7: String = "clears_collection_with_string_id"
        public const val TEST_NAME_8: String = "multiple_inserts_with_string_id"
    }

    @Test
    public fun insertsAndRetrievesDocumentWithStringId(): TestResult =
        runDatabaseTest(TEST_NAME_1) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            val user = TypedUser<String>(
                name = "alice",
                age = 25,
                id = "user-001"
            )

            val inserted = collection.insert(user) { prev -> "user-${prev.substringAfter("-").toInt() + 1}" }

            assertEquals(
                expected = user.copy(id = "user-001"),
                actual = inserted,
                message = "Inserted user should have the provided ID"
            )

            val retrieved = collection.findById("user-001")
            assertNotNull(retrieved, "Should find user by ID")
            assertEquals(
                expected = user,
                actual = retrieved,
                message = "Retrieved user should match inserted user"
            )
        }

    @Test
    public fun insertsDocumentWithCustomStringId(): TestResult =
        runDatabaseTest(TEST_NAME_2) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            val user1 = TypedUser<String>(name = "bob", age = 30, id = "custom-id-123")
            val user2 = TypedUser<String>(name = "charlie", age = 35, id = "custom-id-456")

            val inserted1 = collection.insert(user1)
            val inserted2 = collection.insert(user2)

            assertEquals("custom-id-123", inserted1.id)
            assertEquals("custom-id-456", inserted2.id)

            val retrieved1 = collection.findById("custom-id-123")
            val retrieved2 = collection.findById("custom-id-456")

            assertNotNull(retrieved1)
            assertNotNull(retrieved2)
            assertEquals(user1, retrieved1)
            assertEquals(user2, retrieved2)
        }

    @Test
    public fun updatesDocumentWithStringId(): TestResult =
        runDatabaseTest(TEST_NAME_3) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            val user = TypedUser<String>(name = "dave", age = 40, id = "user-dave")
            collection.insert(user)

            val updated = collection.updateById("user-dave") { current ->
                current.copy(age = 41)
            }

            assertTrue(updated, "Update should succeed")

            val retrieved = collection.findById("user-dave")
            assertNotNull(retrieved)
            assertEquals(41, retrieved.age, "Age should be updated")
            assertEquals("dave", retrieved.name, "Name should remain unchanged")
        }

    @Test
    public fun removesDocumentWithStringId(): TestResult =
        runDatabaseTest(TEST_NAME_4) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            val user = TypedUser<String>(name = "eve", age = 28, id = "user-eve")
            collection.insert(user)

            val removed = collection.removeById("user-eve")
            assertNotNull(removed, "Should return removed user")
            assertEquals(user, removed)

            val retrieved = collection.findById("user-eve")
            assertNull(retrieved, "User should no longer exist")
        }

    @Test
    public fun findsDocumentsWithStringId(): TestResult =
        runDatabaseTest(TEST_NAME_5) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            collection.insert(TypedUser(name = "frank", age = 25, id = "user-001"))
            collection.insert(TypedUser(name = "grace", age = 25, id = "user-002"))
            collection.insert(TypedUser(name = "henry", age = 30, id = "user-003"))

            val users25 = collection.find("age", 25, kotlinx.serialization.serializer()).toList()

            assertEquals(2, users25.size, "Should find 2 users aged 25")
            assertTrue(users25.any { it.name == "frank" })
            assertTrue(users25.any { it.name == "grace" })
        }

    @Test
    public fun countsDocumentsCorrectlyWithStringId(): TestResult =
        runDatabaseTest(TEST_NAME_6) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            assertEquals(0L, collection.size(), "Collection should start empty")

            collection.insert(TypedUser(name = "user1", age = 20, id = "id-1"))
            collection.insert(TypedUser(name = "user2", age = 21, id = "id-2"))
            collection.insert(TypedUser(name = "user3", age = 22, id = "id-3"))

            assertEquals(3L, collection.size(), "Collection should have 3 documents")
        }

    @Test
    public fun clearsCollectionWithStringId(): TestResult =
        runDatabaseTest(TEST_NAME_7) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            collection.insert(TypedUser(name = "user1", age = 20, id = "id-1"))
            collection.insert(TypedUser(name = "user2", age = 21, id = "id-2"))

            assertEquals(2L, collection.size())

            collection.clear()

            assertEquals(0L, collection.size(), "Collection should be empty after clear")
        }

    @Test
    public fun multipleInsertsWithStringId(): TestResult =
        runDatabaseTest(TEST_NAME_8) { db ->
            val collection = db.getTypedObjectCollection<TypedUser<String>, String>("test")

            val users = (1..100).map { i ->
                TypedUser(name = "user$i", age = 20 + (i % 10), id = "id-$i")
            }

            users.forEach { collection.insert(it) }

            assertEquals(100L, collection.size(), "Should have 100 documents")

            val retrieved = collection.findById("id-50")
            assertNotNull(retrieved)
            assertEquals("user50", retrieved.name)
        }
}

/**
 * Test data class for typed ID tests with generic ID type.
 */
@Serializable
public data class TypedUser<ID>(
    val name: String,
    val age: Int,
    @SerialName("_id") val id: ID? = null,
)

