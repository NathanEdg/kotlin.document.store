@file:Suppress("FunctionName")

package com.github.lamba92.kotlin.document.store.tests

import com.github.lamba92.kotlin.document.store.core.getTypedObjectCollection
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.TestResult
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Abstract base class for testing typed ID functionality with Int IDs.
 *
 * Tests the strongly-typed ID system with Int IDs, ensuring that:
 * - Documents can be inserted with Int IDs using generators
 * - Documents can be retrieved by Int ID
 * - ID generation works correctly with increment logic
 * - Collections work correctly with Int typed IDs
 */
public abstract class AbstractTypedIdIntTests(store: DataStoreProvider) : BaseTest(store) {
    public companion object {
        public const val TEST_NAME_1: String = "inserts_with_int_id_generator"
        public const val TEST_NAME_2: String = "int_id_increments_correctly"
        public const val TEST_NAME_3: String = "retrieves_by_int_id"
        public const val TEST_NAME_4: String = "updates_with_int_id"
        public const val TEST_NAME_5: String = "removes_with_int_id"
        public const val TEST_NAME_6: String = "multiple_inserts_with_int_id_generator"
        public const val TEST_NAME_7: String = "finds_documents_with_int_id"
    }

    @Test
    public fun insertsWithIntIdGenerator(): TestResult =
        runDatabaseTest(TEST_NAME_1) { db ->
            val collection = db.getTypedObjectCollection<IntIdUser, Int>("test")

            // Insert without ID, using generator
            val user = IntIdUser(name = "alice", age = 25)
            val inserted = collection.insert(user, 0) { prev -> prev + 1 }

            assertNotNull(inserted.id, "ID should be generated")
            assertEquals(1, inserted.id, "First ID should be 1")
        }

    @Test
    public fun intIdIncrementsCorrectly(): TestResult =
        runDatabaseTest(TEST_NAME_2) { db ->
            val collection = db.getTypedObjectCollection<IntIdUser, Int>("test")

            val user1 = collection.insert(IntIdUser(name = "user1", age = 20), 0) { prev -> prev + 1 }
            val user2 = collection.insert(IntIdUser(name = "user2", age = 21), 0) { prev -> prev + 1 }
            val user3 = collection.insert(IntIdUser(name = "user3", age = 22), 0) { prev -> prev + 1 }

            assertEquals(1, user1.id)
            assertEquals(2, user2.id)
            assertEquals(3, user3.id)
        }

    @Test
    public fun retrievesByIntId(): TestResult =
        runDatabaseTest(TEST_NAME_3) { db ->
            val collection = db.getTypedObjectCollection<IntIdUser, Int>("test")

            val user = IntIdUser(name = "bob", age = 30, id = 42)
            collection.insert(user)

            val retrieved = collection.findById(42)
            assertNotNull(retrieved)
            assertEquals(user, retrieved)

            val notFound = collection.findById(999)
            assertNull(notFound)
        }

    @Test
    public fun updatesWithIntId(): TestResult =
        runDatabaseTest(TEST_NAME_4) { db ->
            val collection = db.getTypedObjectCollection<IntIdUser, Int>("test")

            collection.insert(IntIdUser(name = "charlie", age = 35, id = 100))

            val updated = collection.updateById(100) { current ->
                current.copy(age = 36)
            }

            assertTrue(updated)

            val retrieved = collection.findById(100)
            assertNotNull(retrieved)
            assertEquals(36, retrieved.age)
        }

    @Test
    public fun removesWithIntId(): TestResult =
        runDatabaseTest(TEST_NAME_5) { db ->
            val collection = db.getTypedObjectCollection<IntIdUser, Int>("test")

            collection.insert(IntIdUser(name = "dave", age = 40, id = 200))

            val removed = collection.removeById(200)
            assertNotNull(removed)
            assertEquals("dave", removed.name)

            assertNull(collection.findById(200))
        }

    @Test
    public fun multipleInsertsWithIntIdGenerator(): TestResult =
        runDatabaseTest(TEST_NAME_6) { db ->
            val collection = db.getTypedObjectCollection<IntIdUser, Int>("test")

            val users = (1..50).map { i ->
                collection.insert(IntIdUser(name = "user$i", age = 20 + i), 0) { prev -> prev + 1 }
            }

            assertEquals(50, users.size)

            // Verify IDs are sequential
            users.forEachIndexed { index, user ->
                assertEquals(index + 1, user.id, "ID should be ${index + 1}")
            }

            assertEquals(50L, collection.size())
        }

    @Test
    public fun findsDocumentsWithIntId(): TestResult =
        runDatabaseTest(TEST_NAME_7) { db ->
            val collection = db.getTypedObjectCollection<IntIdUser, Int>("test")

            collection.insert(IntIdUser(name = "eve", age = 25, id = 1))
            collection.insert(IntIdUser(name = "frank", age = 25, id = 2))
            collection.insert(IntIdUser(name = "grace", age = 30, id = 3))

            val users25 = collection.find("age", 25, kotlinx.serialization.serializer()).toList()

            assertEquals(2, users25.size)
            assertTrue(users25.any { it.name == "eve" })
            assertTrue(users25.any { it.name == "frank" })
        }
}

/**
 * Test data class for Int ID tests.
 */
@Serializable
public data class IntIdUser(
    val name: String,
    val age: Int,
    @SerialName("_id") val id: Int? = null,
)

