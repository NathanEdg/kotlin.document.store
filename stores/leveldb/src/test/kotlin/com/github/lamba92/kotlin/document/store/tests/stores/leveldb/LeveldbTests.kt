@file:Suppress("unused")

package com.github.lamba92.kotlin.document.store.tests.stores.leveldb

import com.github.lamba92.kotlin.document.store.core.DataStore
import com.github.lamba92.kotlin.document.store.stores.leveldb.LevelDBStore
import com.github.lamba92.kotlin.document.store.tests.AbstractDeleteTests
import com.github.lamba92.kotlin.document.store.tests.AbstractDocumentDatabaseTests
import com.github.lamba92.kotlin.document.store.tests.AbstractFindTests
import com.github.lamba92.kotlin.document.store.tests.AbstractIndexTests
import com.github.lamba92.kotlin.document.store.tests.AbstractInsertTests
import com.github.lamba92.kotlin.document.store.tests.AbstractObjectCollectionTests
import com.github.lamba92.kotlin.document.store.tests.AbstractTypedIdGeneratorTests
import com.github.lamba92.kotlin.document.store.tests.AbstractTypedIdIntTests
import com.github.lamba92.kotlin.document.store.tests.AbstractTypedIdStringTests
import com.github.lamba92.kotlin.document.store.tests.AbstractTypedJsonCollectionTests
import com.github.lamba92.kotlin.document.store.tests.AbstractUpdateTests
import com.github.lamba92.kotlin.document.store.tests.DataStoreProvider
import com.github.lamba92.leveldb.LevelDB
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.file.Files.createDirectories
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteRecursively

class LevelDBDeleteTests : AbstractDeleteTests(LevelDBStoreProvider)

class LevelDBDocumentDatabaseTests : AbstractDocumentDatabaseTests(LevelDBStoreProvider)

class LevelDBIndexTests : AbstractIndexTests(LevelDBStoreProvider)

class LevelDBInsertTests : AbstractInsertTests(LevelDBStoreProvider)

class LevelDBUpdateTests : AbstractUpdateTests(LevelDBStoreProvider)

class LevelDBFindTests : AbstractFindTests(LevelDBStoreProvider)

class LevelDBObjectCollectionTests : AbstractObjectCollectionTests(LevelDBStoreProvider)

class LevelDBTypedIdStringTests : AbstractTypedIdStringTests(LevelDBStoreProvider)

class LevelDBTypedIdIntTests : AbstractTypedIdIntTests(LevelDBStoreProvider)

class LevelDBTypedIdGeneratorTests : AbstractTypedIdGeneratorTests(LevelDBStoreProvider)

class LevelDBTypedJsonCollectionTests : AbstractTypedJsonCollectionTests(LevelDBStoreProvider)

object LevelDBStoreProvider : DataStoreProvider {
    private fun getDbPath(testName: String) = Path(DB_PATH).resolve(testName)

    @OptIn(ExperimentalPathApi::class)
    override suspend fun deleteDatabase(testName: String) =
        withContext(Dispatchers.IO) {
            getDbPath(testName).deleteRecursively()
        }

    override fun provide(testName: String): DataStore =
        LevelDBStore(
            LevelDB(
                getDbPath(testName).createDirectories().toString(),
            ),
        )
}

fun Path.resolve(vararg other: String): Path = other.fold(this) { acc, s -> acc.resolve(s) }

fun Path.createDirectories(): Path {
    createDirectories(this)
    return this
}

val DB_PATH: String = System.getProperty("java.io.tmpdir") + "/leveldb_tests"