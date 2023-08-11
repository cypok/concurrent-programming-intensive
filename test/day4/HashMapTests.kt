package day4

import TestBase
import org.jetbrains.kotlinx.lincheck.Options
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.junit.Test
import kotlin.test.assertEquals

class SingleWriterHashTableTest : TestBase(
    sequentialSpecification = SequentialHashTableIntInt::class,
    scenarios = 300
) {
    private val hashTable = SingleWriterHashTable<Int, Int>(initialCapacity = 2)

    @Operation(nonParallelGroup = "writer")
    fun put(key: Int, value: Int): Int? = hashTable.put(key, value)

    @Operation
    fun get(key: Int): Int? = hashTable.get(key)

    @Operation(nonParallelGroup = "writer")
    fun remove(key: Int): Int? = hashTable.remove(key)

@Test
fun testResizeWithCollisions() {
    val baseContent = listOf(-2 to 30, 1 to 20, -1 to 40, 0 to 10)
    for ((k, v) in baseContent) {
        put(k, v)
    }
    for ((k, v) in baseContent) {
        assertEquals(v, get(k))
    }

    val extra = 2 to 60
    extra.let { (k, v) -> put(k, v) }

    for ((k, v) in baseContent + extra) {
        assertEquals(v, get(k))
    }
}
}

class ConcurrentHashTableWithoutResizeTest : TestBase(
    sequentialSpecification = SequentialHashTableIntInt::class,
    scenarios = 300
) {
    private val hashTable = ConcurrentHashTableWithoutResize<Int, Int>(initialCapacity = 30)

    @Operation
    fun put(key: Int, value: Int): Int? = hashTable.put(key, value)

    @Operation
    fun get(key: Int): Int? = hashTable.get(key)

    @Operation
    fun remove(key: Int): Int? = hashTable.remove(key)
}

class ConcurrentHashTableTest : TestBase(
    sequentialSpecification = SequentialHashTableIntInt::class,
    checkObstructionFreedom = false, // FIXME: revert!!!!
    scenarios = 300
) {
    private val hashTable = ConcurrentHashTable<Int, Int>(initialCapacity = 2)

    @Operation
    fun put(key: Int, value: Int): Int? = hashTable.put(key, value)

    @Operation
    fun get(key: Int): Int? = hashTable.get(key)

    @Operation
    fun remove(key: Int): Int? = hashTable.remove(key)

    override fun Options<*, *>.customConfiguration() {
        // Skip stress testing. FIXME: revert!!!
        if (this is StressOptions) {
            iterations(0)
            return
        }
        addCustomScenario {
            parallel {
                thread {
                    actor(ConcurrentHashTableTest::put, 0, 10)
                }
                thread {
                    actor(ConcurrentHashTableTest::put, 1, 20)
                    actor(ConcurrentHashTableTest::put, -2, 30)
                }
                thread {
                    actor(ConcurrentHashTableTest::put, -1, 40)
                    actor(ConcurrentHashTableTest::put, 3, 50)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(ConcurrentHashTableTest::put, 0, 10)
                    actor(ConcurrentHashTableTest::get, 1)
                }
                thread {
                    actor(ConcurrentHashTableTest::put, 1, 20)
                    actor(ConcurrentHashTableTest::put, -2, 30)
                }
                thread {
                    actor(ConcurrentHashTableTest::put, -1, 40)
                    actor(ConcurrentHashTableTest::put, 3, 50)
                }
            }
        }

        addCustomScenario {
            initial {
                actor(ConcurrentHashTableTest::put, 0, 0)
            }
            parallel {
                thread {
                    actor(ConcurrentHashTableTest::remove, 0)
                }
                thread {
                    actor(ConcurrentHashTableTest::put, 0, -2)
                }
            }
        }
    }
}

class SequentialHashTableIntInt {
    private val map = HashMap<Int, Int>()

    fun put(key: Int, value: Int): Int? = map.put(key, value)

    fun get(key: Int): Int? = map.get(key)

    fun remove(key: Int): Int? = map.remove(key)
}