package day3

import TestBase
import org.jetbrains.kotlinx.lincheck.*
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.paramgen.*
import org.junit.Test
import kotlin.test.assertEquals

@Param(name = "index", gen = IntGen::class, conf = "0:${ARRAY_SIZE - 1}")
@Param(name = "value", gen = IntGen::class, conf = "0:2")
class AtomicArrayWithCAS2Test : TestBase(
    sequentialSpecification = IntAtomicArraySequential::class,
    scenarios = 1000
) {
    private val array = AtomicArrayWithCAS2(ARRAY_SIZE, 0)

    @Operation(params = ["index"])
    fun get(index: Int) = array.get(index)

    @Operation(params = ["index", "value", "value"])
    fun cas(index: Int, expected: Int, update: Int) = array.cas(index, expected, update)

    @Operation(params = ["index", "value", "value", "index", "value", "value"])
    fun cas2(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int, update2: Int
    ) = array.cas2(index1, expected1, update1, index2, expected2, update2)
}

@Param(name = "index", gen = IntGen::class, conf = "0:${ARRAY_SIZE - 1}")
@Param(name = "value", gen = IntGen::class, conf = "0:2")
class AtomicArrayWithCAS2SingleWriterTest : TestBase(
    sequentialSpecification = IntAtomicArraySequential::class,
    scenarios = 1000
) {
    private val array = AtomicArrayWithCAS2SingleWriter(ARRAY_SIZE, 0)

    @Operation(params = ["index"])
    fun get(index: Int) = array.get(index)

    @Operation(
        params = ["index", "value", "value", "index", "value", "value"],
        nonParallelGroup = "writer")
    fun cas2(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int, update2: Int
    ) = array.cas2(index1, expected1, update1, index2, expected2, update2)

    @Test
    fun checkUnapply() {
        assertEquals(false,
            array.cas2(
                0, 0, 10,
                1, 9, 20))
        assertEquals(0, array.get(0))
        assertEquals(0, array.get(1))
        assertEquals(true,
            array.cas2(
                0, 0, 10,
                1, 0, 20))
        assertEquals(10, array.get(0))
        assertEquals(20, array.get(1))
    }
}

class AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest : TestBase(
    sequentialSpecification = IntAtomicArraySequential::class,
    scenarios = 0
) {
    private val array = AtomicArrayWithSingleWriterCas2AndCasSimplified(ARRAY_SIZE, 0)

    fun get(index: Int): Int = array.get(index)

    fun cas2(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int, update2: Int
    ) = array.cas2(index1, expected1, update1, index2, expected2, update2)

    fun cas(index: Int, expected: Int, update: Int) =
        array.cas(index, expected, update)

    override fun Options<*, *>.customConfiguration() {
        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 0, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 1, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 1, 2)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 2, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 1, 0, 1, 2, 0, 1)
                }
                thread {
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 0, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 0, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 1, 2)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 1, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 1, 2)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 1, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 2, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 2, 3)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 2, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas2, 1, 1, 2, 2, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                }
                thread {
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 1, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 2, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 1, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::cas, 0, 0, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 0)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 1)
                    actor(AtomicArrayWithSingleWriterCas2AndCasSimplifiedTest::get, 2)
                }
            }
        }
    }
}

class AtomicArrayWithCAS2SimplifiedTest : TestBase(
    sequentialSpecification = IntAtomicArraySequential::class,
    scenarios = 0
) {
    private val array = AtomicArrayWithCAS2Simplified(ARRAY_SIZE, 0)

    fun get(index: Int): Int = array.get(index)

    fun cas2(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int, update2: Int
    ) = array.cas2(index1, expected1, update1, index2, expected2, update2)

    override fun Options<*, *>.customConfiguration() {
        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 1, 0, 1, 0, 0, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 1, 2)
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 1, 2, 1, 0, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 1, 0, 1, 2, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 2, 0, 1, 0, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 2, 0, 1, 0, 1, 2)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 1, 0, 1, 2, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 2, 0, 1, 0, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 2, 0, 1, 0, 1, 2)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 1, 2, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 1, 2, 1, 1, 2)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 1, 1, 2, 2, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 1, 2, 3, 2, 0, 1)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 2)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 2)
                }
            }
        }

        addCustomScenario {
            parallel {
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 2)
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 1, 1, 2, 2, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 2)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 1, 2, 1, 2, 3)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 2)
                }
                thread {
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 1, 1, 2, 2, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 2)
                    actor(AtomicArrayWithCAS2SimplifiedTest::cas2, 0, 0, 1, 1, 0, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 0)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 1)
                    actor(AtomicArrayWithCAS2SimplifiedTest::get, 2)
                }
            }
        }
    }
}

class IntAtomicArraySequential {
    private val array = IntArray(ARRAY_SIZE)

    fun get(index: Int): Int = array[index]

    fun set(index: Int, value: Int) {
        array[index] = value
    }

    fun cas(index: Int, expected: Int, update: Int): Boolean {
        if (array[index] != expected) return false
        array[index] = update
        return true
    }

    fun dcss(
        index1: Int, expected1: Int, update1: Int,
        index2: Int, expected2: Int
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        if (array[index1] != expected1 || array[index2] != expected2) return false
        array[index1] = update1
        return true
    }

    fun cas2(
        index1: Int, expected1: Int, update1: Int, index2: Int, expected2: Int, update2: Int
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        if (array[index1] != expected1 || array[index2] != expected2) return false
        array[index1] = update1
        array[index2] = update2
        return true
    }
}

private const val ARRAY_SIZE = 3