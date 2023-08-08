package day2

import AbstractQueueTest
import IntQueueSequential
import TestBase
import org.jetbrains.kotlinx.lincheck.annotations.*
import org.jetbrains.kotlinx.lincheck.paramgen.*
import org.junit.Test

class FAABasedQueueSimplifiedTest : AbstractQueueTest(FAABasedQueueSimplified())
class FAABasedQueueTest : AbstractQueueTest(FAABasedQueue())

class MSQueueWithOnlyLogicalRemoveTest : AbstractQueueWithRemoveTest(MSQueueWithOnlyLogicalRemove())
class MSQueueWithLinearTimeRemoveTest : AbstractQueueWithRemoveTest(MSQueueWithLinearTimeRemove())
class MSQueueWithConstantTimeRemoveTest : AbstractQueueWithRemoveTest(MSQueueWithConstantTimeRemove())

abstract class AbstractQueueWithRemoveTest(
    private val queue: QueueWithRemove<Int>,
    checkObstructionFreedom: Boolean = true,
) : AbstractQueueTest(
    queue = queue,
    checkObstructionFreedom = checkObstructionFreedom,
    threads = 3,
    actorsBefore = 4
) {
    @Operation
    fun remove(@Param(name = "element") element: Int) = queue.remove(element)

    @Test
    fun testPhysicalRemovalInTheBeginning() {
        listOf(10, 20, 30, 40, 50).forEach(::enqueue)
        check(remove(10))
        validate()
    }

    @Test
    fun testPhysicalRemovalInTheMiddle() {
        listOf(10, 20, 30, 40, 50).forEach(::enqueue)
        check(remove(30))
        validate()
    }

    @Test
    fun testPhysicalRemovalInTheEnd() {
        listOf(10, 20, 30, 40, 50).forEach(::enqueue)
        check(remove(50))
        validate()
        enqueue(60)
        validate()
    }
}

@Param(name = "element", gen = IntGen::class, conf = "0:3")
class MSQueueWithLinearTimeNonParallelRemoveTest: TestBase(
    sequentialSpecification = IntQueueSequential::class,
    checkObstructionFreedom = true,
    threads = 2,
    actorsBefore = 5
) {
    private val queue = MSQueueWithLinearTimeNonParallelRemove<Int>()

    @Operation(nonParallelGroup = "removeAndEnqueue")
    fun enqueue(@Param(name = "element") element: Int) = queue.enqueue(element)

    @Operation
    fun dequeue() = queue.dequeue()

    @Operation(nonParallelGroup = "removeAndEnqueue")
    fun remove(@Param(name = "element") element: Int) = queue.remove(element)

    @Validate
    fun validate() = queue.validate()

    @Test
    fun testPhysicalRemovalInTheBeginning() {
        listOf(10, 20, 30, 40, 50).forEach(::enqueue)
        check(remove(10))
        validate()
    }

    @Test
    fun testPhysicalRemovalInTheMiddle() {
        listOf(10, 20, 30, 40, 50).forEach(::enqueue)
        check(remove(30))
        validate()
    }

    @Test
    fun testPhysicalRemovalInTheEnd() {
        listOf(10, 20, 30, 40, 50).forEach(::enqueue)
        check(remove(50))
        validate()
        enqueue(60)
        validate()
    }

}


