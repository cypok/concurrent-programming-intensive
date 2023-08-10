package day4

import AbstractQueueTest
import org.junit.Test
import kotlin.test.assertEquals

class FlatCombiningQueueTest : AbstractQueueTest(FlatCombiningQueue(), checkObstructionFreedom = false) {
    @Test
    fun testNullable() {
        val q = FlatCombiningQueue<String?>()
        q.enqueue("one")
        assertEquals("one", q.dequeue())
        q.enqueue(null)
        assertEquals(null, q.dequeue())
    }
}