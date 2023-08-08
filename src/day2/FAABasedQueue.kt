package day2

import day1.*
import java.util.concurrent.atomic.*

// TODO: Copy the code from `FAABasedQueueSimplified`
// TODO: and implement the infinite array on a linked list
// TODO: of fixed-size `Segment`s.
class FAABasedQueue<E> : Queue<E> {
    private val head: AtomicReference<Segment>
    private val tail: AtomicReference<Segment>

    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)

    override fun enqueue(element: E) {
        TODO("Implement me!")
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        TODO("Implement me!")
    }

    override fun validate() {
        fun checkIdxAndPointerAreNotFar(pointerName: String, pointer: AtomicReference<Segment>,
                                        idxName: String, idx: AtomicLong): Segment? {
            val expectedSegId = idx.get() / SEGMENT_SIZE
            val seg = pointer.get()
            return when (expectedSegId) {
                seg.id -> seg

                // there are tricky cases on boundaries, allow them
                seg.id + 1 -> seg.next.get()

                else -> throw IllegalStateException(
                    "`${pointerName}` must point to segment with id ${expectedSegId - 1} or $expectedSegId " +
                            "but points to segment with id ${seg.id} and `${idxName} = ${idx.get()}`"
                )
            }
        }

        val deqSeg = checkIdxAndPointerAreNotFar("head", head, "deqIdx", deqIdx)
        val enqSeg = checkIdxAndPointerAreNotFar("tail", tail, "enqIdx", enqIdx)

        val deqInfo = Pair(deqSeg, deqIdx.get() % SEGMENT_SIZE)
        val enqInfo = Pair(enqSeg, enqIdx.get() % SEGMENT_SIZE)
        val (lo, hi) =
            (if (deqIdx.get() <= enqIdx.get()) Pair(deqInfo, enqInfo) else Pair(enqInfo, deqInfo))
        val (loSeg, loCellIdx) = lo
        val (hiSeg, hiCellIdx) = hi

        for (i in 0 until loCellIdx.toInt()) {
            val loSeg = loSeg!!
            check(loSeg.cells[i] == null || loSeg.cells[i] == POISONED) {
                "`segment#${loSeg.id}.cells[$i]` must be `null` or `POISONED` with `deqIdx = ${deqIdx.get()}` and `enqIdx = ${enqIdx.get()}`"
            }
        }

        for (i in hiCellIdx.toInt() until SEGMENT_SIZE) {
            if (hiSeg == null) break
            check(hiSeg.cells[i] == null || hiSeg.cells[i] == POISONED) {
                "`segment#${hiSeg.id}.cells[$i]` must be `null` or `POISONED` with `deqIdx = ${deqIdx.get()}` and `enqIdx = ${enqIdx.get()}`"
            }
        }
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
