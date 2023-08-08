package day2

import day1.*
import java.util.concurrent.atomic.*
import kotlin.math.*

class FAABasedQueueSimplified<E> : Queue<E> {
    private val infiniteArray = AtomicReferenceArray<Any?>(1024) // conceptually infinite array
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)

    override fun enqueue(element: E) {
        while (true) {
            val curEnqIdx = enqIdx.getAndIncrement()
            val arrIdx = curEnqIdx.toInt()
            if (infiniteArray.compareAndSet(arrIdx, null, element)) {
                break
            }
        }
    }

    override fun dequeue(): E? {
        while (true) {
            if (canAssumeEmptiness()) return null
            val curDeqIdx = deqIdx.getAndIncrement()
            val arrIdx = curDeqIdx.toInt()
            if (!infiniteArray.compareAndSet(arrIdx, null, POISONED)) {
                return infiniteArray.get(arrIdx).let {
                    infiniteArray.set(arrIdx, null)
                    @Suppress("UNCHECKED_CAST")
                    it as E
                }
            }
        }
    }
    
    private fun canAssumeEmptiness(): Boolean {
        while (true) {
            val curEnqIdx = enqIdx.get()
            val curDeqIdx = deqIdx.get()
            if (curEnqIdx != enqIdx.get()) {
                continue
            }
            return curDeqIdx >= curEnqIdx
        }
    }

    override fun validate() {
        for (i in 0 until min(deqIdx.get().toInt(), enqIdx.get().toInt())) {
            check(infiniteArray[i] == null || infiniteArray[i] == POISONED) {
                "`infiniteArray[$i]` must be `null` or `POISONED` with `deqIdx = ${deqIdx.get()}` at the end of the execution"
            }
        }
        for (i in max(deqIdx.get().toInt(), enqIdx.get().toInt()) until infiniteArray.length()) {
            check(infiniteArray[i] == null || infiniteArray[i] == POISONED) {
                "`infiniteArray[$i]` must be `null` or `POISONED` with `enqIdx = ${enqIdx.get()}` at the end of the execution"
            }
        }
    }
}

private val POISONED = Any()
