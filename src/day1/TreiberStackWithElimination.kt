package day1

import java.util.concurrent.*
import java.util.concurrent.atomic.*

open class TreiberStackWithElimination<E> : Stack<E> {
    private val stack = TreiberStack<E>()

    private val eliminationArray = AtomicReferenceArray<Any?>(ELIMINATION_ARRAY_SIZE)

    override fun push(element: E) {
        if (tryPushElimination(element)) return
        stack.push(element)
    }

    protected open fun tryPushElimination(element: E): Boolean {
        val idx = randomCellIndex()

        if (!eliminationArray.compareAndSet(idx, CELL_STATE_EMPTY, element)) {
            return false
        }

        // I'm not really sure how we are supposed to wait here, so let's just fire some CASes.
        repeat(ELIMINATION_WAIT_CYCLES) {
            if (eliminationArray.compareAndSet(idx, CELL_STATE_RETRIEVED, CELL_STATE_EMPTY)) {
                return true
            }
        }

        if (eliminationArray.compareAndSet(idx, element, CELL_STATE_EMPTY)) {
            return false
        }

        // It's guaranteed that state is equal to RETRIEVED and cannot mutate anymore, so just change it.
        eliminationArray[idx] = CELL_STATE_EMPTY
        return true
    }

    override fun pop(): E? = tryPopElimination() ?: stack.pop()

    private fun tryPopElimination(): E? {
        val idx = randomCellIndex()
        val state = eliminationArray[idx]
        if (state == CELL_STATE_EMPTY || state == CELL_STATE_RETRIEVED ||
            !eliminationArray.compareAndSet(idx, state, CELL_STATE_RETRIEVED)) {
            return null
        } else {
            @Suppress("UNCHECKED_CAST")
            return state as E
        }
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(eliminationArray.length())

    companion object {
        private const val ELIMINATION_ARRAY_SIZE = 2 // Do not change!
        private const val ELIMINATION_WAIT_CYCLES = 1 // Do not change!

        // Initially, all cells are in EMPTY state.
        private val CELL_STATE_EMPTY = null

        // `tryPopElimination()` moves the cell state
        // to `RETRIEVED` if the cell contains element.
        private val CELL_STATE_RETRIEVED = Any()
    }
}