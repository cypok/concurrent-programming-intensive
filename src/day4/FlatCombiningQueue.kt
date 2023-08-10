package day4

import day1.*
import java.util.concurrent.*
import java.util.concurrent.atomic.*

class FlatCombiningQueue<E> : Queue<E> {
    private val queue = ArrayDeque<E>() // sequential queue
    private val combinerLock = AtomicBoolean(false) // unlocked initially
    private val tasksForCombiner = AtomicReferenceArray<Any?>(TASKS_FOR_COMBINER_SIZE)

    override fun enqueue(element: E) {
        performOperation<Unit> { Enqueue(element) }
    }

    override fun dequeue(): E? {
        return performOperation<E> { Dequeue }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <V> performOperation(createTask: () -> Any): V {
        var taskWasInstalledAt = -1

        while (true) {
            if (combinerLock.compareAndSet(false, true)) {
                // We are the Combiner.
                try {
                    // Do our own stuff unless it was installed.
                    var theResult = if (taskWasInstalledAt == -1) doOperation(createTask()) else null

                    // Help others.
                    for (i in 0 until tasksForCombiner.length()) {
                        val task = tasksForCombiner.get(i) ?: continue

                        if (i == taskWasInstalledAt) {
                            // Our task was here, extract it.
                            theResult = if (task is Result<*>) task.value as V else doOperation(task)
                            tasksForCombiner.set(i, null)

                        } else if (task !is Result<*>) {
                            // Perform another operation and save the result.
                            assert(tasksForCombiner.get(i) === task)
                            tasksForCombiner.set(i, Result(doOperation(task)))
                        }
                    }

                    return theResult as V

                } finally {
                    combinerLock.set(false)
                }

            } else if (taskWasInstalledAt == -1) {
                // No luck with the lock, we need help from the Combiner.
                val taskIdx = randomCellIndex()
                if (tasksForCombiner.compareAndSet(taskIdx, null, createTask())) {
                    taskWasInstalledAt = taskIdx
                }

            } else {
                // Check if somebody helped with our installed task.
                val task = tasksForCombiner.get(taskWasInstalledAt)
                if (task is Result<*>) {
                    tasksForCombiner.set(taskWasInstalledAt, null)
                    return task.value as V
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun doOperation(task: Any): Any? {
        return when (task) {
            Dequeue -> queue.removeFirstOrNull()
            is Enqueue<*> -> queue.addLast(task.value as E)
            else -> throw IllegalStateException()
        }
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(tasksForCombiner.length())
}

private const val TASKS_FOR_COMBINER_SIZE = 3 // Do not change this constant!

private object Dequeue

private class Enqueue<E>(
    val value: E
)

private class Result<V>(
    val value: V
)