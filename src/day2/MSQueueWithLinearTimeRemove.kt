@file:Suppress("FoldInitializerAndIfToElvis", "DuplicatedCode")

package day2

import java.util.concurrent.atomic.*

class MSQueueWithLinearTimeRemove<E> : QueueWithRemove<E> {
    private val head: AtomicReference<Node>
    private val tail: AtomicReference<Node>

    init {
        val dummy = Node(null)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.get()
            val newNode = Node(element)
            if (curTail.next.compareAndSet(null, newNode)) {
                tail.compareAndSet(curTail, newNode)
                if (curTail.extractedOrRemoved) {
                    curTail.removePhysically()
                }
                return
            }
            tail.compareAndSet(curTail, curTail.next.get())
        }
    }

    override fun dequeue(): E? {
        while (true) {
            val curHead = head.get()
            val newHead = curHead.next.get() ?: return null
            if (head.compareAndSet(curHead, newHead) && newHead.markExtractedOrRemoved()) {
                newHead.element.let {
                    newHead.element = null
                    return it
                }
            }
        }
    }

    override fun remove(element: E): Boolean {
        // Traverse the linked list, searching the specified
        // element. Try to remove the corresponding node if found.
        // DO NOT CHANGE THIS CODE.
        var node = head.get()
        while (true) {
            val next = node.next.get()
            if (next == null) return false
            node = next
            if (node.element == element && node.remove()) return true
        }
    }

    /**
     * This is an internal function for tests.
     * DO NOT CHANGE THIS CODE.
     */
    override fun validate() {
        check(tail.get().next.get() == null) {
            "tail.next must be null"
        }
        var node = head.get()
        // Traverse the linked list
        while (true) {
            if (node !== head.get() && node !== tail.get()) {
                check(!node.extractedOrRemoved) {
                    "Removed node with element ${node.element} found in the middle of this queue"
                }
            }
            node = node.next.get() ?: break
        }
    }

    private inner class Node(
        var element: E?
    ) {
        val next = AtomicReference<Node?>(null)

        private val _extractedOrRemoved = AtomicBoolean(false)
        val extractedOrRemoved
            get() =
                _extractedOrRemoved.get()

        fun markExtractedOrRemoved(): Boolean =
            _extractedOrRemoved.compareAndSet(false, true)

        /**
         * Removes this node from the queue structure.
         * Returns `true` if this node was successfully
         * removed, or `false` if it has already been
         * removed by [remove] or extracted by [dequeue].
         */
        fun remove(): Boolean {
            if (!markExtractedOrRemoved()) return false
            removePhysically()
            return true
        }

        fun removePhysically() {
            assert(extractedOrRemoved)
            val prev = findPrev() ?: return
            val next = this.next.get()
            if (next != null) {
                // prevent tail removal, it would be done by enqueue
                prev.next.set(next)
                if (next.extractedOrRemoved) {
                    next.removePhysically()
                }
            }
        }

        private fun findPrev(): Node? {
            var prev = head.get()
            while (true) {
                val next = prev.next.get()
                if (next == null) return null
                if (next === this) return prev
                prev = next
            }
        }
    }
}
