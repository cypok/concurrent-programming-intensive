@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day3

import day3.AtomicArrayWithCAS2.Status.*
import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E? {
        val cell = array[index]
        return when (cell) {
            is Descriptor<*> -> (cell as Descriptor<E>).getElementAt(index)
            else -> cell as E?
        }
    }

    fun cas(index: Int, expected: E?, update: E?): Boolean {
        TODO("the cell can store a descriptor")
        return array.compareAndSet(index, expected, update)
    }

    fun cas2(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?, update2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        val descriptor = CAS2Descriptor(
            index1 = index1, expected1 = expected1, update1 = update1,
            index2 = index2, expected2 = expected2, update2 = update2
        )
        descriptor.apply()
        return descriptor.status.get() === SUCCESS
    }

    private fun dcss(index1: Int, expected1: E?, update1: Descriptor<E>, reference2: AtomicReference<Status>, expected2: Status): Boolean {
        val descriptor = DCSSDescriptor(index1, expected1, update1, reference2, expected2)
        descriptor.apply()
        return descriptor.status.get() === SUCCESS
    }

    interface Descriptor<E> {
        fun help()
        fun getElementAt(index: Int): E?
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    inner class CAS2Descriptor(
        private val index1: Int,
        private val expected1: E?,
        private val update1: E?,
        private val index2: Int,
        private val expected2: E?,
        private val update2: E?
    ) : Descriptor<E> {
        val status = AtomicReference(UNDECIDED)

        private val worklist
            get() = listOf(
                Triple(index1, expected1, update1),
                Triple(index2, expected2, update2)
            ).sortedBy { (idx, _, _) -> idx }

        fun apply() {
            val installed = install()
            performLogically(installed)
            performPhysically()
        }

        override fun help() {
            apply()
        }

        private fun install(): Boolean {
            return worklist.all { (idx, exp, _) -> installOne(idx, exp) }
        }

        private fun installOne(idx: Int, exp: E?): Boolean {
            while (true) {
                when (status.get()) {
                    SUCCESS -> return true
                    FAILED -> return false
                    UNDECIDED -> Unit
                }

                when (val cur = array.get(idx)) {
                    this ->
                        // somebody helped us
                        return true

                    is Descriptor<*> ->
                        // let's help somebody else, and try one more time
                        cur.help()

                    exp -> {
                        // it's expected value, try to install
                        if (dcss(idx, exp, this, status, UNDECIDED)) {
                            return true
                        }
                    }

                    else ->
                        // it's unexpected value, CAS2 is failed
                        return false
                }
            }
        }

        private fun performLogically(installed: Boolean) {
            status.compareAndSet(UNDECIDED, if (installed) SUCCESS else FAILED)
        }

        private fun performPhysically() {
            val success = when (status.get()) {
                SUCCESS -> true
                FAILED -> false
                UNDECIDED -> throw IllegalStateException()
            }
            for ((idx, exp, upd) in worklist) {
                array.compareAndSet(idx, this, if (success) upd else exp)
            }
        }

        override fun getElementAt(index: Int): E? {
            val (_, exp, upd) = worklist.find { it.first == index }!!
            return when (status.get()) {
                UNDECIDED, FAILED -> exp
                SUCCESS -> upd
                else -> throw IllegalStateException()
            }
        }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    inner class DCSSDescriptor(
        private val index1: Int,
        private val expected1: E?,
        private val update1: Descriptor<E>,
        private val reference2: AtomicReference<Status>,
        private val expected2: Status,
    ) : Descriptor<E> {
        var status = AtomicReference(UNDECIDED)

        fun apply() {
            val installed = install1() && install2()
            perform(installed)
        }

        override fun help() {
            // install1 was done for sure
            val installed = install2()
            perform(installed)
        }

        private fun perform(installed: Boolean) {
            performLogically(installed)
            performPhysically()
        }

        private fun install1(): Boolean {
            while (true) {
                assert(status.get() == UNDECIDED)

                when (val cur = array.get(index1)) {
                    this ->
                        // nobody could help us before first install
                        throw IllegalStateException()

                    is Descriptor<*> ->
                        // let's help somebody else, and try one more time
                        cur.help()

                    expected1 ->
                        // it's expected value, try to install
                        if (array.compareAndSet(index1, expected1, this)) {
                            return true
                        }

                    else ->
                        // it's unexpected value, DCSS is failed
                        return false
                }
            }
        }

        private fun install2(): Boolean {
            when (status.get()) {
                SUCCESS -> return true
                FAILED -> return false
                UNDECIDED -> Unit
            }

            return reference2.get() == expected2
        }

        private fun performLogically(installed: Boolean) {
            status.compareAndSet(UNDECIDED, if (installed) SUCCESS else FAILED)
        }

        private fun performPhysically() {
            val success = when (status.get()) {
                SUCCESS -> true
                FAILED -> false
                UNDECIDED -> throw IllegalStateException()
            }
            array.compareAndSet(index1, this, if (success) update1 else expected1)
        }

        override fun getElementAt(index: Int): E? {
            assert(index == index1)
            return when (status.get()) {
                UNDECIDED, FAILED -> expected1
                SUCCESS -> update1.getElementAt(index)
                else -> throw IllegalStateException()
            }
        }
    }

    enum class Status {
        UNDECIDED, SUCCESS, FAILED
    }
}