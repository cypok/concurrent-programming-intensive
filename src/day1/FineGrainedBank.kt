@file:Suppress("DuplicatedCode")

package day1

import day1.Bank.Companion.MAX_AMOUNT
import java.util.concurrent.locks.*

class FineGrainedBank(accountsNumber: Int) : Bank {
    private val accounts: Array<Account> = Array(accountsNumber) { Account() }

    private fun <T> withLocked(accId: Int, action: (Account) -> T): T {
        val account = accounts[accId]
        account.lock.lock()
        try {
            return action(account)
        } finally {
            account.lock.unlock()
        }
    }

    private fun <T> withLocked(accId1: Int, accId2: Int, action: (Account, Account) -> T): T {
        val okOrder = accId1 <= accId2
        return withLocked(if (okOrder) accId1 else accId2) { acc1 ->
            withLocked(if (okOrder) accId2 else accId1) { acc2 ->
                action(if (okOrder) acc1 else acc2, if (okOrder) acc2 else acc1)
            }
        }
    }

    override fun getAmount(id: Int): Long {
        return withLocked(id) { it.amount }
    }

    override fun deposit(id: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        return withLocked(id) { account ->
            check(!(amount > MAX_AMOUNT || account.amount + amount > MAX_AMOUNT)) { "Overflow" }
            account.amount += amount
            account.amount
        }
    }

    override fun withdraw(id: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        return withLocked(id) { account ->
            check(account.amount - amount >= 0) { "Underflow" }
            account.amount -= amount
            account.amount
        }
    }

    override fun transfer(fromId: Int, toId: Int, amount: Long) {
        require(amount > 0) { "Invalid amount: $amount" }
        require(fromId != toId) { "fromId == toId" }
        return withLocked(fromId, toId) { from, to ->
            check(amount <= from.amount) { "Underflow" }
            check(!(amount > MAX_AMOUNT || to.amount + amount > MAX_AMOUNT)) { "Overflow" }
            from.amount -= amount
            to.amount += amount
        }
    }

    /**
     * Private account data structure.
     */
    class Account {
        /**
         * Amount of funds in this account.
         */
        var amount: Long = 0

        val lock = ReentrantLock()
    }
}