package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {
            if(this.locks != null && !this.locks.isEmpty()) {
                for(Lock lock : this.locks) {
                    if(lock.transactionNum != except && !LockType.compatible(lockType, lock.lockType)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {
            if(this.locks == null || this.locks.isEmpty()) {
                this.locks = new ArrayList<>();
            }else {
                this.locks.removeIf(curLock -> curLock.transactionNum.equals(lock.transactionNum)
                        && LockType.substitutable(lock.lockType, curLock.lockType));
            }
            this.locks.add(lock);
        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            if(this.locks != null && !this.locks.isEmpty()) {
                this.locks.remove(lock);
                this.waitingQueue.removeIf(lockRequest -> lockRequest.lock.equals(lock));
            }
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            if(this.waitingQueue == null) {
                this.waitingQueue = new ArrayDeque<>();
            }
            if(addFront) {
                this.waitingQueue.addFirst(request);
            }else {
                this.waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {
            if(this.waitingQueue != null) {
                for(LockRequest lockRequest : this.waitingQueue) {
                    // check whether could accept the current lock request
                    for(Lock lock : this.locks) {
                        // if current lock request is not compatible with other transactions held lock
                        if(!LockType.compatible(lock.lockType, lockRequest.lock.lockType)
                            && (lock.transactionNum != lockRequest.transaction.getTransNum())) {
                                return;
                        }
                    }
                    // could grant lock to this lockRequest, firstly remove this request from queue
                    this.waitingQueue.remove(lockRequest);
                    // release this request's released locks if it has
                    if(lockRequest.releasedLocks != null && !lockRequest.releasedLocks.isEmpty()) {
                        this.locks.removeAll(lockRequest.releasedLocks);
                    }
                    // grant lock by adding to current locks
                    this.locks.add(lockRequest.lock);
                    // update outside lock manager state(LockManager.this.transactionLocks)
                    LockManager.this.grantLockByAddToTransactionLocks(lockRequest.transaction, lockRequest.lock);
                    // unblock this transaction
                    lockRequest.transaction.unblock();
                }
            }
        }

        /**
         * Gets the type of lock TRANSACTION has on this resource.
         */
        LockType getTransactionLockType(long transaction) {
            if(this.locks != null && !this.locks.isEmpty()) {
                for(Lock lock : this.locks) {
                    if(lock.transactionNum == transaction) {
                        return lock.lockType;
                    }
                }
            }
            return LockType.NL;
        }

        Lock getTransactionLock(long transaction) {
            if(this.locks != null && !this.locks.isEmpty()) {
                for(Lock lock : this.locks) {
                    if(lock.transactionNum == transaction) {
                        return lock;
                    }
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     *  developer helper functions
     */
    private void grantLockByAddToTransactionLocks(TransactionContext transaction, Lock txnLock) {
        if(this.transactionLocks.containsKey(transaction.getTransNum())) {
            this.transactionLocks.get(transaction.getTransNum()).add(txnLock);
        }else {
            List<Lock> locks = new ArrayList<>();
            locks.add(txnLock);
            this.transactionLocks.put(transaction.getTransNum(), locks);
        }
    }

    private void grantLock(TransactionContext transaction, ResourceName name, Lock txnLock) {
        this.grantLockByAddToTransactionLocks(transaction, txnLock);

        // grant lock by add to resource entry's locks
        ResourceEntry resourceEntry = this.resourceEntries.get(name);
        if(resourceEntry == null) {
            resourceEntry = new ResourceEntry();
            this.resourceEntries.put(name, resourceEntry);
        }
        resourceEntry.grantOrUpdateLock(txnLock);
    }

    private boolean checkShouldBlock(TransactionContext transaction, ResourceEntry resourceEntry,
                                     LockType lockType) {
        if(resourceEntry != null) {
            if(!resourceEntry.checkCompatible(lockType, transaction.getTransNum())) {
                return true;
            }
            return resourceEntry.waitingQueue != null && !resourceEntry.waitingQueue.isEmpty();
        }
        return false;
    }

    private void releaseLockForTransactionAndResourceName(TransactionContext transaction, ResourceName name) {
        ResourceEntry resourceEntry = this.resourceEntries.get(name);
        if(resourceEntry != null) {
            // update this resource state
            Lock txnLock = resourceEntry.getTransactionLock(transaction.getTransNum());
            resourceEntry.releaseLock(txnLock);
            resourceEntry.processQueue();
            // update LockManager state
            if(resourceEntry.locks.isEmpty() && resourceEntry.waitingQueue.isEmpty()) {
                this.resourceEntries.remove(name);
            }
            this.transactionLocks.get(transaction.getTransNum()).remove(txnLock);
            if(this.transactionLocks.get(transaction.getTransNum()).isEmpty()) {
                this.transactionLocks.remove(transaction.getTransNum());
            }
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // if no locks need to be released, just convert to call this.acquire()
        if(releaseLocks == null || releaseLocks.isEmpty()) {
            this.acquire(transaction, name, lockType);
            return;
        }

        // check if there is a duplicate lock
        LockType currLockType = this.getLockType(transaction, name);
        if(currLockType == lockType || (currLockType != LockType.NL && !releaseLocks.contains(name))) {
            throw new DuplicateLockRequestException(String.format("transaction %d already holds a lock on resource %s", transaction.getTransNum(), name));
        }

        List<Lock> locks = this.transactionLocks.get(transaction.getTransNum());
        // if this transaction doesn't hold locks that he is going to release, throw an NoLockHeldException
        if((locks == null || locks.isEmpty()) && !releaseLocks.isEmpty()) {
            throw new NoLockHeldException(String.format("transaction %d does not hold any lock but it is going to release lock",
                    transaction.getTransNum()));
        }
        Set<ResourceName> releaseLocksVerify = new HashSet<>(releaseLocks);
        if(locks != null && !locks.isEmpty()) {
            // if held locks doesn't include release locks
            for(Lock heldLock : locks) {
                for(ResourceName resourceName : releaseLocks) {
                    if(heldLock.name.equals(resourceName)) {
                        releaseLocksVerify.remove(resourceName);
                    }
                }
            }
            if(!releaseLocksVerify.isEmpty()) {
                throw new NoLockHeldException(String.format("transaction %d does not hold any lock but it is going to release lock",
                        transaction.getTransNum()));
            }
        }

        // step here prove it is safe to do this lockRequest
        boolean shouldBlock;
        synchronized (this) {
            // check if there are not compatible locks on this resource
            ResourceEntry resourceEntry = this.resourceEntries.get(name);
            shouldBlock = this.checkShouldBlock(transaction, resourceEntry, lockType);

            Lock txnLock = new Lock(name, lockType, transaction.getTransNum());
            if(shouldBlock) {
                for(ResourceName resourceName : releaseLocks) {
                    // get txn held lock on resource
                    ResourceEntry currResourceEntry = this.resourceEntries.get(resourceName);
                    Lock currTxnLock = currResourceEntry.getTransactionLock(transaction.getTransNum());
                    // generate a lock request for every waiting to released locks
                    LockRequest lockRequest;
                    if(name.equals(resourceName)) {
                        lockRequest = new LockRequest(transaction, txnLock, Collections.singletonList(currTxnLock));
                    }else {
                        lockRequest = new LockRequest(transaction, null, Collections.singletonList(currTxnLock));
                    }
                    currResourceEntry.addToQueue(lockRequest, true);
                }
                transaction.prepareBlock();
            }else {
                // release locks
                for(ResourceName resourceName : releaseLocks) {
                    this.releaseLockForTransactionAndResourceName(transaction, resourceName);
                }
                this.grantLock(transaction, name, txnLock);
            }
        }
        if(shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // check if there are duplicate lock
        LockType currLockType = this.getLockType(transaction, name);
        if(currLockType != LockType.NL) {
            throw new DuplicateLockRequestException(String.format("transaction %d already holds a lock on resource %s", transaction.getTransNum(), name));
        }
        boolean shouldBlock;
        synchronized (this) {
            // check if there are not compatible locks on this resource
            // or if there are lock request in this resource's queue
            ResourceEntry resourceEntry = this.resourceEntries.get(name);
            shouldBlock = this.checkShouldBlock(transaction, resourceEntry, lockType);

            Lock txnLock = new Lock(name, lockType, transaction.getTransNum());
            if(shouldBlock) {
                // if should block this transaction, new a LockRequest
                // and put it in this resource queue, block this transaction
                LockRequest lockRequest = new LockRequest(transaction, txnLock);
                resourceEntry.addToQueue(lockRequest, false);
                transaction.prepareBlock();
            }else {
                grantLock(transaction, name, txnLock);
            }
        }
        if(shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // check if this transaction holds locks on this resource
        LockType currLockType = this.getLockType(transaction, name);
        if(currLockType == LockType.NL) {
            throw new NoLockHeldException(String.format("transaction %d does not hold a lock on resource %s", transaction.getTransNum(), name));
        }
        // release lock and update LockManager state, this resource needs to process its queue
        synchronized (this) {
            releaseLockForTransactionAndResourceName(transaction, name);
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // error checking
        LockType currLockType = this.getLockType(transaction, name);
        if(currLockType == newLockType) {
            throw new DuplicateLockRequestException(String.format("transaction %d already holds a lock on resource %s", transaction.getTransNum(), name));
        }else if(currLockType == LockType.NL) {
            throw new NoLockHeldException(String.format("transaction %d does not hold a lock on resource %s", transaction.getTransNum(), name));
        }else if(!LockType.substitutable(newLockType, currLockType)) {
            throw new InvalidLockException(String.format("transaction %d cannot promote his held lock", transaction.getTransNum()));
        }

        boolean shouldBlock ;
        synchronized (this) {
            // check if there are not compatible locks on this resource
            ResourceEntry resourceEntry = this.resourceEntries.get(name);
            shouldBlock = checkShouldBlock(transaction, resourceEntry, newLockType);

            Lock txnLock = new Lock(name, newLockType, transaction.getTransNum());
            if(shouldBlock) {
                LockRequest lockRequest = new LockRequest(transaction, txnLock, Collections.singletonList(new Lock(name, currLockType, transaction.getTransNum())));
                resourceEntry.addToQueue(lockRequest, true);
                transaction.prepareBlock();
            }else{
                if(resourceEntry != null) {
                    resourceEntry.grantOrUpdateLock(txnLock);
                    // update this.transactions
                    this.transactionLocks.get(transaction.getTransNum()).remove(new Lock(name, currLockType, transaction.getTransNum()));
                    this.transactionLocks.get(transaction.getTransNum()).add(txnLock);
                }
            }
        }
        if(shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        ResourceEntry resourceEntry = this.resourceEntries.get(name);
        if(resourceEntry != null) {
            return resourceEntry.getTransactionLockType(transaction.getTransNum());
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
