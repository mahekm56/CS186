package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if(transaction == null || lockContext == null) {
            return;
        }

        LockType currLockType = lockContext.getEffectiveLockType(transaction);
        // when the lock type already held bu the current txn, do nothing
        if(LockType.substitutable(currLockType, lockType)) {
            return;
        }
        try {
            // when require to release this lock
            if(lockType == LockType.NL) {
                lockContext.release(transaction);
                return;
            }
             /*By escalate to get hold lock:
             1) hold lock itself
             2) to escalate->S, children mustn't have X
             3) to escalate->X, children must have X*/
            currLockType = lockContext.getExplicitLockType(transaction);
            if(currLockType != LockType.NL) {
                boolean hasXDesc = lockContext.hasXDescendants(transaction);
                if((hasXDesc && lockType == LockType.X) || (!hasXDesc && lockType == LockType.S)) {
                    lockContext.escalate(transaction);
                    return;
                }
            }

            updateNotNLLock(lockContext, lockType, currLockType);
        }catch (UnsupportedOperationException ignored){
        }
    }

    private static void updateNotNLLock(LockContext lockContext, LockType lockType, LockType currLockType) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if(lockContext.parentContext() != null) {
            LockContext parentLockContext = lockContext.parentContext();
            LockType parentLockType = parentLockContext.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                LockType expectedParentLockType = LockType.parentLock(lockType);
                updateNotNLLock(parentLockContext, expectedParentLockType, parentLockType);
            }
        }
        if(currLockType == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        }else if(LockType.substitutable(lockType, currLockType)) {
            lockContext.promote(transaction, lockType);
        }else{
            if((lockType == LockType.IX && currLockType == LockType.S) || (lockType == LockType.S && currLockType == LockType.IX)) {
                lockContext.promote(transaction, LockType.SIX);
            }else {
                lockContext.release(transaction);
                lockContext.acquire(transaction, lockType);
            }
        }
    }
}
