package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import javax.swing.text.html.Option;
import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    // recLSN: Oldest update to page since it was last flushed
    // for Redo stage
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    // for Undo stage
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        TransactionTableEntry transactionTableEntry = this.transactionTable.get(transNum);
        CommitTransactionLogRecord commitTransactionLogRecord = new CommitTransactionLogRecord(transNum, transactionTableEntry.lastLSN);
        long currLSN = this.logManager.appendToLog(commitTransactionLogRecord);
        this.logManager.flushToLSN(currLSN);
        // update ATT
        Transaction transaction = transactionTableEntry.transaction;
        transaction.setStatus(Transaction.Status.COMMITTING);
        transactionTableEntry.lastLSN = currLSN;
        this.transactionTable.put(transNum, transactionTableEntry);
        return currLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        TransactionTableEntry transactionTableEntry = this.transactionTable.get(transNum);
        AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(transNum, transactionTableEntry.lastLSN);
        long currLSN = this.logManager.appendToLog(abortTransactionLogRecord);
        // update ATT
        Transaction transaction = transactionTableEntry.transaction;
        transaction.setStatus(Transaction.Status.ABORTING);
        transactionTableEntry.lastLSN = currLSN;
        this.transactionTable.put(transNum, transactionTableEntry);
        return currLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        TransactionTableEntry transactionTableEntry = this.transactionTable.get(transNum);
        Transaction transaction = transactionTableEntry.transaction;
        long currLSN = transactionTableEntry.lastLSN;
        // ??????transaction is aborting??? rollback its changes
        if(transaction.getStatus() == Transaction.Status.ABORTING) {
            // roll back changes
            Optional<Long> lastLSN = Optional.of(transactionTableEntry.lastLSN);
            currLSN = lastLSN.get();
            while (lastLSN.isPresent() && lastLSN.get() > 0) {
                LogRecord logRecord = this.logManager.fetchLogRecord(lastLSN.get());
                if(logRecord.isUndoable()) {
                    Pair<LogRecord, Boolean> clrPair = logRecord.undo(currLSN);
                    currLSN = this.logManager.appendToLog(clrPair.getFirst());
                    if(clrPair.getSecond()) {
                        this.logManager.flushToLSN(currLSN);
                    }
                    lastLSN = clrPair.getFirst().getUndoNextLSN();

                    if(clrPair.getFirst().getPageNum().isPresent()) {
                        this.updateDPTWhenRollback(clrPair.getFirst(), currLSN);
                    }

                    clrPair.getFirst().redo(this.diskSpaceManager, this.bufferManager);
                }else{
                    if(logRecord.type == LogType.UNDO_UPDATE_PAGE || logRecord.type == LogType.UNDO_ALLOC_PAGE
                            || logRecord.type == LogType.UNDO_FREE_PAGE || logRecord.type == LogType.UNDO_ALLOC_PART
                            || logRecord.type == LogType.UNDO_FREE_PART) {
                        lastLSN = logRecord.getUndoNextLSN();
                    }else{
                        lastLSN = logRecord.getPrevLSN();
                    }
                }
            }
        }
        // remove this transaction from ATT
        this.transactionTable.remove(transNum);
        // update transaction status
        transaction.setStatus(Transaction.Status.COMPLETE);
        // append a log record
        EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(transNum, currLSN);
        long resLSN = this.logManager.appendToLog(endTransactionLogRecord);
        this.logManager.print();
        return resLSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        // 1. ?????? page+offset ??????byte[]?????????before??????content(no need now)
        // 2. ??????before???length??????????????????divide into 2 records(an undo-only record followed by a redo-only record)
        // 3. ??????1???2???UpdatePageLogRecord
        // 4. update ATT & DPT
        TransactionTableEntry transactionTableEntry = this.transactionTable.get(transNum);
        long prevLSN = transactionTableEntry == null ?  -1 : transactionTableEntry.lastLSN;
        if(before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            UpdatePageLogRecord updatePageLogRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
            long currLSN = this.logManager.appendToLog(updatePageLogRecord);
            // update ATT
            if(transactionTableEntry != null) {
                transactionTableEntry.lastLSN = currLSN;
                transactionTableEntry.touchedPages.add(pageNum);
            }
            this.transactionTable.put(transNum, transactionTableEntry);
            // update DPT
            if(!this.dirtyPageTable.containsKey(pageNum)) {
                this.dirtyPageTable.put(pageNum, currLSN);
            }else{
                long recLSN = this.dirtyPageTable.get(pageNum);
                if(recLSN > currLSN) {
                    this.dirtyPageTable.put(pageNum, currLSN);
                }
            }
            return currLSN;
        }else{
            UpdatePageLogRecord undoOnlyLogRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, new byte[0]);
            long undoOnlyLSN = this.logManager.appendToLog(undoOnlyLogRecord);
            UpdatePageLogRecord redoOnlyLogRecord = new UpdatePageLogRecord(transNum, pageNum, undoOnlyLSN, pageOffset, new byte[0], after);
            long redoOnlyLSN = this.logManager.appendToLog(redoOnlyLogRecord);
            // update ATT
            if(transactionTableEntry != null) {
                transactionTableEntry.lastLSN = redoOnlyLSN;
                transactionTableEntry.touchedPages.add(pageNum);
            }
            // update DPT
            if(!this.dirtyPageTable.containsKey(pageNum)) {
                this.dirtyPageTable.put(pageNum, undoOnlyLSN);
            }else{
                long recLSN = this.dirtyPageTable.get(pageNum);
                if(recLSN > undoOnlyLSN) {
                    this.dirtyPageTable.put(pageNum, undoOnlyLSN);
                }
            }
            return redoOnlyLSN;
        }
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);
        Optional<Long> lastLSN = Optional.of(transactionEntry.lastLSN);
        long currLSN = lastLSN.get();
        while (lastLSN.isPresent() && lastLSN.get() > LSN) {
            LogRecord logRecord = this.logManager.fetchLogRecord(lastLSN.get());
            if(logRecord.isUndoable()) {
                Pair<LogRecord, Boolean> clrPair = logRecord.undo(currLSN);
                currLSN = this.logManager.appendToLog(clrPair.getFirst());
                if(clrPair.getSecond()) {
                    this.logManager.flushToLSN(currLSN);
                }
                lastLSN = clrPair.getFirst().getUndoNextLSN();

                if(clrPair.getFirst().getPageNum().isPresent()) {
                    this.updateDPTWhenRollback(clrPair.getFirst(), currLSN);
                }

                clrPair.getFirst().redo(this.diskSpaceManager, this.bufferManager);
            }else{
                if(logRecord.type == LogType.UNDO_UPDATE_PAGE || logRecord.type == LogType.UNDO_ALLOC_PAGE
                    || logRecord.type == LogType.UNDO_FREE_PAGE || logRecord.type == LogType.UNDO_ALLOC_PART
                    || logRecord.type == LogType.UNDO_FREE_PART) {
                        lastLSN = logRecord.getUndoNextLSN();
                }else{
                    lastLSN = logRecord.getPrevLSN();
                }
            }
        }
        // for debug
        // this.logManager.print();
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // iterate DPT
        for(Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size()+1, txnTable.size(), touchedPages.size(), numTouchedPages);
            if(!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
            dpt.put(entry.getKey(), entry.getValue());
        }
        // iterate status/lastLSN inside ATT
        for(Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry transactionTableEntry = entry.getValue();
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size()+1, touchedPages.size(), numTouchedPages);
            if(!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }
            txnTable.put(transNum, new Pair<>(transactionTableEntry.transaction.getStatus(), transactionTableEntry.lastLSN));
        }
        // iterate touch pages inside ATT
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        restartAnalysis();
        restartRedo();
        for(Map.Entry<Long,Long> entry : this.dirtyPageTable.entrySet()) {
            long pageNum = entry.getKey();
            Page dirtyPage = this.bufferManager.fetchPage(this.dbContext, pageNum, false);
            byte[] before = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
            dirtyPage.getBuffer().get(before);
            byte[] after = new byte[DiskSpaceManager.PAGE_SIZE];
            this.diskSpaceManager.readPage(pageNum, after);
            byte[] trueAfter = Arrays.copyOfRange(after, BufferManager.RESERVED_SPACE, after.length);
            if(Arrays.equals(before, trueAfter)) {
                this.dirtyPageTable.remove(pageNum);
            }
        }
        return () -> {
            restartUndo();
            this.checkpoint();
            // for debug
            this.logManager.print();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // for debug
        System.out.println("---------- before restart analysis ----------");
        this.logManager.print();
        Set<Long> presentTransactionNum = new HashSet<>();
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // construct DPT & ATT
        Iterator<LogRecord> iterator = this.logManager.scanFrom(LSN);
        while (iterator.hasNext()) {
            LogRecord logRecord = iterator.next();
            switch (logRecord.type) {
                case ALLOC_PAGE:
                case FREE_PART:
                case FREE_PAGE:
                case ALLOC_PART:
                case UPDATE_PAGE:
                case UNDO_ALLOC_PAGE:
                case UNDO_UPDATE_PAGE:
                case UNDO_FREE_PAGE:
                case UNDO_ALLOC_PART:
                case UNDO_FREE_PART:
                    this.restartAnalysisForTxnOperation(logRecord, presentTransactionNum);
                    break;
                case COMMIT_TRANSACTION:
                case ABORT_TRANSACTION:
                case END_TRANSACTION:
                    this.restartAnalysisForTxnStatusChange(logRecord, presentTransactionNum);
                    break;
                case BEGIN_CHECKPOINT:
                    updateTransactionCounter.accept(logRecord.getMaxTransactionNum().get());
                    break;
                case END_CHECKPOINT:
                    this.restartAnalysisForEndCheckpoint(logRecord, presentTransactionNum);
                    break;
            }
        }
        // Ending Transactions
        for(Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()) {
            long transactionNum = entry.getKey();
            TransactionTableEntry transactionTableEntry = entry.getValue();
            if(transactionTableEntry.transaction.getStatus() == Transaction.Status.COMMITTING) {
                transactionTableEntry.transaction.cleanup();
                transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                EndTransactionLogRecord endTransactionLogRecord = new EndTransactionLogRecord(transactionNum, transactionTableEntry.lastLSN);
                this.logManager.appendToLog(endTransactionLogRecord);
                this.transactionTable.remove(transactionNum);
            }else if(transactionTableEntry.transaction.getStatus() == Transaction.Status.RUNNING) {
                transactionTableEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                AbortTransactionLogRecord abortTransactionLogRecord = new AbortTransactionLogRecord(transactionNum, transactionTableEntry.lastLSN);
                transactionTableEntry.lastLSN = this.logManager.appendToLog(abortTransactionLogRecord);
            }
        }
        // for debug
        System.out.println("---------- after restart analysis ----------");
        this.logManager.print();
    }

    private void restartAnalysisForTxnOperation(LogRecord logRecord, Set<Long> presentTransactionNum) {
        long transactionNum = logRecord.getTransNum().get();
        presentTransactionNum.add(transactionNum);
        TransactionTableEntry transactionTableEntry;
        // get transaction table entry
        if(!this.transactionTable.containsKey(transactionNum)) {
            Transaction transaction = newTransaction.apply(transactionNum);
            transactionTableEntry = new TransactionTableEntry(transaction);
        }else{
            transactionTableEntry = this.transactionTable.get(transactionNum);
        }
        // update ATT
        if(transactionTableEntry.lastLSN < logRecord.LSN) {
            transactionTableEntry.lastLSN = logRecord.LSN;
        }
        // if page related
        if(logRecord.type != LogType.ALLOC_PART && logRecord.type != LogType.FREE_PART
            && logRecord.type != LogType.UNDO_ALLOC_PART && logRecord.type != LogType.UNDO_FREE_PART) {
            long pageNum = logRecord.getPageNum().get();
            if(!transactionTableEntry.touchedPages.contains(pageNum)) {
                transactionTableEntry.touchedPages.add(pageNum);
                // request X lock
                LockContext lockContext = this.getPageLockContext(pageNum);
                this.acquireTransactionLock(transactionTableEntry.transaction, lockContext, LockType.X);
            }
            // update DPT
            if(logRecord.type == LogType.UPDATE_PAGE || logRecord.type == LogType.UNDO_UPDATE_PAGE) {
                // UpdatePage/UndoUpdatePage both may dirty a page in memory, without flushing changes to disk.
                if(!this.dirtyPageTable.containsKey(pageNum) || this.dirtyPageTable.get(pageNum) > logRecord.LSN) {
                    this.dirtyPageTable.put(pageNum, logRecord.LSN);
                }
            }else{
                // AllocPage/FreePage/UndoAllocPage/UndoFreePage all make their changes visible on disk immediately,
                // and can be seen as flushing all changes at the time (including their own) to disk
                this.dirtyPageTable.remove(pageNum);
            }
        }
        this.transactionTable.put(transactionNum, transactionTableEntry);
    }

    private void restartAnalysisForTxnStatusChange(LogRecord logRecord, Set<Long> presentTransactionNum) {
        long transactionNum = logRecord.getTransNum().get();
        presentTransactionNum.add(transactionNum);
        TransactionTableEntry transactionTableEntry = this.transactionTable.get(transactionNum);
        if(transactionTableEntry == null) {
            Transaction transaction = newTransaction.apply(transactionNum);
            transactionTableEntry = new TransactionTableEntry(transaction);
            this.transactionTable.put(transactionNum, transactionTableEntry);
        }
        transactionTableEntry.lastLSN = logRecord.LSN;
        Transaction transaction = transactionTableEntry.transaction;
        if(logRecord.type == LogType.COMMIT_TRANSACTION) {
            transaction.setStatus(Transaction.Status.COMMITTING);
        }else if(logRecord.type == LogType.ABORT_TRANSACTION) {
            transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        }else{
            transaction.cleanup();
            transaction.setStatus(Transaction.Status.COMPLETE);
            this.transactionTable.remove(transactionNum);
        }
    }

    private void restartAnalysisForEndCheckpoint(LogRecord logRecord, Set<Long> presentTransactionNum) {
        if(logRecord.type == LogType.END_CHECKPOINT) {
            // update DPT
            for(Map.Entry<Long, Long> entry : logRecord.getDirtyPageTable().entrySet()) {
                this.dirtyPageTable.put(entry.getKey(), entry.getValue());
            }
            // update ATT
            for(Map.Entry<Long, Pair<Transaction.Status, Long>> entry : logRecord.getTransactionTable().entrySet()) {
                long transactionNum = entry.getKey();
                Transaction.Status status = entry.getValue().getFirst();
                long lastLSN = entry.getValue().getSecond();
                if(this.transactionTable.containsKey(transactionNum)) {
                    TransactionTableEntry transactionTableEntry = this.transactionTable.get(transactionNum);
                    if(transactionTableEntry.lastLSN < lastLSN) {
                        transactionTableEntry.lastLSN = lastLSN;
                        this.transactionTable.put(transactionNum, transactionTableEntry);
                    }
                }else if(!presentTransactionNum.contains(transactionNum)){
                    Transaction transaction = newTransaction.apply(transactionNum);
                    transaction.setStatus(status);
                    TransactionTableEntry transactionTableEntry = new TransactionTableEntry(transaction);
                    transactionTableEntry.lastLSN = lastLSN;
                    this.transactionTable.put(transactionNum, transactionTableEntry);
                }
            }
            // read touched pages
            for(Map.Entry<Long, List<Long>> entry : logRecord.getTransactionTouchedPages().entrySet()) {
                long transactionNum = entry.getKey();
                List<Long> touchedPages = entry.getValue();
                TransactionTableEntry transactionTableEntry = this.transactionTable.get(transactionNum);
                if(transactionTableEntry.transaction.getStatus() != Transaction.Status.COMPLETE) {
                    for(Long pageNum : touchedPages) {
                        if(!transactionTableEntry.touchedPages.contains(pageNum)) {
                            transactionTableEntry.touchedPages.add(pageNum);
                            LockContext lockContext = this.getPageLockContext(pageNum);
                            this.acquireTransactionLock(transactionTableEntry.transaction, lockContext, LockType.X);
                        }
                    }
                }
            }
        }
    }
    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        long startpoint = Long.MAX_VALUE;
        for(Map.Entry<Long, Long> entry : this.dirtyPageTable.entrySet()) {
            if(entry.getValue() < startpoint) {
                startpoint = entry.getValue();
            }
        }
        // from startpoint redo
        Iterator<LogRecord> iterator = this.logManager.scanFrom(startpoint);
        while (iterator.hasNext()) {
            LogRecord logRecord = iterator.next();
            if(logRecord.isRedoable()) {
                if(logRecord.type == LogType.ALLOC_PART || logRecord.type == LogType.FREE_PAGE
                    ||  logRecord.type == LogType.UNDO_ALLOC_PART || logRecord.type == LogType.UNDO_FREE_PART) {
                    logRecord.redo(this.diskSpaceManager,this.bufferManager);
                }else{
                    long pageNum = logRecord.getPageNum().get();
                    long recLSN = this.dirtyPageTable.get(pageNum);
                    if(logRecord.LSN >= recLSN) {
                        Page page = this.bufferManager.fetchPage(this.dbContext, pageNum, false);
                        long pageLSN = page.getPageLSN();
                        if(pageLSN < logRecord.LSN) {
                            logRecord.redo(this.diskSpaceManager, this.bufferManager);
                        }
                    }
                }
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        Queue<Long> queue = new PriorityQueue<>(Collections.reverseOrder());
        for(Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()) {
            queue.add(entry.getValue().lastLSN);
        }
        while (!queue.isEmpty()) {
            long lastLSN = queue.poll();
            LogRecord logRecord = this.logManager.fetchLogRecord(lastLSN);
            if(logRecord.isUndoable()) {
                long transactionNum = logRecord.getTransNum().get();
                TransactionTableEntry transactionTableEntry = this.transactionTable.get(transactionNum);
                long prevLSN = transactionTableEntry.lastLSN;

                Pair<LogRecord, Boolean> clr = logRecord.undo(prevLSN);
                long currLSN = this.logManager.appendToLog(clr.getFirst());
                if (clr.getSecond()) {
                    this.logManager.flushToLSN(currLSN);
                }
                this.updateDPTWhenRollback(clr.getFirst(), currLSN);
                // update ATT
                transactionTableEntry.lastLSN = currLSN;

                clr.getFirst().redo(this.diskSpaceManager, this.bufferManager);
            }
            long undoNextLSN;
            if(logRecord.getUndoNextLSN().isPresent()) {
                undoNextLSN = logRecord.getUndoNextLSN().get();
            }else{
                undoNextLSN = logRecord.getPrevLSN().get();
            }
            if(undoNextLSN == 0) {
                // update DPT
                // this.updateDPTWhenFlush();
                this.end(logRecord.getTransNum().get());
            }else{
                queue.add(undoNextLSN);
            }
        }
        // for debug
        this.logManager.print();
    }

    private void updateDPTWhenRollback(LogRecord logRecord, long currLSN) {
        long pageNum = logRecord.getPageNum().get();
        // update DPT
        if(logRecord.type == LogType.UPDATE_PAGE || logRecord.type == LogType.UNDO_UPDATE_PAGE) {
            // UpdatePage/UndoUpdatePage both may dirty a page in memory, without flushing changes to disk.
            if(!this.dirtyPageTable.containsKey(pageNum) || this.dirtyPageTable.get(pageNum) > logRecord.LSN) {
                this.dirtyPageTable.put(pageNum, currLSN);
            }
        }else{
            // AllocPage/FreePage/UndoAllocPage/UndoFreePage all make their changes visible on disk immediately,
            // and can be seen as flushing all changes at the time (including their own) to disk
            this.dirtyPageTable.remove(pageNum);
        }
    }
    /*private void updateDPTWhenFlush() {
        if(this.transactionTable != null && !this.transactionTable.isEmpty()) {
            for(Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()) {
                TransactionTableEntry transactionTableEntry = entry.getValue();
                if(transactionTableEntry.touchedPages != null && !transactionTableEntry.touchedPages.isEmpty()) {
                    for(Long pageNum : transactionTableEntry.touchedPages) {
                        if(!this.dirtyPageTable.containsKey(pageNum)) {
                            this.dirtyPageTable.put(pageNum, transactionTableEntry.lastLSN);
                        }else{
                            if(this.dirtyPageTable.get(pageNum) < transactionTableEntry.lastLSN) {
                                this.dirtyPageTable.put(pageNum, transactionTableEntry.lastLSN);
                            }
                        }
                    }
                }
            }
        }
    }*/
    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
