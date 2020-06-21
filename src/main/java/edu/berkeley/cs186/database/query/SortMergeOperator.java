package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.EmptyBacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;

        private SortMergeIterator() {
            super();

            try {
                // sort left and right and get their iterator
                SortOperator sortedLeft = new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator());
                String sortedLeftTableName = sortedLeft.sort();
                this.leftIterator = getRecordIterator(sortedLeftTableName);
                this.leftRecord = this.leftIterator.next();

                SortOperator sortedRight = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());
                String sortedRightTableName = sortedRight.sort();
                this.rightIterator = getRecordIterator(sortedRightTableName);
                this.rightIterator.markNext();

                fetchNextRecord();
            }catch (NoSuchElementException | UnsupportedOperationException e) {
                this.nextRecord = null;
            }
        }

        public void fetchNextRecord() {
            if(this.leftIterator == null) {
                throw new NoSuchElementException("All Done!");
            }
            this.nextRecord = null;
            do{
                if(this.rightIterator.hasNext()) {
                    DataBox leftValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    Record rightRecord = this.rightIterator.next();
                    DataBox rightValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                    while (leftValue.compareTo(rightValue) > 0 ) {
                        if(!this.rightIterator.hasNext()) {
                            throw new NoSuchElementException("All Done!");
                        }
                        rightRecord = this.rightIterator.next();
                        rightValue = rightRecord.getValues().get(getRightColumnIndex());
                    }
                    if(leftValue.equals(rightValue)) {
                        this.nextRecord = joinRecords(this.leftRecord, rightRecord);
                    }else {
                        if(!this.leftIterator.hasNext()) {
                            throw new NoSuchElementException("All Done!");
                        }
                        this.leftRecord = this.leftIterator.next();
                        resetRightIterator();
                    }
                }else {
                    if(!this.leftIterator.hasNext()){
                        throw new NoSuchElementException("All Done!");
                    }
                    this.leftRecord = this.leftIterator.next();
                    resetRightIterator();
                }
            }while (!hasNext());

            /*do{
                DataBox leftValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                DataBox rightValue = this.rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());
                while (leftValue.compareTo(rightValue) > 0 ) {
                    if(!this.rightIterator.hasNext()) {
                        throw new NoSuchElementException("All Done!");
                    }
                    this.rightRecord = this.rightIterator.next();
                    rightValue = this.rightRecord.getValues().get(getRightColumnIndex());
                }
                if(leftValue.equals(rightValue)) {
                    this.nextRecord = joinRecords(this.leftRecord, this.rightRecord);
                    if(this.rightIterator.hasNext()) {
                        this.rightRecord = this.rightIterator.next();
                    }else {
                        if(!this.leftIterator.hasNext()){
                            throw new NoSuchElementException("All Done!");
                        }
                        this.leftRecord = this.leftIterator.next();
                        resetRightIterator();
                    }
                }else {
                    if(!this.leftIterator.hasNext()) {
                        throw new NoSuchElementException("All Done!");
                    }
                    this.leftRecord = this.leftIterator.next();
                    resetRightIterator();
                }
            }while (!hasNext());*/
        }

        private void resetRightIterator() {
            this.rightIterator.reset();
            assert (this.rightIterator.hasNext());
        }

        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;

            try {
                fetchNextRecord();
            }catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
