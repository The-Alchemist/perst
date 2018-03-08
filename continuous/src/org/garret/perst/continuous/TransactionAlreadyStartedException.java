package org.garret.perst.continuous;

/**
 * Exception thrown when thread attempts to start new transaction without committing or aborting previous one.
 * Transaction should be explicitly finished using CDatabase.commit or CDatabase.rollback method.
 * Each thread should start its own transaction, it is not possible to share the same transaction by more than one threads.
 * It is possible to call beginTransaction several times without commit or rollback only if previous transaction was read-only 
 * - didn't change any object
 */
public class TransactionAlreadyStartedException extends ContinuousException {
    TransactionAlreadyStartedException() { 
        super("Transaction already started exception");
    }
}
