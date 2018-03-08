package org.garret.perst.continuous;

/**
 * Exception thrown when thread attempts to modify the database 
 * outside transaction body (transaction was not previously started by CDatabase.beginTransaction).
 * Each thread should start its own transaction, it is not possible to share the same transaction by more than one threads.
 */
public class TransactionNotStartedException extends ContinuousException {
    TransactionNotStartedException() { 
        super("Transaction not started exception");
    }
}
