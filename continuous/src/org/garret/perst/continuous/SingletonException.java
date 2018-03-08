package org.garret.perst.continuous;

/**
 * Exception thrown by CDatabase.getSingleton method when result set contains more than one element
 */
public class SingletonException extends ContinuousException 
{
    SingletonException() { 
        super("Result set contains more than one element");
    }
}
