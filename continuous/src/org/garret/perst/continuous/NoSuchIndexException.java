package org.garret.perst.continuous;

/**
 * Exception thrown when CDatabase.find method is invoked for the field not declared as indexable
 */
public class NoSuchIndexException extends ContinuousException 
{
    NoSuchIndexException(String field) 
    { 
        super("There is no index for field " + field);
    }
}
