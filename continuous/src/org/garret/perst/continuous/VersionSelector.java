package org.garret.perst.continuous;

import java.util.Date;

/**
 * Class used to specify version selection rule.
 * Three kind of selectors are supported:
 * <ol>
 * <li>Current - select in the version history current version for the current transaction</li>
 * <li>All - select all versions</li>
 * <li>TimeSlice - select versions belonging to the specified time interval, both from and till date can be omitted producing 
 * open interval</li>
 * </ol>
 */
public class VersionSelector 
{
    public enum Kind {        
        Current,
        All,
        TimeSlice
    };
    Kind kind;
    Date from;
    Date till;

    /**
     * Construct version selector of Current or All kind
     */
    public VersionSelector(Kind kind) { 
        this.kind = kind;
    }

    /**
     * Construct TimeSlice version selector. Both from and till data can be omitted producing open interval
     * @param from start of the time interval (inclusive)
     * @param till end of the time interval (inclusive)
     */
    public VersionSelector(Date from, Date till) { 
        this.kind = Kind.TimeSlice;
        this.from = from;
        this.till = till;
    }

    /**
     * Constant for the current version selector
     */
    public static final VersionSelector CURRENT = new VersionSelector(Kind.Current);
}