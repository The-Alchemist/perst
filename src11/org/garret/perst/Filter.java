package org.garret.perst;

/**
 * Interface used to fileter records in result set (ResultSet class)
 */
public interface Filter { 
    public boolean fit(Object obj);
}
        