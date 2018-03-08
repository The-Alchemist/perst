package org.garret.perst.continuous;

import java.io.IOException;

/**
 * Wrapper for IOException
 */
public class IOError extends ContinuousException { 
    IOError(IOException x) { 
        super(x);
    }
}