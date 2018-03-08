/*
 * $URL: All.java $ 
 * $Rev: 3582 $ 
 * $Date: 2007-11-25 14:29:06 +0300 (Вс., 25 нояб. 2007) $
 *
 * Copyright 2005 Netup, Inc. All rights reserved.
 * URL:    http://www.netup.biz
 * e-mail: info@netup.biz
 */

package org.garret.perst;

import junit.framework.*;
import org.garret.perst.*;

 /**
 * <p>
 *   Collect all the PERST tests into one suite.
 * </p>
 */ 
public class All extends TestCase
{
    
    public All(String testName)
    {
        super(testName);
    }

    public static junit.framework.Test suite()
    {
        junit.framework.TestSuite suite = new junit.framework.TestSuite("All");
        suite.addTest(org.garret.perst.BlobTest.suite());
        suite.addTest(org.garret.perst.DatabaseTest.suite());
        suite.addTest(org.garret.perst.PersistentSetTest.suite());
        suite.addTest(org.garret.perst.QueryTest.suite());
        suite.addTest(org.garret.perst.StorageFactoryTest.suite());
        suite.addTest(org.garret.perst.StorageTest.suite());
        suite.addTest(org.garret.perst.StorageTestThreaded.suite());
        return suite;
    }
    
}
