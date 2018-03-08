/*
 * $URL: StorageFactoryTest.java $ 
 * $Rev: 3582 $ 
 * $Date: 2007-11-25 14:29:06 +0300 (Вс., 25 нояб. 2007) $
 *
 * Copyright 2005 Netup, Inc. All rights reserved.
 * URL:    http://www.netup.biz
 * e-mail: info@netup.biz
 */

package org.garret.perst;

import junit.framework.*;

/**
 * These tests verifies an functionality of the <code>StorageFactory</code> class.
 */
public class StorageFactoryTest extends TestCase {

    public StorageFactoryTest(String testName) {
        super(testName);
    }

    public static junit.framework.Test suite()
    {
        junit.framework.TestSuite suite =
                new junit.framework.TestSuite(StorageFactoryTest.class);

        return suite;
    }

    protected void setUp() throws java.lang.Exception {
    }

    protected void tearDown() throws java.lang.Exception {
    }

    /**
     * Verifies that a <CODE>createStorage()</CODE> method invocation returns a
     * not-<CODE>null</CODE> object.
     */
    public void testCreateStorage() {
        Storage storage = StorageFactory.getInstance().createStorage();
        assertNotNull(storage);
    }

    /**
     * Verifies that a <CODE>createStorage()</CODE> method invocation returns different values
     * in sequential  calls.
     */
    public void testCreateTwice() {
        Storage storage0 = StorageFactory.getInstance().createStorage();
        Storage storage1 = StorageFactory.getInstance().createStorage();
        assertNotNull(storage0);
        assertNotNull(storage1);
        assertNotSame(storage0, storage1);
    }

}
