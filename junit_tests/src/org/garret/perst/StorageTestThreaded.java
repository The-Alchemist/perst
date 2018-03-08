/*
 * $URL: StorageTestThreaded.java $ 
 * $Rev: 3582 $ 
 * $Date: 2007-11-25 14:29:06 +0300 (Вс., 25 нояб. 2007) $
 *
 * Copyright 2005 Netup, Inc. All rights reserved.
 * URL:    http://www.netup.biz
 * e-mail: info@netup.biz
 */

package org.garret.perst;

import static org.garret.perst.Storage.INFINITE_PAGE_POOL;
import junit.framework.*;

/**
 * These tests verifies an implementation of the <code>Storage</code> interface. <br />
 * he implementation is created by the following way : 
 * <pre>
 *   storage = org.garret.perst.StorageFactory.getInstance().createStorage()
 * </pre>
 * The tested features:
 * <ul>
 * <li>Synchronisation in multithreaded applications.</li>
 * </ul>
 * <p>
 * In test are used simple <CODE>Persistent</CODE> class <CODE>Root</CODE>:
 * <pre>
 *   class Root extends Persistent {
 *       int i;
 *   }
 * </pre>
 */
public class StorageTestThreaded extends TestCase {

    Storage storage;

    public StorageTestThreaded(String testName) {
        super(testName);
    }
    
    public static junit.framework.Test suite()
    {
        junit.framework.TestSuite suite =
                new junit.framework.TestSuite(StorageTestThreaded.class);
        
        return suite;
    }

    protected void setUp() throws java.lang.Exception {
        storage = StorageFactory.getInstance().createStorage();
    }

    protected void tearDown() throws java.lang.Exception {
        if (storage.isOpened())
            storage.close();
    }

    /**
     * <B>Goal:</B> To prove correct synchronisation in <code>EXCLUSIVE_TRANSACTION</code>.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>Two threads started: <br />
     *   Each thread  accessed storage in <code>EXCLUSIVE_TRANSACTION</code>.
     * </li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>Storage provided correct synchronisation.</li>
     * </ul>
     */
    public void testThreadExclusive() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        storage.setRoot(new Root());
        storage.commit();
        new TestThreadExclusive(storage);
        new TestThreadExclusive(storage);

        while(TestThread.cnt>0){
            try {
            Thread.sleep(100);
            }catch(InterruptedException ex){
            }
        }
        if(TestThread.failed!=""){
            fail(TestThread.failed);
        }
    }

    /**
     * <B>Goal:</B> To prove correct synchronisation in <code>SERIALIZABLE_TRANSACTION</code>.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>Two threads started: <br />
     * Each thread  accessed storage in <code>SERIALIZABLE_TRANSACTION</code>.
     * </li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>Storage provided correct synchronisation.</li>
     * </ul>
     */
    public void testThreadSerializable() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        storage.setRoot(new Root());
        storage.commit();
        new TestThreadSerializable(storage);
        new TestThreadSerializable(storage);

        while (TestThread.cnt > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
        }
        if (TestThread.failed != "") {
            fail(TestThread.failed);
        }
    }

    /**
     * Internal class.
     */
    private static class Root extends PersistentResource {
        int i;

        public Root(int i) {
            this.i = i;
        }

        public Root() {
        }
        public String toString(){
            return "Root{"+i+"}";
        }
    }

    private static class TestThread extends Thread{
        static Integer lock = new Integer(5);
        static int max_id = 0;
        static int cnt = 0;
        Storage storage;
        static String failed = "";
        int myID = ++max_id;
        TestThread(Storage storage){
            cnt++;
            this.storage = storage;
            this.start();
        }
        protected void print(String s){
            //System.out.println("Thread "+myID+": "+s+".");
        }

    }

    private static class TestThreadExclusive extends TestThread {
        TestThreadExclusive(Storage storage) {
            super(storage);
        }
        public void run() {
            print("ready");
            storage.beginThreadTransaction(Storage.EXCLUSIVE_TRANSACTION);
            print("start");
            Root root = (Root) storage.getRoot();
            if (0 != root.i) failed = "Test failed.";
            root.i = 10;
            try {
                sleep(100+myID*50);
            } catch (InterruptedException e) {
            }
            if (10 != root.i) failed = "Test failed.";
            root.i = 0;
            print("end");
            storage.rollbackThreadTransaction();
            cnt--;
        }
    }

    private static class TestThreadSerializable extends TestThread {
        TestThreadSerializable(Storage storage) {
            super(storage);
        }

        public void run() {
            storage.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
            Root root = (Root) storage.getRoot();
            print("ready");
            root.exclusiveLock();
            print("start");
            if (0 != root.i) failed = "Test failed.";
            root.i = 10;
            try {
                sleep(100);
            } catch (InterruptedException e) {
            }
            if (10 != root.i) failed = "Test failed.";
            root.i = 0;
            print("end");
            root.unlock();
            storage.rollbackThreadTransaction();
            cnt--;
        }
    }
}
