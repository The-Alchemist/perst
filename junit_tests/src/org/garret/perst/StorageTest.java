/*
 * $URL: StorageTest.java $ 
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

import java.util.*;
import java.io.File;

/**
 * These tests verifies an implementation of the <code>Storage</code> interface. <br />
 * The implementation is created by the following way :
 * <pre>
 *   storage = org.garret.perst.StorageFactory.getInstance().createStorage()
 * </pre>
 * <p>
 * In test are used simple <CODE>Persistent</CODE> class <CODE>Stored</CODE>:
 * <pre>
 *   class Stored extends Persistent {
 *       public String name;
 *   }
 * </pre>
 */
public class StorageTest extends TestCase {
    Storage storage;

    public StorageTest(String testName) {
        super(testName);
    }

    public static junit.framework.Test suite()
    {
        junit.framework.TestSuite suite =
                new junit.framework.TestSuite(StorageTest.class);

        return suite;
    }

    protected void setUp() throws java.lang.Exception {
        storage = StorageFactory.getInstance().createStorage();
    }

    protected void tearDown() throws java.lang.Exception {
        if(storage.isOpened()){
            storage.close();
        }
        try {
            (new File("StorageTest.dbs")).delete();
        } catch (Exception e) {
        }
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>open(...)</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>The <code>open(...)</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>isOpened()</code> returned <i>true</i>.</li>
     * </ul>
     */
    public void testOpen(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertTrue(storage.isOpened());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>createQuery()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>createQuery()</code> are invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>createQuery()</code> returned not-<i>null</i> object.</li>
     * </ul>
     */
    public void testCreateQuery(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertTrue(storage.isOpened());
        Query q = storage.createQuery();
        assertNotNull(q);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>createIndex()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>createIndex()</code> are invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>createIndex()</code> returned not-<i>null</i> object.</li>
     * </ul>
     */
    public void testCreateIndex() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertTrue(storage.isOpened());
        Index<Root> idx = storage.createIndex(Root.class, false);
        assertNotNull(idx);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>createFieldIndex()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>createFieldIndex()</code> are invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>createFieldIndex()</code> returned not-<i>null</i> object.</li>
     * </ul>
     */
    public void testCreateFieldIndex() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertTrue(storage.isOpened());
        FieldIndex<Root> idx = storage.createFieldIndex(Root.class, "i", false);
        assertNotNull(idx);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>createLink()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>createLink()</code> are invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>createLink()</code> returned not-<i>null</i> object.</li>
     * </ul>
     */
    public void testCreateLink() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertTrue(storage.isOpened());
        Link<Root> l = storage.createLink();
        assertNotNull(l);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>createBlob()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>createBlob()</code> are invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>createBlob()</code> returned not-<i>null</i> object.</li>
     * </ul>
     */
    public void testCreateBlob() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertTrue(storage.isOpened());
        Blob b = storage.createBlob();
        assertNotNull(b);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>createSet()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>createSet()</code> are invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>createSet()</code> returned not-<i>null</i> object.</li>
     * </ul>
     */
    public void testCreateSet(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertTrue(storage.isOpened());
        IPersistentSet ps = storage.createSet();
        assertNotNull(ps);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>getRoot()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>The <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>getRoot()</code> returned <i>null</i>.</li>
     * </ul>
     */
    public void testGetRoot(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        assertNull(storage.getRoot());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>setRoot(...)</CODE> and
     * <CODE>getRoot()</CODE> methods.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>The <code>setRoot(root)</code> method is invoked.</li>
     * <li>The <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>getRoot()</code> returned <i>root</i>.</li>
     * </ul>
     */
    public void testSetRoot(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        Root root = new Root( (IPersistentSet)storage.createSet() );
        storage.setRoot(root);
        assertEquals(storage.getRoot(), root);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>commit()</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>root</code> object implements the <code>Persistent</code>
     * interface.</li>
     * <li>The <code>setRoot(root)</code> method is invoked.</li>
     * <li>The <code>commit()</code> method is invoked.</li>
     * <li>The <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>getRoot()</code> returned <i>root</i>.</li>
     * </ul>
     */
    public void testCommit(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        Root root = new Root( (IPersistentSet) storage.createSet() );
        root.i = 64;
        storage.setRoot(root);
        storage.commit();
        root = (Root)storage.getRoot();
        assertEquals(root.i, 64);
    }

    /**
     * <B>Goal:</B> To verify the transaction functionality.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>The <code>setRoot(root)</code> method is invoked.</li>
     * <li>The <code>commit()</code> method is invoked.</li>
     * <li>The <code>rollback()</code> method is invoked.</li>
     * <li>The <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>getRoot()</code> returned <i>root</i>.</li>
     * </ul>
     */
    public void testTransaction00(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        Root root = new Root( (IPersistentSet) storage.createSet() );
        root.i = 128;
        storage.setRoot(root);
        storage.commit();
        storage.rollback();
        root = (Root)storage.getRoot();
        assertEquals(root.i, 128);
    }

    /**
     * Checks the <CODE>commit</CODE> and <CODE>rollback</CODE>
     * methods.<P>
     * <B>Conditions:</B> <CODE>rollback</CODE> invoked after storage
     * root has changed.<P>
     * <B>Result:</B> changes successfully rolled back.
     */
    /**
     * <B>Goal:</B> To verify the transaction functionality.
     * <P>* <B>Conditions:</B>
     * <ul>
     * <li><code>setRoot(root)</code> is invoked.</li>
     * <li><code>getRoot()</code> is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>getRoot()</code> returned <i>root</i>.</li>
     * </ul>
     */
    public void testTransaction01(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        Root root = new Root( (IPersistentSet) storage.createSet() );
        root.i = 10;
        storage.setRoot(root);
        storage.commit();
        root.i = 20;
        storage.rollback();
        root = (Root)storage.getRoot();
        assertEquals(root.i, 10);
    }

    /**
     * <B>Goal:</B> To verify the transaction functionality.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><CODE>rollback</CODE> invoked after storage root has changed.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li>changes successfully rolled back.</li>
     * </ul>
     */
    public void testTransaction02(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        Root root = new Root( (IPersistentSet) storage.createSet() );
        storage.setRoot(root);
        storage.commit();
        root.records.add( new Stored("rec1") );
        storage.rollback();
        root = (Root)storage.getRoot();
        Iterator iterator = root.records.iterator();
        assertFalse(iterator.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the transaction functionality.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><CODE>rollback</CODE> invoked after storage root has changed.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li>changes successfully rolled back.</li>
     * </ul>
     */
    public void testTransaction03(){
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        Root root = new Root( (IPersistentSet)storage.createSet() );
        storage.setRoot(root);
        root.records.add( new Stored("rec1") );
        storage.commit();
        root.records.add( new Stored("rec2") );
        storage.rollback();
        root = (Root)storage.getRoot();
        Iterator iterator = root.records.iterator();
        assertTrue(iterator.hasNext());
        assertEquals( ((Stored)iterator.next()).name, "rec1" );
        assertFalse(iterator.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the storage listener functionality.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>set storage listener by invocation of <code>setListener(listener)</code>.</li>
     * <li>invoke <code>select(...)</code> with runtime exception (division by zero).</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * </ul>
     */
    public void testStorageListener00() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        TestStorageListener listener = new TestStorageListener();
        Root root = new Root((IPersistentSet) storage.createSet());
        storage.setRoot(root);
        root.records.add( new Stored() );
        storage.setListener(listener);
        Query query = storage.createQuery();
        query.enableRuntimeErrorReporting(true);
        Iterator i = query.select(Stored.class, root.records.iterator(), "(1/i)=1");
        i.hasNext();
    }

    /**
     * <B>Goal:</B> To verify the storage listener functionality.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>set storage listener by invocation of <code>setListener(listener)</code>.</li>
     * <li>invoke <code>select(...)</code> with runtime exception (division by zero).</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>One exception was passed to storage listener.</li>
     * </ul>
     */
    public void testStorageListener01() {
        storage.open(new NullFile(), INFINITE_PAGE_POOL);
        TestStorageListener listener = new TestStorageListener();
        Root root = new Root((IPersistentSet) storage.createSet());
        storage.setRoot(root);
        root.records.add(new Stored());
        storage.setListener(listener);
        Query query = storage.createQuery();
        query.enableRuntimeErrorReporting(true);
        try {
            Iterator i = query.select(Stored.class, root.records.iterator(), "(1/i)=1");
            i.hasNext();
        } catch (Exception e) {
            //
        };
        assertEquals(1, listener.exceptions.size());
    }

    /**
     * <B>Goal:</B> To prove coreect storing and loading objects.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>the <code>root</code> object implements the <code>Persistent</code> interface.</li>
     * <li>the <code>setRoot(root)</code> method is invoked.</li>
     * <li>the <code>close()</code> method is invoked.</li>
     * <li>the <code>open(...)</code> method is invoked.</li>
     * <li>the <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li><code>integer</code> field correctly stored in db-file:
     * stored and retrieved fields are identical.</li>
     * </ul>
     */
    public void testStoreLoad00() {
        storage.open("StorageTest.dbs");
        Root root = new Root((IPersistentSet) storage.createSet());
        root.i = 25;
        storage.setRoot(root);
        storage.close();
        storage.open("StorageTest.dbs");
        root = (Root) storage.getRoot();
        assertEquals(25, root.i);
    }

    /**
     * <B>Goal:</B> To prove coreect storing and loading objects.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>the <code>root</code> object implements the <code>Persistent</code> interface.</li>
     * <li>the <code>setRoot(root)</code> method is invoked.</li>
     * <li>the <code>close()</code> method is invoked.</li>
     * <li>the <code>open(...)</code> method is invoked.</li>
     * <li>the <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li><code>String</code> field correctly stored in db-file:
     * stored and retrieved fields are identical.</li>
     * </ul>
     */
    public void testStoreLoad01() {
        storage.open("StorageTest.dbs");
        Root root = new Root((IPersistentSet) storage.createSet());
        String str = "test string";
        root.s = str;
        storage.setRoot(root);
        storage.close();
        storage.open("StorageTest.dbs");
        root = (Root) storage.getRoot();
        assertEquals(str, root.s);
    }

    /**
     * <B>Goal:</B> To prove coreect storing and loading objects.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>the <code>root</code> object implements the <code>Persistent</code> interface.</li>
     * <li>the <code>setRoot(root)</code> method is invoked.</li>
     * <li>the <code>close()</code> method is invoked.</li>
     * <li>the <code>open(...)</code> method is invoked.</li>
     * <li>the <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li><code>double</code> field correctly stored in db-file:
     * stored and retrieved fields are identical.</li>
     * </ul>
     */
    public void testStoreLoad02() {
        storage.open("StorageTest.dbs");
        Root root = new Root((IPersistentSet) storage.createSet());
        double d = 12345E-42;
        root.d = d;
        storage.setRoot(root);
        storage.close();
        storage.open("StorageTest.dbs");
        root = (Root) storage.getRoot();
        assertEquals(d, root.d);
    }

    /**
     * <B>Goal:</B> To prove correct storing and loading objects.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>the <code>root</code> object implements the <code>Persistent</code> interface.</li>
     * <li>the <code>setRoot(root)</code> method is invoked.</li>
     * <li>the <code>close()</code> method is invoked.</li>
     * <li>the <code>open(...)</code> method is invoked.</li>
     * <li>the <code>getRoot()</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>Object reference field correctly stored in db-file:
     * stored and retrieved objects are identical.</li>
     * </ul>
     */
    public void testStoreLoad03() {
        storage.open("StorageTest.dbs");
        Root root = new Root(null);
        root.next = new Root(null);
        root.next.i = 25;
        storage.setRoot(root);
        storage.close();
        storage.open("StorageTest.dbs");
        root = (Root) storage.getRoot();
        assertEquals(25, root.next.i);
    }

    /**
     * Internal class.
     */
    private static class Root extends Persistent{
        IPersistentSet records;
        int i;
        Root next;
        String s;
        double d;
        public Root(IPersistentSet records){
            this.records = records;
        }
        public Root(){
        }
    }

    /**
     * Internal class.
     */
    private static class Stored extends Persistent{
        public String name;
        int i=0;
        public Stored(String name){
            this.name = name;
        }
        public Stored(){}
    }

    private static class TestStorageListener extends StorageListener{
        public Vector<JSQLRuntimeException> exceptions =
                new Vector<JSQLRuntimeException>();
        public void JSQLRuntimeError(JSQLRuntimeException x) {
            exceptions.add(x);
        }
    }

 }
