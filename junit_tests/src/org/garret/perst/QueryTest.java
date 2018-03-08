/*
 * $URL: QueryTest.java $ 
 * $Rev: 4351 $ 
 * $Date: 2008-04-26 11:49:31 +0400 (Сб., 26 апр. 2008) $
 *
 * Copyright 2005 Netup, Inc. All rights reserved.
 * URL:    http://www.netup.biz
 * e-mail: info@netup.biz
 */

package org.garret.perst;

import junit.framework.TestCase;

import java.util.Iterator;
import java.util.Vector;

/**
 * These tests verifies an implementation of the <code>Query</code> interface. <br />
 * The implementation is created by the following way :
 * <pre>
 *   storage = org.garret.perst.StorageFactory.getInstance().createStorage()
 *   Query query = storage.createQuery()
 * </pre>
 * <P>
 * In test are used two simple <CODE>Persistent</CODE> classes <CODE>Stored</CODE> and
 * <CODE>StoredEx</CODE>:
 * <pre>
 *   class Stored extends Persistent{
 *       int i;
 *       String s;
 *   }
 *
 *   class StoredEx extends Stored{
 *       int ex;
 *   }
 * </pre>
 */
public class QueryTest extends TestCase {
    Storage storage;
    Query query;

    public QueryTest(String testName) {
        super(testName);
    }

    public static junit.framework.Test suite()
    {
        junit.framework.TestSuite suite =
                new junit.framework.TestSuite(QueryTest.class);

        return suite;
    }

    protected void setUp() throws java.lang.Exception {
        storage = StorageFactory.getInstance().createStorage();
        storage.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
        query = storage.createQuery();
    }

    protected void tearDown() throws java.lang.Exception {
        if (storage.isOpened())
            storage.close();
    }
    /**
     * Checks that iterator contains all objects in the <code>objects</code> array.
     * @param it
     * @param objects
     */
    private void checkIteratorContains(Iterator it, Object[] objects){
        Object[] objs = objects.clone();
        outer: for (int i = 0; i < objs.length; i++) {
            assertTrue(it.hasNext());
            Object current = it.next();
            assertNotNull(current);
            for (int j = 0; j < objs.length; j++) {
                if (current == objs[j]) {
                    objs[j] = null;
                    continue outer;
                }
            }
            fail();
        }
        assertFalse(it.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE>
     * method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>The <code>select((Class)null, (Iterator)null, (String)null)</code> method is invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B> <code>NullPointerException</code> was thrown.
     */
    public void test00() {
        try{
            query.select((Class)null, (Iterator)null, (String)null);
            fail();
        }catch(NullPointerException e){
            // expected exception
        }
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE>
     * method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>select(Class clazz, Iterator iter, "")</code> invoked with empty
     * iterator <code>iter</code>.</li>
     * </ul>
     * <P>
     * <B>Result:</B> no  exceptions are thrown.
     */
    public void test01() {
        Vector v = new Vector();
        Iterator i = query.select(Stored.class, v.iterator(), "i=6");
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE>
     * method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>no suitable object in iterator.</li>
     * <li><code>select(Class clazz, Iterator iter, "i=6")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li><code>select(...)</code> returned empty recordset.</li>
     * <li>no  exceptions are thrown.</li>
     * </ul>
     */
    public void test02(){
        Vector v = new Vector();
        v.add(new Stored(5,"t"));
        v.add(new Stored(10, "s"));
        Iterator i = query.select(Stored.class, v.iterator(), "i=6");
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE>
     * method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>no suitable object in iterator.</li>
     * <li><code>select(Class clazz, Iterator iter, "i=6")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li><code>select(...)</code> returned empty recordset.</li>
     * <li>no  exceptions are thrown.</li>
     * </ul>
     */
    public void test03() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        v.add(tc);
        v.add(new Stored(10, "s"));
        Iterator i = query.select(Stored.class, v.iterator(), "i=5");
        assertEquals(tc, i.next());
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE>
     * method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li><code>select(Class clazz, Iterator iter, "asdf=5")</code> invoked
     * (there is no such field "asdf" in Stored).</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li><code>CompileError</code> are thrown.</li>
     * </ul>
     */
    public void test04() {
        try{
            Vector v = new Vector();
            v.add(new Stored(10, "s"));
            query.select(Stored.class, v.iterator(), "asdf=5");
            fail();
        }catch(CompileError e){
            // expected exception
        }
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE>
     * method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>one suitable object in iterator.</li>
     * <li><code>select(Class clazz, Iterator iter, "getI=5")</code> invoked<br>
     * (<code>getI</code> is <code>Stored</code> method).</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned expected object.</li>
     * </ul>
     */
    public void test05() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        v.add(tc);
        Iterator i = query.select(Stored.class, v.iterator(), "getI=5");
        assertEquals(tc, i.next());
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE>
     * method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>one suitable object in iterator.</li>
     * <li><code>select(Class clazz, Iterator iter, "i=5 and s='t'")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned expected object.</li>
     * </ul>
     */
    public void test06() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        v.add(tc);
        Iterator i = query.select(Stored.class, v.iterator(), "i=5 and s='t'");
        assertEquals(tc, i.next());
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>one suitable object in iterator.</li>
     * <li><code>select(Class clazz, Iterator iter, "i>4")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned expected object.</li>
     * </ul>
     */
    public void test07() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        v.add(tc);
        Iterator i = query.select(Stored.class, v.iterator(), "i>4");
        assertEquals(tc, i.next());
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>two suitable objects in iterator.</li>
     * <li><code>select(Class clazz, Iterator iter, "i>4")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned two expected objects.</li>
     * </ul>
     */
    public void test08() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        Stored tc1= new Stored(6, "t");
        v.add(tc );
        v.add(tc1);
        Iterator i = query.select(Stored.class, v.iterator(), "i<10");
        for(int idx=0; idx<2; idx++){
            Object curr = i.next();
            assertNotNull(curr);
            if(curr==tc){
                tc = null;
            }else
            if (curr == tc1) {
                tc1 = null;
            }else{
                fail();
            }
        }
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>two suitable object in iterator(one object of derived class
     * <code>StoredEX</code>).</li>
     * <li><code>select(Class clazz, Iterator iter, "i<10")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned two expected objects.</li>
     * </ul>
     */
    public void test09() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        Stored tc1 = new StoredEx(6, "t", 25);
        v.add(tc);
        v.add(tc1);
        Iterator i = query.select(Stored.class, v.iterator(), "i<10");
        for (int idx = 0; idx < 2; idx++) {
            Object curr = i.next();
            assertNotNull(curr);
            if (curr == tc) {
                tc = null;
            } else if (curr == tc1) {
                tc1 = null;
            } else {
                fail();
            }
        }
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>two suitable objects in iterator.</li>
     * <li><code>select(StoredEx.class, Iterator iter, "i<10")</code> invoked<br>
     * (field <i>i</i> defined in base class <code>Stored</code>).</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned two expected objects.</li>
     * </ul>
     */
    public void test10() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        Stored tc1 = new StoredEx(6, "t", 25);
        v.add(tc);
        v.add(tc1);
        Iterator i = query.select(Stored.class, v.iterator(), "i<10");
        for (int idx = 0; idx < 2; idx++) {
            Object curr = i.next();
            assertNotNull(curr);
            if (curr == tc) {
                tc = null;
            } else if (curr == tc1) {
                tc1 = null;
            } else {
                fail();
            }
        }
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>prepare(...)</CODE> and
     * <code>execute(...)</code> methods.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>two suitable objects in iterator.</li>
     * <li><code>select(Stored.class, Iterator iter, "i<10")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned two expected objects.</li>
     * </ul>
     */
    public void test11() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        Stored tc1 = new Stored(6, "t");
        v.add(tc);
        v.add(tc1);
        query.prepare(Stored.class, "i<10");
        Iterator i = query.execute(v.iterator());
        for (int idx = 0; idx < 2; idx++) {
            Object curr = i.next();
            assertNotNull(curr);
            if (curr == tc) {
                tc = null;
            } else if (curr == tc1) {
                tc1 = null;
            } else {
                fail();
            }
        }
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select(...)</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>one suitable object in iterator.</li>
     * <li><code>select(Stored.class, Iterator iter, "s like '%t%'")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned expected object.</li>
     * </ul>
     */
    public void test12() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "t");
        v.add(tc);
        Iterator i = query.select(Stored.class, v.iterator(), "s like '%t%'");
        assertEquals(tc, i.next());
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select(...)</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>one suitable object in iterator.</li>
     * <li><code>select(Stored.class, Iterator iter, "s like '%t%'")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned expected object.</li>
     * </ul>
     */
    public void test13() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "123t456");
        v.add(tc);
        Iterator i = query.select(Stored.class, v.iterator(), "s like '%t%'");
        assertEquals(tc, i.next());
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select(...)</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>one suitable object in iterator.</li>
     * <li><code>select(Stored.class, Iterator iter, "s like '123%456'")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned expected object.</li>
     * </ul>
     */
    public void test14() {
        Vector v = new Vector();
        Stored tc = new Stored(5, "123t456");
        v.add(tc);
        Iterator i = query.select(Stored.class, v.iterator(), "s like '123%456'");
        assertEquals(tc, i.next());
        assertFalse(i.hasNext());
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select(...)</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>two suitable objects in iterator.</li>
     * <li><code>select(Stored.class, Iterator iter, "i=5 or i=10")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned two expected objects.</li>
     * </ul>
     */
    public void test15() {
        Vector v = new Vector();
        Stored[] tcs = {new Stored(5, ""), new Stored(10, "")};
        for (Stored tc : tcs) {
            v.add(tc);
        }
        Iterator i = query.select(Stored.class, v.iterator(), "i=5 or i=10");
        checkIteratorContains(i, tcs);
    }

    /**
     * <B>Goal:</B> To verify the functionality of the <CODE>select(...)</CODE> method.
     * <P>
     * <B>Conditions:</B>
     * <ul>
     * <li>two suitable objects in iterator.</li>
     * <li><code>select(Stored.class, Iterator iter, "")</code> invoked.</li>
     * </ul>
     * <P>
     * <B>Result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li><code>select(...)</code> returned two expected objects.</li>
     * </ul>
     */
    public void test16() {
        Vector v = new Vector();
        Stored[] tcs = {new Stored(5, ""), new Stored(10, "")};
        for (Stored tc : tcs) {
            v.add(tc);
        }
        Iterator i = query.select(Stored.class, v.iterator(), "");
        checkIteratorContains(i, tcs);
    }
    /**
     * Internal classes.
     */
    private class Stored extends Persistent{
        int i;
        String s;
        Stored(int i, String s){
            this.i = i;
            this.s = s;
        }
        public Stored(){}
        int getI(){
            return i;
        }
    }

    private class StoredEx extends Stored{
        int ex;
        StoredEx(int i, String s, int ex) {
            super(i, s);
            this.ex = ex;
        }
        public StoredEx() {}
    }
}
