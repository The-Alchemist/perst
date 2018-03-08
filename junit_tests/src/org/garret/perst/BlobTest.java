/*
 * $URL: BlobTest.java $ 
 * $Rev: 3582 $ 
 * $Date: 2007-11-25 14:29:06 +0300 (Вс., 25 нояб. 2007) $
 *
 * Copyright 2005 Netup, Inc. All rights reserved.
 * URL:    http://www.netup.biz
 * e-mail: info@netup.biz
 */

package org.garret.perst;

import junit.framework.*;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.File;

/**
 * This test class verifies the <CODE>Blob</CODE> type supporting by the Perst
 * storage.
 */
public class BlobTest extends TestCase {

    Storage storage;
    Blob blob;

    public BlobTest(String testName) {
        super(testName);
    }
    public static junit.framework.Test suite()
    {
        junit.framework.TestSuite suite =
                new junit.framework.TestSuite(BlobTest.class);

        return suite;
    }


    protected void setUp() throws java.lang.Exception {
    }

    protected void tearDown() throws java.lang.Exception {
        if (storage.isOpened()){
            storage.close();
        }
        try{
            (new File("BlobTest.dbs")).delete();
        }catch(Exception e){
        }
    }

    /**
     * <b>Goal:</b> To verify whether the Perst correct stores an instance of <code>Blob</code> class into the memory.<br />
     * <B>Conditions:</B>
     * <ul>
     * <li>an instance of the <code>Blob</code> class created and stored into the storage.</li>
     * <li>the an instance of the <code>Blob</code> class retrieved from the storage.</li>
     * </ul>
     * <P>
     * <B>Expected result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li>Stored and retrieved blob content are the same.</li>
     * </ul>
     */
    public void test00() throws Exception{
        storage = StorageFactory.getInstance().createStorage();
        storage.open("BlobTest.dbs");
        blob = storage.createBlob();
        storage.setRoot(blob);
        OutputStream os = blob.getOutputStream();
        byte[] arr = new byte[1024*1024];
        for(int i = 0; i<arr.length; i++){
            arr[i] = (byte)(i % 256);
        }
        // test target
        os.write(arr);
        blob = (Blob)storage.getRoot();
        InputStream is = blob.getInputStream();
        byte[] arr2 = arr.clone();
        is.read(arr2);
        for(int i=0; i<arr.length; i++){
            if (arr[i]!=arr2[i]){
                fail();
            }
        }
    }

    /**
     * <b>Goal:</b>  To verify whether the Perst correct stores an instance of <code>Blob</code> class into a file.<br />
     * <B>Conditions:</B>
     * <ul>
     * <li>an instance of the <code>Blob</code> class created and stored into the storage.</li>
     * <li>Storage closed and reopened.</li>
     * <li>the an instance of the <code>Blob</code> class retrieved from the storage.</li>
     * </ul>
     * <P>
     * <B>Expected result:</B>
     * <ul>
     * <li>no exceptions are thrown.</li>
     * <li>Stored and retrieved blob content are the same.</li>
     * </ul>
     */
    public void test01() throws Exception {
        storage = StorageFactory.getInstance().createStorage();
        storage.open("BlobTest.dbs");
        blob = storage.createBlob();
        storage.setRoot(blob);
        OutputStream os = blob.getOutputStream();
        byte[] arr = new byte[1024 * 1024];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (byte) (i % 256);
        }
        // test target
        os.write(arr);
        storage.close();
        storage.open("BlobTest.dbs");
        blob = (Blob) storage.getRoot();
        InputStream is = blob.getInputStream();
        byte[] arr2 = arr.clone();
        is.read(arr2);
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] != arr2[i]) {
                fail();
            }
        }
    }
}
