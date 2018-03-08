package org.garret.perst.impl;

import java.io.*;
import java.net.*;

import org.garret.perst.*;


public abstract class ReplicationSlaveStorageImpl extends StorageImpl implements ReplicationSlaveStorage, Runnable
{ 
    static final int REPL_CLOSE = -1;
    static final int REPL_SYNC  = -2;
    static final int INIT_PAGE_TIMESTAMPS_LENGTH = 64*1024;

    
    protected ReplicationSlaveStorageImpl(String pageTimestampFilePath) { 
        if (pageTimestampFilePath != null) { 
            pageTimestampFile = new OSFile(pageTimestampFilePath, false, noFlush);
            long fileLength = pageTimestampFile.length();
            if (fileLength == 0) { 
                pageTimestamps = new int[INIT_PAGE_TIMESTAMPS_LENGTH];
            } else {
                pageTimestamps = new int[(int)(fileLength/4)];
                byte[] page = new byte[Page.pageSize];
                int i = 0;
                for (long pos = 0; pos < fileLength; pos += Page.pageSize) { 
                    int rc = pageTimestampFile.read(pos, page);
                    for (int offs = 0; offs < rc; offs += 4) { 
                        pageTimestamps[i++] = Bytes.unpack4(page, offs);
                    }
                }
                if (i != pageTimestamps.length) { 
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }
            }
            dirtyPageTimestampMap = new int[(((pageTimestamps.length*4 + Page.pageSize - 1) >> Page.pageSizeLog) + 31) >> 5];
        }
    }        


    public void open(IFile file, long pagePoolSize) {
        if (opened) {
            throw new StorageError(StorageError.STORAGE_ALREADY_OPENED);
        }
        initialize(file, pagePoolSize);
        lock = new PersistentResource();
        init = new Object();
        sync = new Object();
        done = new Object();
        commit = new Object();
        listening = true;
        connect();        
        thread = new Thread(this);
        thread.start();
        waitSynchronizationCompletion();
        waitInitializationCompletion(); 
        opened = true;
        beginThreadTransaction(REPLICATION_SLAVE_TRANSACTION);
        reloadScheme();
        endThreadTransaction();
    }


    /**
     * Check if socket is connected to the master host
     * @return <code>true</code> if connection between slave and master is sucessfully established
     */
    public boolean isConnected() {
        return socket != null;
    }
    
    public void beginThreadTransaction(int mode)
    {
        if (mode != REPLICATION_SLAVE_TRANSACTION) {
            throw new IllegalArgumentException("Illegal transaction mode");
        }
        lock.sharedLock();
        Page pg = pool.getPage(0);
        header.unpack(pg.data);
        pool.unfix(pg);
        currIndex = 1-header.curr;
        currIndexSize = header.root[1-currIndex].indexUsed;
        committedIndexSize = currIndexSize;
        usedSize = header.root[currIndex].size;
        objectCache.clear();
    }
     
    public void endThreadTransaction(int maxDelay)
    {
        lock.unlock();
    }

    protected void waitSynchronizationCompletion() {
        try { 
            synchronized (sync) { 
                while (outOfSync) { 
                    sync.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    protected void waitInitializationCompletion() {
        try { 
            synchronized (init) { 
                while (!initialized) { 
                    init.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    /**
     * Wait until database is modified by master
     * This method blocks current thread until master node commits trasanction and
     * this transanction is completely delivered to this slave node
     */
    public void waitForModification() { 
        try { 
            synchronized (commit) { 
                if (socket != null) { 
                    commit.wait();
                }
            }
        } catch (InterruptedException x) { 
        }
    }

    protected static final int DB_HDR_CURR_INDEX_OFFSET  = 0;
    protected static final int DB_HDR_DIRTY_OFFSET       = 1;
    protected static final int DB_HDR_INITIALIZED_OFFSET = 2;
    protected static final int PAGE_DATA_OFFSET          = 8;
    
    public static int LINGER_TIME = 10; // linger parameter for the socket

    /**
     * When overriden by base class this method perfroms socket error handling
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */     
    public boolean handleError() 
    {
        return (listener != null) ? listener.replicationError(null) : false;
    }

    void connect()
    {
        try { 
            socket = getSocket();
            if (socket != null) { 
                try {
                    socket.setSoLinger(true, LINGER_TIME);
                } catch (NoSuchMethodError er) {}
                try { 
                    socket.setTcpNoDelay(true);
                } catch (Exception x) {}
                in = socket.getInputStream();
                if (replicationAck || pageTimestamps != null) { 
                    out = socket.getOutputStream();
                }
                if (pageTimestamps != null) { 
                    int size = pageTimestamps.length;
                    byte[] psBuf = new byte[4 + size*4];
                    Bytes.pack4(psBuf, 0, size);
                    for (int i = 0; i < size; i++) { 
                        Bytes.pack4(psBuf, (i+1)*4, pageTimestamps[i]);
                    }
                    out.write(psBuf, 0, psBuf.length);
                }
            }            
        } catch (IOException x) { 
            x.printStackTrace();
            socket = null;
            in = null;
        }
    }

    abstract Socket getSocket() throws IOException;

    void cancelIO() {}

    public void run() { 
        byte[] buf = new byte[Page.pageSize+PAGE_DATA_OFFSET + (pageTimestamps != null ? 4 : 0)];

        while (listening) { 
            int offs = 0;
            do {
                int rc = -1;
                if (in != null) { 
                    try { 
                        rc = in.read(buf, offs, buf.length - offs);
                    } catch (IOException x) { 
                        x.printStackTrace();
                    }
                }
                synchronized(done) { 
                    if (!listening) { 
                        return;
                    }
                }
                if (rc < 0) { 
                    if (handleError()) { 
                        connect();
                    } else { 
                        return;
                    }
                } else { 
                    offs += rc;
                }
            } while (offs < buf.length);
            
            long pos = Bytes.unpack8(buf, 0);
            boolean transactionCommit = false;
            if (pos == 0) { 
                if (replicationAck) { 
                    try { 
                        out.write(buf, 0, 1);
                    } catch (IOException x) {
                        handleError();
                    }
                }
                if (buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET] != prevIndex) { 
                    prevIndex = buf[PAGE_DATA_OFFSET + DB_HDR_CURR_INDEX_OFFSET];
                    lock.exclusiveLock();
                    transactionCommit = true;
                }
            } else if (pos == REPL_SYNC) { 
                synchronized(sync) { 
                    outOfSync = false;
                    sync.notify();
                }
                continue;
            } else if (pos == REPL_CLOSE) { 
                synchronized(commit) { 
                    hangup();
                    commit.notifyAll();
                }     
                return;
            }
            if (pageTimestamps != null) { 
                int pageNo = (int)(pos >> Page.pageSizeLog);
                if (pageNo >= pageTimestamps.length) { 
                    int newLength = pageNo >= pageTimestamps.length*2 ? pageNo+1 : pageTimestamps.length*2;

                    int[] newPageTimestamps = new int[newLength];
                    System.arraycopy(pageTimestamps, 0, newPageTimestamps, 0, pageTimestamps.length);
                    pageTimestamps = newPageTimestamps;

                    int[] newDirtyPageTimestampMap = new int[(((newLength*4 + Page.pageSize - 1) >> Page.pageSizeLog) + 31) >> 5];
                    System.arraycopy(dirtyPageTimestampMap, 0, newDirtyPageTimestampMap, 0, dirtyPageTimestampMap.length);
                    dirtyPageTimestampMap = newDirtyPageTimestampMap;                    
                }
                int timestamp = Bytes.unpack4(buf, Page.pageSize+PAGE_DATA_OFFSET);
                pageTimestamps[pageNo] = timestamp;
                dirtyPageTimestampMap[pageNo >> (Page.pageSizeLog - 2 + 5)] |= 1 << ((pageNo >> (Page.pageSizeLog - 2)) & 31);
            }
            Page pg = pool.putPage(pos);
            System.arraycopy(buf, PAGE_DATA_OFFSET, pg.data, 0, Page.pageSize);
            pool.unfix(pg);
            
            if (pos == 0) { 
                if (!initialized && buf[PAGE_DATA_OFFSET + DB_HDR_INITIALIZED_OFFSET] != 0) { 
                    synchronized(init) { 
                        initialized = true;
                        init.notify();
                    }
                }
                if (transactionCommit) { 
                    lock.unlock();
                    synchronized(commit) { 
                        commit.notifyAll();
                    }
                    if (listener != null) { 
                        listener.onMasterDatabaseUpdate();
                    }
                    pool.flush();
                    if (pageTimestamps != null) { 
                        byte[] page = new byte[Page.pageSize];
                        for (int i = 0; i < dirtyPageTimestampMap.length; i++) { 
                            if (dirtyPageTimestampMap[i] != 0) { 
                                for (int j = 0; j < 32; j++) { 
                                    if ((dirtyPageTimestampMap[i] & (1 << j)) != 0) { 
                                        int pageNo = (i << 5) + j;
                                        int beg = pageNo << (Page.pageSizeLog - 2);
                                        int end = beg + Page.pageSize/4;
                                        if (end > pageTimestamps.length) { 
                                            end = pageTimestamps.length;
                                        }
                                        offs = 0;
                                        while (beg < end) {
                                            Bytes.pack4(page, offs, pageTimestamps[beg]);
                                            beg += 1;
                                            offs += 4;
                                        }
                                        pageTimestampFile.write(pageNo << Page.pageSizeLog, page);
                                    }
                                }
                            }
                            dirtyPageTimestampMap[i] = 0;
                        }
                        pageTimestampFile.sync();
                    }
                }
            }
        }            
    }

    public void close() {
        synchronized (done) {
            listening = false;
        }
        cancelIO();
        try { 
            thread.interrupt();
            thread.join();
        } catch (InterruptedException x) {}

        hangup();

        pool.flush();
        super.close();
        if (pageTimestampFile != null) { 
            pageTimestampFile.close();
        }
    }

    protected void hangup() { 
        if (socket != null) { 
            try { 
                in.close();
                if (out != null) { 
                    out.close();
                }
                socket.close();
            } catch (IOException x) {}
            in = null;
            socket = null;
        }
    }

    protected boolean isDirty() { 
        return false;
    }

    protected InputStream  in;
    protected OutputStream out;
    protected Socket       socket;
    protected boolean      outOfSync;
    protected boolean      initialized;
    protected boolean      listening;
    protected Object       sync;
    protected Object       init;
    protected Object       done;
    protected Object       commit;
    protected int          prevIndex;
    protected IResource    lock;
    protected Thread       thread;
    protected int[]        pageTimestamps;
    protected int[]        dirtyPageTimestampMap;
    protected OSFile       pageTimestampFile;
}
