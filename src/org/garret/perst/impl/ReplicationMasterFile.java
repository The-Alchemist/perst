package org.garret.perst.impl;

import java.io.*;
import java.net.*;

import org.garret.perst.*;


/**
 * File performing replication of changed pages to specified slave nodes.
 */
public class ReplicationMasterFile implements IFile, Runnable 
{ 
    /**
     * Constructor of replication master file
     * @param storage replication storage
     * @param file local file used to store data locally
     * @param pageTimestampFile path to the file with pages timestamps. This file is used for synchronizing
     * with master content of newly attached node
     */
    public ReplicationMasterFile(ReplicationMasterStorageImpl storage, IFile file, String pageTimestampFile) { 
        this(storage, file, storage.port, storage.hosts, storage.replicationAck, pageTimestampFile);
    }

    /**
     * Constructor of replication master file
     * @param file local file used to store data locally
     * @param hosts slave node hosts to which replicastion will be performed
     * @param ack whether master should wait acknowledgment from slave node during trasanction commit
     * @param pageTimestampFile path to the file with pages timestamps. This file is used for synchronizing
     * with master content of newly attached node
     */
    public ReplicationMasterFile(IFile file, String[] hosts, boolean ack, String pageTimestampFile) {         
        this(null, file, -1, hosts, ack, pageTimestampFile);
    }
    
    /**
     * Constructor of replication master file
     * @param file local file used to store data locally
     * @param hosts slave node hosts to which replicastion will be performed
     * @param ack whether master should wait acknowledgment from slave node during trasanction commit
     * @param pageTimestampFilePath path to the file with pages timestamps. This file is used for synchronizing
     * with master content of newly attached node
     */
    public ReplicationMasterFile(IFile file, String[] hosts, boolean ack) {         
        this(null, file, -1, hosts, ack, null);
    }
    
    private ReplicationMasterFile(ReplicationMasterStorageImpl storage, IFile file, int port, String[] hosts, boolean ack, String pageTimestampFilePath) 
    {
        this.storage = storage;
        this.file = file;
        this.hosts = hosts;
        this.ack = ack;
        this.port = port;
        mutex = new Object();
        sockets = new Socket[hosts.length];
        syncThreads = new Thread[hosts.length];
        out = new OutputStream[hosts.length];
        if (ack) { 
            in = new InputStream[hosts.length];
            rcBuf = new byte[1];
        }
        nHosts = 0;
        if (pageTimestampFilePath != null) { 
            pageTimestampFile = new OSFile(pageTimestampFilePath, false, storage != null && storage.noFlush);
            long fileLength = pageTimestampFile.length();
            if (fileLength == 0) { 
                pageTimestamps = new int[INIT_PAGE_TIMESTAMPS_LENGTH];
            } else {
                pageTimestamps = new int[(int)(fileLength/4)];
                byte[] page = new byte[Page.pageSize];
                int i = 0;
                for (long pos = 0; pos < fileLength; pos += Page.pageSize) { 
                    int rc = pageTimestampFile.read(pos, page);
                    for (int offs = 0; offs < rc; offs += 4, i++) { 
                        pageTimestamps[i] = Bytes.unpack4(page, offs);
                        if (pageTimestamps[i] > timestamp) { 
                            timestamp = pageTimestamps[i];
                        }
                    }
                }
                if (i != pageTimestamps.length) { 
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR);
                }
            }
            dirtyPageTimestampMap = new int[(((pageTimestamps.length*4 + Page.pageSize - 1) >> Page.pageSizeLog) + 31) >> 5];
            txBuf = new byte[12 + Page.pageSize];
        } else { 
            txBuf = new byte[8 + Page.pageSize];
        }
        for (int i = 0; i < hosts.length; i++) { 
            connect(i);
        }
        if (port >= 0) {
            storage.setProperty("perst.alternative.btree", Boolean.TRUE); // prevent direct modification of pages
            try { 
                listenSocket = new ServerSocket(port);            
            } catch (IOException x) {
                throw new StorageError(StorageError.BAD_REPLICATION_PORT);
            }
            listening = true;
            listenThread = new Thread(this);
            listenThread.start();
        }
    }

    public void run() { 
        while (true) { 
            Socket s = null;
            try { 
                s = listenSocket.accept();
            } catch (IOException x) {
                x.printStackTrace();
            }
            synchronized (mutex) { 
                if (!listening) { 
                    return;
                }
            }
            if (s != null) { 
                try {
                    s.setSoLinger(true, LINGER_TIME);
                } catch (Exception x) {}
                try { 
                    s.setTcpNoDelay(true);
                } catch (Exception x) {}
                addConnection(s);
            }
        }
    }
         
    private void addConnection(Socket s) {
        OutputStream os = null;
        InputStream is = null;
        try { 
            os = s.getOutputStream();
            if (ack || pageTimestamps != null) { 
                is = s.getInputStream();
            }
        } catch (IOException x) { 
            x.printStackTrace();
            return;
        }
        synchronized (mutex) { 
            int n = hosts.length;
            String[] newHosts = new String[n+1];
            System.arraycopy(hosts, 0, newHosts, 0, n);
            newHosts[n] = s.getRemoteSocketAddress().toString();
            hosts = newHosts;
            OutputStream[] newOut = new OutputStream[n+1];
            System.arraycopy(out, 0, newOut, 0, n);            
            newOut[n] = os; 
            out = newOut;
            if (ack || pageTimestamps != null) { 
                InputStream[] newIn = new InputStream[n+1];
                if (in != null) { 
                    System.arraycopy(in, 0, newIn, 0, n);            
                }
                newIn[n] = is; 
                in = newIn;
            }
            Socket[] newSockets = new Socket[n+1];
            System.arraycopy(sockets, 0, newSockets, 0, n);
            newSockets[n] = s;
            sockets = newSockets;
            nHosts += 1;

            Thread t = new SynchronizeThread(n);           
            Thread[] newThreads = new Thread[n+1];        
            System.arraycopy(syncThreads, 0, newThreads, 0, n);
            newThreads[n] = t;
            syncThreads = newThreads;
                            
            t.start();
        }
    }

    private void synchronizeNode(int i) {  
        long size = storage.getDatabaseSize();
        Socket s;
        OutputStream os = null;
        InputStream is = null;
        synchronized (mutex) { 
            s = sockets[i];
            if (s == null) {
                syncThreads[i] = null;
                return;
            }
            os = out[i];
            if (ack || pageTimestamps != null) { 
                is = in[i];
            }
        }
        int[] syncNodeTimestamps = null;
        try { 
          Sync:
            do { 
                byte[] txBuf;
                if (pageTimestamps != null) { 
                    txBuf = new byte[12 + Page.pageSize]; 
                    byte[] psBuf = new byte[4];
                    if (is.read(psBuf) != 4) { 
                        System.err.println("Failed to receive page timestamps length from slave node");
                        break Sync;
                    }
                    int psSize = Bytes.unpack4(psBuf, 0);
                    psBuf = new byte[psSize*4];
                    int offs = 0;
                    while (offs < psBuf.length) { 
                        int rc = is.read(psBuf, offs, psBuf.length - offs);
                        if (rc <= 0) { 
                            System.err.println("Failed to receive page timestamps from slave node");
                            
                            break Sync;
                        }
                        offs += rc;
                    }
                    syncNodeTimestamps = new int[psSize];
                    for (int j = 0; j < psSize; j++) { 
                        syncNodeTimestamps[j] = Bytes.unpack4(psBuf, j*4);
                    }
                } else { 
                    txBuf = new byte[8 + Page.pageSize];                                
                }                               
                for (long pos = 0; pos < size; pos += Page.pageSize) { 
                    int pageNo = (int)(pos >> Page.pageSizeLog);
                    if (syncNodeTimestamps != null) { 
                        if (pageNo < syncNodeTimestamps.length 
                            && pageNo < pageTimestamps.length 
                            && syncNodeTimestamps[pageNo] == pageTimestamps[pageNo])
                        {
                            continue;
                        }
                    }   
                    synchronized (storage)
                    {   
                        synchronized (storage.objectCache)
                        {   
                            Page pg = storage.pool.getPage(pos);
                            Bytes.pack8(txBuf, 0, pos);
                            System.arraycopy(pg.data, 0, txBuf, 8, Page.pageSize);
                            storage.pool.unfix(pg);
                            if (syncNodeTimestamps != null) { 
                                Bytes.pack4(txBuf, Page.pageSize + 8, pageNo < pageTimestamps.length ? pageTimestamps[pageNo] : 0);
                            }                            
                        }                            
                    }
                    Page pg = storage.pool.getPage(pos);
                    synchronized (s) {
                        os.write(txBuf);
                        if (ack && pos == 0 && is.read(rcBuf) != 1) { 
                            System.err.println("Failed to receive ACK");
                            break Sync;
                        }
                    }
                }
                synchronized (s) {
                    Bytes.pack8(txBuf, 0, ReplicationSlaveStorageImpl.REPL_SYNC);
                    os.write(txBuf); // end of synchronization
                }   
                return;
            } while (false);
        } catch (IOException x) {
            x.printStackTrace();
        }
        synchronized (mutex) { 
            if (sockets[i] != null) { 
                handleError(hosts[i]);
                sockets[i] = null;
                out[i] = null;
                syncThreads[i] = null;
                nHosts -= 1;
            }
        }
    }
   
    class SynchronizeThread extends Thread { 
        int i;

        SynchronizeThread(int i) { 
            this.i = i;
            //setPriority(Thread.NORM_PRIORITY-1);
        }

        public void run() { 
            synchronizeNode(i);
        }
    }          
          
    public int getNumberOfAvailableHosts() { 
        return nHosts;
    }

    protected void connect(int i)
    {
        String host = hosts[i];
        int colon = host.indexOf(':');
        int port = Integer.parseInt(host.substring(colon+1));
        host = host.substring(0, colon);
        Socket socket = null; 
        try { 
            int maxAttempts = storage != null 
                ? storage.slaveConnectionTimeout : MAX_CONNECT_ATTEMPTS;
            for (int j = 0; j < maxAttempts; j++) { 
                try { 
                    socket = new Socket(InetAddress.getByName(host), port);
                    if (socket != null) { 
                        break;
                    }
                    Thread.sleep(CONNECTION_TIMEOUT);
                } catch (IOException x) {}
            }
        } catch (InterruptedException x) {}
            
        if (socket != null) { 
            try { 
                try {
                    socket.setSoLinger(true, LINGER_TIME);
                } catch (NoSuchMethodError er) {}
                try { 
                    socket.setTcpNoDelay(true);
                } catch (Exception x) {}
                sockets[i] = socket;
                out[i] = socket.getOutputStream();
                if (ack || pageTimestamps != null) {
                    in[i] = socket.getInputStream();
                }
                nHosts += 1;
                if (pageTimestamps != null) {
                    synchronizeNode(i);
                }
            } catch (IOException x) { 
                handleError(hosts[i]);
                sockets[i] = null;
                out[i] = null;
            }
        } 
    }

    /**
     * When overriden by base class this method perfroms socket error handling
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */     
    public boolean handleError(String host) 
    {
        System.err.println("Failed to establish connection with host " + host);
        return (storage != null && storage.listener != null) 
            ? storage.listener.replicationError(host) 
            : false;
    }


    public void write(long pos, byte[] buf) {
        synchronized (mutex) { 
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
                pageTimestamps[pageNo] = ++timestamp;
                dirtyPageTimestampMap[pageNo >> (Page.pageSizeLog - 2 + 5)] |= 1 << ((pageNo >> (Page.pageSizeLog - 2)) & 31);
            }
            for (int i = 0; i < out.length; i++) { 
                while (out[i] != null) {                 
                    try { 
                        synchronized (sockets[i]) { 
                            Bytes.pack8(txBuf, 0, pos);
                            System.arraycopy(buf, 0, txBuf, 8, buf.length);
                            if (pageTimestamps != null) { 
                                Bytes.pack4(txBuf, Page.pageSize + 8, timestamp);
                            }                            
                            out[i].write(txBuf);
                            if (!ack || pos != 0 || in[i].read(rcBuf) == 1) { 
                                break;
                            }
                        }
                    } catch (IOException x) {} 
                    
                    out[i] = null;
                    sockets[i] = null;
                    nHosts -= 1;
                    if (handleError(hosts[i])) { 
                        connect(i);
                    } else { 
                        break;
                    }
                }
            }
        }
        file.write(pos, buf);
    }

    public int read(long pos, byte[] buf) {
        return file.read(pos, buf);
    }

    public void sync() {
        if (pageTimestamps != null) { 
            synchronized (mutex) { 
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
                                int offs = 0;
                                while (beg < end) {
                                    Bytes.pack4(page, offs, pageTimestamps[beg]);
                                    beg += 1;
                                    offs += 4;
                                }
                                long pos = pageNo << Page.pageSizeLog;
                                pageTimestampFile.write(pos, page);
                            }
                        }
                        dirtyPageTimestampMap[i] = 0;
                    }
                }
            }
            pageTimestampFile.sync();
        }  
        file.sync();
    }

    public boolean tryLock(boolean shared) { 
        return file.tryLock(shared);
    }

    public void lock(boolean shared) { 
        file.lock(shared);
    }

    public void unlock() { 
        file.unlock();
    }

    public void close() {
        if (listenThread != null) { 
            synchronized (mutex) { 
                listening = false;
            }
            try { 
                Socket s = new Socket("localhost", port);
                s.close();
            } catch (IOException x) {}
            try {
                listenThread.join();
            } catch (InterruptedException x) {}
            try { 
                listenSocket.close();
            } catch (IOException x) {}
        }
        for (int i = 0; i < syncThreads.length; i++) {
            Thread t = syncThreads[i];
            if (t != null) { 
                try {
                    t.join(); 
                } catch (InterruptedException x) {}
            }
        }                   
        file.close();
        Bytes.pack8(txBuf, 0, ReplicationSlaveStorageImpl.REPL_CLOSE);
        for (int i = 0; i < out.length; i++) {  
            if (sockets[i] != null) { 
                try {  
                    out[i].write(txBuf);
                    out[i].close();
                    if (in != null) { 
                        in[i].close();
                    }
                    sockets[i].close();
                } catch (IOException x) {}
            }
        }
        if (pageTimestampFile != null) { 
            pageTimestampFile.close();
        }
    }

    public long length() {
        return file.length();
    }

    public static int LINGER_TIME = 10; // linger parameter for the socket
    public static int MAX_CONNECT_ATTEMPTS = 10; // attempts to establish connection with slave node
    public static int CONNECTION_TIMEOUT = 1000; // timeout between attempts to conbbect to the slave
    public static int INIT_PAGE_TIMESTAMPS_LENGTH = 64*1024;

    Object         mutex;
    OutputStream[] out;
    InputStream[]  in;
    Socket[]       sockets;
    byte[]         txBuf;
    byte[]         rcBuf;
    IFile          file;
    String[]       hosts;
    int            nHosts;
    int            port;
    boolean        ack;
    boolean        listening;
    Thread         listenThread;
    ServerSocket   listenSocket;

    int[]          pageTimestamps;
    int[]          dirtyPageTimestampMap;
    OSFile         pageTimestampFile;
    int            timestamp;
    Thread[]       syncThreads;

    ReplicationMasterStorageImpl storage;
}
