package org.garret.perst.impl;

import java.io.*;
import java.net.*;

import org.garret.perst.*;

/**
 * File performing asynchronous replication of changed pages to specified slave nodes.
 */
public class AsyncReplicationMasterFile extends ReplicationMasterFile {
    /**
     * Constructor of replication master file
     * @param storage replication storage
     * @param file local file used to store data locally
     * @param asyncBufSize size of asynchronous buffer
     * @param pageTimestampFile path to the file with pages timestamps. This file is used for synchronizing
     * with master content of newly attached node
     */
    public AsyncReplicationMasterFile(ReplicationMasterStorageImpl storage, IFile file, int asyncBufSize, String pageTimestampFile) { 
        super(storage, file, pageTimestampFile);
        this.asyncBufSize = asyncBufSize;
        start();
    }


    /**
     * Constructor of replication master file
     * @param file local file used to store data locally
     * @param hosts slave node hosts to which replicastion will be performed
     * @param asyncBufSize size of asynchronous buffer
     * @param ack whether master should wait acknowledgment from slave node during trasanction commit
     */
    public AsyncReplicationMasterFile(IFile file, String[] hosts, int asyncBufSize, boolean ack) {                 
        this(file, hosts, asyncBufSize, ack, null);
    }

    /**
     * Constructor of replication master file
     * @param file local file used to store data locally
     * @param hosts slave node hosts to which replicastion will be performed
     * @param asyncBufSize size of asynchronous buffer
     * @param ack whether master should wait acknowledgment from slave node during trasanction commit
     * @param pageTimestampFile path to the file with pages timestamps. This file is used for synchronizing
     * with master content of newly attached node
     */
    public AsyncReplicationMasterFile(IFile file, String[] hosts, int asyncBufSize, boolean ack, String pageTimestampFile) {                 
        super(file, hosts, ack, pageTimestampFile);
        this.asyncBufSize = asyncBufSize;
        start();
    }

    class WriteThread extends Thread { 
        public void run() { 
            asyncWrite();
        }
    }

    private void start() {
        go = new Object();
        async = new Object();
        thread = new WriteThread();
        thread.start();
    }
                
    static class Parcel {
        byte[] data;
        long   pos;
        int    host;
        Parcel next;
    }
    
    public void write(long pos, byte[] buf) {
        file.write(pos, buf);
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
        }
        for (int i = 0; i < out.length; i++) { 
            if (out[i] != null) {                
                byte[] data = new byte[8 + buf.length];
                Bytes.pack8(data, 0, pos);
                System.arraycopy(buf, 0, data, 8, buf.length);
                Parcel p = new Parcel();
                p.data = data;
                p.pos = pos;
                p.host = i;
                try { 
                    synchronized(async) { 
                        buffered += data.length;
                        while (buffered > asyncBufSize && buffered != data.length) { 
                            async.wait();
                        }
                    }
                } catch (InterruptedException x) {}
                    
                synchronized(go) { 
                    if (head == null) { 
                        head = tail = p;
                    } else { 
                        tail = tail.next = p;
                    }
                    go.notify();
                }
            }
        }
    }

    public void asyncWrite() { 
        try { 
            while (true) { 
                Parcel p;
                synchronized(go) {
                    while (head == null) { 
                        if (closed) { 
                            return;
                        }
                        go.wait();
                    }
                    p = head;
                    head = p.next;
                }  
                
                synchronized(async) { 
                    if (buffered > asyncBufSize) { 
                        async.notifyAll();
                    }
                    buffered -= p.data.length;
                }

                int i = p.host;
                while (out[i] != null) { 
                    try { 
                        out[i].write(p.data);
                        if (!ack || p.pos != 0 || in[i].read(rcBuf) == 1) {
                            break;
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
        } catch (InterruptedException x) {}
    }

    public void close() {
        try { 
            synchronized (go) {
                closed = true;
                go.notify();
            }
            thread.join();
        } catch (InterruptedException x) {}
        super.close();
    }

    private int     asyncBufSize;
    private int     buffered;
    private boolean closed;
    private Object  go;
    private Object  async;
    private Parcel  head;
    private Parcel  tail;    
    private Thread  thread;
}
