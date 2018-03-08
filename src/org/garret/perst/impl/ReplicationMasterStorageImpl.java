package org.garret.perst.impl;

import org.garret.perst.*;


public class ReplicationMasterStorageImpl extends StorageImpl implements ReplicationMasterStorage
{ 
    public ReplicationMasterStorageImpl(int port, String[] hosts, int asyncBufSize, String pageTimestampFile) { 
        this.port = port;
        this.hosts = hosts;
        this.asyncBufSize = asyncBufSize;
        this.pageTimestampFile =  pageTimestampFile;
    }
    
    public void open(IFile file, long pagePoolSize) {
        super.open(asyncBufSize != 0 
                   ? (ReplicationMasterFile)new AsyncReplicationMasterFile(this, file, asyncBufSize, pageTimestampFile)
                   : new ReplicationMasterFile(this, file, pageTimestampFile),
                   pagePoolSize);
    }

    public int getNumberOfAvailableHosts() { 
        return ((ReplicationMasterFile)pool.file).getNumberOfAvailableHosts();
    }

    int      port;
    String[] hosts;
    int      asyncBufSize;
    String   pageTimestampFile;
}
