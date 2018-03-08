package org.garret.perst.impl;
import org.garret.perst.*;

import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import javax.microedition.io.*;
import javax.microedition.io.file.*;

public class Jsr75File implements SimpleFile 
{
    private FileConnection fconn;
    private InputStream    in;
    private OutputStream   out;
    private long           inPos;
    private long           outPos;
    private long           currPos;
    private long           fileSize;
    private int            fileSizeLimit;
    private boolean        noFlush;
    private boolean        readOnly;
    private String         url;
    private int            currSegment;

    private static final int ZERO_BUF_SIZE = 4096;

    public void open(String path, boolean readOnly, boolean noFlush, int fileSizeLimit) 
    {
        url = path;
        this.fileSizeLimit = fileSizeLimit;
        this.noFlush = noFlush;
        this.readOnly = readOnly;
        if (!url.startsWith("file:")) { 
            if (url.startsWith("/")) { 
                url = "file:///" + path;
            } else { 
                Enumeration e = FileSystemRegistry.listRoots();
                while (e.hasMoreElements()) {
                    // choose arbitrary root directory
                    url = "file:///" + (String)e.nextElement() + path;
                    try { 
                        fconn = (FileConnection)Connector.open(url);
                        // If no exception is thrown, then the URI is valid, but the file may or may not exist.
                        if (!fconn.exists()) { 
                            fconn.create();  // create the file if it doesn't exist
                        }
                        break;
                    } catch (Exception x) {
                        fconn = null;
                        url = "file:///" + path;
                        // try next root
                    }
                }
            }
        }
        try { 
            if (fconn == null) { 
                fconn = (FileConnection)Connector.open(url);
                // If no exception is thrown, then the URI is valid, but the file may or may not exist.
                if (!fconn.exists()) { 
                    fconn.create();  // create the file if it doesn't exist
                }
            }
            fileSize = fconn.fileSize();
            if (fileSizeLimit != 0) { 
                FileConnection c;
                int segment;
                long lastSegmentSize = fileSize;
                for (segment = 1; (c = (FileConnection)Connector.open(url + segment)) != null && c.exists(); segment++) {
                    lastSegmentSize = c.fileSize();
                    c.close();
                }
                if (c != null) { 
                    c.close();
                }
                fileSize = (segment-1)*fileSizeLimit + lastSegmentSize;
                currSegment = 0;
            }
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }        
        in = null;
        inPos = 0;
        out = null;
        outPos = 0;
    }

    public int read(long pos, byte[] b)
    {
        if (pos >= fileSize) { 
            return 0;
        }
        int len = b.length;

        try {
            if (fileSizeLimit != 0) { 
                int readBytes = 0;
                int dstOffs = 0;
                while (len != 0) { 
                    int segment = (int)(pos / fileSizeLimit);
                    if (currSegment != segment) { 
                        close();
                        currSegment = segment;
                        fconn = (FileConnection)Connector.open(segment == 0 ? url : url + segment);
                    }
                    int srcOffs = (int)(pos % fileSizeLimit);
                    int chunk = fileSizeLimit - srcOffs >= len ? len : fileSizeLimit - srcOffs;
                    int rc = read(srcOffs, b, dstOffs, chunk); 
                    if (rc > 0) { 
                        pos += rc;
                        len -= rc;
                        dstOffs += rc;
                        readBytes += rc;
                    }
                    if (rc != chunk || pos >= fileSize) { 
                        break;
                    }
                }
                return readBytes;
            } else { 
                return read(pos, b, 0, len);
            }
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        } 
    }
                    
    private int read(long pos, byte[] b, int offs, int len) throws IOException 
    {
        if (in == null || inPos > pos) { 
            sync(); 
            if (in != null) { 
                in.close();
            }
            in = fconn.openInputStream();
            inPos = in.skip(pos);
        } else if (inPos < pos) { 
            inPos += in.skip(pos - inPos);
        }
        if (inPos != pos) { 
            return 0;
        }        
        int readBytes = 0;
        while (len > 0) { 
            int rc = in.read(b, offs, len);
            if (rc > 0) { 
                inPos += rc;
                offs += rc;
                len -= rc;
                readBytes += rc;
            } else { 
                break;
            }
        }
        return readBytes;
    }

    public void write(long pos, byte[] b)
    {
        if (readOnly) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, "Illegal mode");
        }
        try {
            if (pos > fileSize) { 
                byte[] zeroBuf = new byte[ZERO_BUF_SIZE];
                do { 
                    int size = pos - fileSize > ZERO_BUF_SIZE ? ZERO_BUF_SIZE : (int)(pos - fileSize);
                    write(fileSize, zeroBuf, size);
                } while (pos != fileSize);
            }
            write(pos, b, b.length);
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        } 
    }
    
    private void write(long pos, byte[] b, int len) throws IOException
    {
        if (fileSizeLimit != 0) { 
            int srcOffs = 0;
            while (len != 0) { 
                int segment = (int)(pos / fileSizeLimit);
                if (currSegment != segment) { 
                    close();
                    currSegment = segment;
                    fconn = (FileConnection)Connector.open(segment == 0 ? url : url + segment);
                    if (!fconn.exists()) {
                        fconn.create();  // create the file if it doesn't exist
                    }
                }
                int dstOffs = (int)(pos % fileSizeLimit);
                int chunk = fileSizeLimit - dstOffs >= len ? len : fileSizeLimit - dstOffs;
                write(dstOffs, b, srcOffs, chunk); 
                pos += chunk;
                len -= chunk;
                srcOffs += chunk;
            }
        } else { 
            write(pos, b, 0, len);
            pos += len;
        }
        if (pos > fileSize) { 
            fileSize = pos;
        }
    }

    private void write(long pos, byte[] b, int offs, int len) throws IOException
    {
        if (out == null || outPos != pos) {                         
            if (out != null) {
                out.close();
            }
            out = fconn.openOutputStream(pos);
            outPos = pos;
        }
        out.write(b, offs, len);
        outPos += len;
        if (in != null) { 
            in.close();
            in = null;
        }
    }

    public long length() { 
        return fileSize;
    }

    public void close() { 
        try {
            if (in != null) { 
                in.close();
                in = null;
            }
            if (out != null) { 
                out.close();
                out = null;                
            }
            inPos = outPos = 0;
            fconn.close();
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }
    
    public void sync() {
        if (out != null) { 
            try { 
                out.flush();
            } catch(IOException x) { 
                throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
            }
        }
    }
}
