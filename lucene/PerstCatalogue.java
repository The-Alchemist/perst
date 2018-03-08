import org.garret.perst.*;
import org.apache.lucene.store.*;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * Perst implementation of Lucene Directory
 * For some strange reasons designed of Lucene have made Directory and anstract class and not interface
 * (although all its methods are abstract)
 * Frankly speaking I do not understand logic of Lucene designers - there completely no sense in
 * delcaring abstract class with all abstract methods. Using interfaces is much flexible, natural and 
 * convenient.
 * Because Java doesn't support multiple inheritace I can no use inheritance and implement
 * Directory methods directly, but use adapter class which just perform rediretion of method invocation.
 */
public class PerstCatalogue extends PersistentResource 
{ 
    static class PerstFile extends Persistent { 
        String name;
        long   timestamp;
        Blob   content;
        
        public void deallocate() {
            content.deallocate();
            super.deallocate();
        }

        PerstFile() {}
        PerstFile(String name) {
            this.name = name;
            timestamp = System.currentTimeMillis();
        }                              
    }

    static class PerstOutputStream extends BufferedIndexOutput { 
        PerstOutputStream(RandomAccessOutputStream stream) { 
            this.stream = stream;
        }

        protected void flushBuffer(byte[] b, int len) throws IOException {
            flushBuffer(b, 0, len);
        }

        protected void flushBuffer(byte[] b, int off, int len) throws IOException {
            stream.write(b, off, len);
        }

        public void seek(long pos) throws IOException {
            super.seek(pos);
            stream.setPosition(pos);
        }

        public long length() throws IOException { 
            return stream.size();
        }

        public void close() throws IOException {
            super.close();
            stream.close();
        }

        RandomAccessOutputStream stream;
    }

    static class PerstInputStream extends BufferedIndexInput { 
        PerstInputStream(RandomAccessInputStream stream) { 
            this.stream = stream;
        }

        public long length() { 
            return stream.size();
        }

        public Object clone() {
            PerstInputStream clone = (PerstInputStream)super.clone();
            clone.isClone = true;
            return clone;
        }


        protected void readInternal(byte[] b, int offset, int length) throws IOException {
            synchronized (stream) {
                stream.setPosition(getFilePointer());
                int rc = stream.read(b, offset, length);
                if (rc != length) { 
                    throw new IOException("Failed to read all requested data");
                }
            }
        }

        public void close() throws IOException {
            if (!isClone) { 
                stream.close();
            }
        }
        
        protected void seekInternal(long pos) throws IOException { 
        }
        
        RandomAccessInputStream stream;
        boolean                 isClone;
    }

    
    static class PerstLock extends Lock { 
        private boolean   locked;
        private IResource resource;

        /** Attempts to obtain exclusive access and immediately return
         *  upon success or failure.
         * @return true iff exclusive access is obtained
         */
        public boolean obtain() { 
            return locked = resource.exclusiveLock(0); 
        }

        /** Attempts to obtain an exclusive lock within amount
         *  of time given. Currently polls once per second until
         *  lockWaitTimeout is passed.
         * @param lockWaitTimeout length of time to wait in ms
         * @return true if lock was obtained
         * @throws IOException if lock wait times out or obtain() throws an IOException
         */
        public boolean obtain(long lockWaitTimeout) {
            return locked = resource.exclusiveLock(lockWaitTimeout);            
        }
            
        public void release() {
            if (locked) { 
                resource.unlock();
                locked = false;
            }
        }

        /** Returns true if the resource is currently locked.  Note that one must
         * still call {@link #obtain()} before using the resource. */
        public boolean isLocked() { 
            return locked;
        }

        PerstLock(IResource resource) { 
            this.resource = resource;
        }
    }

    private FieldIndex index;
                
    private PerstCatalogue() {}
        
    public PerstCatalogue(Storage storage) { 
        super(storage);
        index = storage.createFieldIndex(PerstFile.class, "name", true);
    }

    /** Returns an array of strings, one for each file in the directory. */
    public String[] list()
    {
        String[] names = new String[index.size()];
        Iterator iterator = index.entryIterator();
        for (int i = 0; i < names.length; i++) {
            names[i] = (String)((Map.Entry)iterator.next()).getKey();
        }
        return names;
    }
    
    /** Returns true iff a file with the given name exists. */
    public boolean fileExists(String name) 
    {
        return index.get(name) != null;
    }
    
    /** Returns the time the named file was last modified. */
    public long fileModified(String name) throws IOException 
    { 
        PerstFile file = (PerstFile)index.get(name);
        if (file == null) { 
            throw new FileNotFoundException(name);
        }
        return file.timestamp;
    }
    
    /** Set the modified time of an existing file to now. */
    public void touchFile(String name) throws IOException 
    {
        PerstFile file = (PerstFile)index.get(name);
        if (file == null) { 
            throw new FileNotFoundException(name);
        }
        file.timestamp = System.currentTimeMillis();
        file.modify();
    }
    
    /** Removes an existing file in the directory. */
    public void deleteFile(String name)throws IOException
    {        
        PerstFile file = (PerstFile)index.get(name);
        if (file == null) { 
            throw new FileNotFoundException(name);
        }
        index.remove(file);
        file.deallocate();
    }
    
    /** Renames an existing file in the directory.
        If a file already exists with the new name, then it is replaced.
        This replacement should be atomic. */
    public void renameFile(String from, String to) throws IOException
    {
        if (from.equals(to)) { 
            return;
        }
        PerstFile fromFile = (PerstFile)index.get(from);
        if (fromFile == null) { 
            throw new FileNotFoundException(from);
        }
        PerstFile toFile = (PerstFile)index.get(to);
        index.remove(fromFile);
        fromFile.name = to;
        index.set(fromFile);
        fromFile.modify();
        if (toFile != null) { 
            toFile.deallocate();
        }
    }
    
    /** Returns the length of a file in the directory. */
    public long fileLength(String name) throws IOException
    {        
        PerstFile file = (PerstFile)index.get(name);
        if (file == null) { 
            throw new FileNotFoundException(name);
        }
        return file.content.getInputStream().size();
    }
    
    /** Creates a new, empty file in the directory with the given name.
        Returns a stream writing this file. */
    public IndexOutput createOutput(String name) throws IOException
    {
        PerstFile file = (PerstFile)index.get(name);
        if (file == null) { 
            file = new PerstFile(name);
            index.put(file);
        } else { 
            file.content.deallocate();
        }
        file.content = getStorage().createRandomAccessBlob();
        file.modify();
        return new PerstOutputStream(file.content.getOutputStream(Blob.DOUBLE_SEGMENT_SIZE|Blob.ENABLE_SEGMENT_CACHING));
    }
    
    /** Returns a stream reading an existing file. */
    public IndexInput openInput(String name) throws IOException
    {
        PerstFile file = (PerstFile)index.get(name);
        if (file == null) { 
            throw new FileNotFoundException(name);
        }
        return new PerstInputStream(file.content.getInputStream(Blob.ENABLE_SEGMENT_CACHING));
    }
    
    /** Construct a {@link Lock}.
     * @param name the name of the lock file
     */
    public Lock makeLock(String name) { 
        return new PerstLock(this);
    }
    
    /** Closes the store. */
    public void close() {}
}
