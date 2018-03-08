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
public class PerstDirectory extends Directory 
{    
    /**
     * Create new Lucene directory wrapper using provided Perst implementation of catalogue
     * @param catalogue Perst implemennataion of Lucene directory
     */
    public PerstDirectory(PerstCatalogue catalogue) { 
        this.impl = catalogue;
        setLockFactory(NoLockFactory.getNoLockFactory());
    }

    /**
     * Create new Lucene directory wrapper. If Perst storaeg is not yet initialized then 
     * new instance of PerstCatalogue is created and registered as root object
     * @param storage Perst storage where Lucene indices will be stored
     */
    public PerstDirectory(Storage storage) { 
        impl = (PerstCatalogue)storage.getRoot();
        if (impl == null) { 
            impl = new PerstCatalogue(storage);
            storage.setRoot(impl);
        }
        setLockFactory(NoLockFactory.getNoLockFactory());
    }

    /** Returns an array of strings, one for each file in the directory. */
    public String[] list() throws IOException {
        return impl.list();
    }

    /** Returns true iff a file with the given name exists. */
    public boolean fileExists(String name) throws IOException {
        return impl.fileExists(name);
    }
    
    /** Returns the time the named file was last modified. */
    public long fileModified(String name) throws IOException {
        return impl.fileModified(name); 
    }

    /** Set the modified time of an existing file to now. */
    public void touchFile(String name) throws IOException {
        impl.touchFile(name); 
    }
        
    /** Removes an existing file in the directory. */
    public void deleteFile(String name) throws IOException {
        impl.deleteFile(name); 
    }

    /** Renames an existing file in the directory.
        If a file already exists with the new name, then it is replaced.
        This replacement should be atomic. */
    public void renameFile(String from, String to) throws IOException {
        impl.renameFile(from, to);
    }

    /** Returns the length of a file in the directory. */
    public long fileLength(String name) throws IOException {
        return impl.fileLength(name);
    }

    /** Creates a new, empty file in the directory with the given name.
        Returns a stream writing this file. */
    public IndexOutput createOutput(String name) throws IOException {
        return impl.createOutput(name);
    }

    /** Returns a stream reading an existing file. */
    public IndexInput openInput(String name) throws IOException {
        return impl.openInput(name);
    }

    /** Construct a {@link Lock}.
     * @param name the name of the lock file
     */
    public Lock makeLock(String name) {
        return impl.makeLock(name);
    }

    /** Closes the store. */
    public void close() throws IOException {
        impl.close();
    }

    private PerstCatalogue impl;
}