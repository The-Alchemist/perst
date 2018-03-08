package org.garret.perst;

/**
 * Interface of simple file. Simple file can be automatically opened by database given file path and access mode.
 */
public interface SimpleFile extends IFile { 
    /**
     * Open the file
     * @param path path to the file
     * @param readOnly if file is readonly
     * @param noFlush whther synchronous flush is needed
     * @param storageSegmentLimit maximal size of storage segment
     */
    void open(String path, boolean readOnly, boolean noFlush, int storageSegmentLimit);
}
