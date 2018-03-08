package org.garret.perst.impl;
import org.garret.perst.*;
import java.io.*;

public class FileFactory 
{ 
    public static SimpleFile createFile() 
    {
        Class systemClass;
        Class wrapperClass;
        try { 
            systemClass = Class.forName("java.io.RandomAccessFile");
            wrapperClass = Class.forName("org.garret.perst.impl.CdcFile");
            return (SimpleFile)wrapperClass.newInstance();
        } catch (Exception x) {
            try {
                systemClass = Class.forName("javax.microedition.io.file.FileConnection");
                wrapperClass = Class.forName("org.garret.perst.impl.Jsr75File");
                return (SimpleFile)wrapperClass.newInstance();
            } catch (Exception x2) {  
                try { 
                    systemClass = Class.forName("javax.microedition.rms.RecordStore");
                    wrapperClass = Class.forName("org.garret.perst.impl.RmsFile");
                    return (SimpleFile)wrapperClass.newInstance();
                } catch (Exception x3) {  
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR, x3);
                }
            }
        } catch (Throwable x) {
            try {
                systemClass = Class.forName("javax.microedition.io.file.FileConnection");
                wrapperClass = Class.forName("org.garret.perst.impl.Jsr75File");
                return (SimpleFile)wrapperClass.newInstance();
            } catch (Throwable x2) {  
                try { 
                    systemClass = Class.forName("javax.microedition.rms.RecordStore");
                    wrapperClass = Class.forName("org.garret.perst.impl.RmsFile");
                    return (SimpleFile)wrapperClass.newInstance();
                } catch (Throwable x3) {  
                    throw new StorageError(StorageError.FILE_ACCESS_ERROR, x3);
                }
            }
        }
    }
}
