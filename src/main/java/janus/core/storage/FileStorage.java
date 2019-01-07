package janus.core.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileStorage implements Storage {
    
    public static FileStorage of(File file) {
        try {
            return new FileStorage(new RandomAccessFile(file, "rwd"));
        } catch (FileNotFoundException e) {
            throw new UnsupportedOperationException(e);
        }
    }
    
    protected FileStorage(RandomAccessFile file) {
        this.file = file;
    }

    @Override
    public void write(long at, byte[] data, int off, int len) {
        synchronized(this) {
            try {
                this.file.seek(at);
                this.file.write(data, off, len);
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void read(long at, byte[] data, int off, int len) {
        synchronized(this) {
            try {
                this.file.seek(at);
                if(this.file.read(data, off, len) != len) {
                    throw new IllegalStateException("Failed to read partial or all data.");
                }
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    @Override
    public void close() throws Exception {
        this.file.close();
    }

    private RandomAccessFile file;    
}
