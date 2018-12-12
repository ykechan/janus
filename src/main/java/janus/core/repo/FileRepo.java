package janus.core.repo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileRepo implements Repository {
    
    public static FileRepo of(File file) {
        try {
            return new FileRepo(new RandomAccessFile(file, "rwd"));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    protected FileRepo(RandomAccessFile file) {
        this.file = file;
    }

    @Override
    public void read(long at, byte[] buf) {
        try {
            this.file.seek(at);
            this.file.read(buf, 0, buf.length);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }        
    }

    @Override
    public void write(long at, byte[] buf) {
        try {
            this.file.seek(at);
            this.file.write(buf, 0, buf.length);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            this.file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private RandomAccessFile file;
}
