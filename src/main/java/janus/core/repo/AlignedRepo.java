package janus.core.repo;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class AlignedRepo implements Repository {
    
    public static final int META_DATA_LEN = 256;
    
    public AlignedRepo(Repository base, int pageLen) {
        if(pageLen < META_DATA_LEN) {
            throw new UnsupportedOperationException("Page length must be at least " + META_DATA_LEN);
        }
        this.base = base;
        this.pageLen = pageLen;
        this.init(this.base, pageLen);
    }

    @Override
    public void read(long at, byte[] buf) {
        if(at % this.pageLen != 0 || buf.length % this.pageLen != 0) {
            throw new UnsupportedOperationException("Read " 
                    + buf.length + " at " + at
                    + " is unaligned with page length " + this.pageLen);
        }        
        this.base.read(at, buf);
        if(at < META_DATA_LEN) {
            Arrays.fill(buf, 0, META_DATA_LEN - (int) at, (byte) 0);
        }
    }

    @Override
    public void write(long at, byte[] buf) {
        if(at % this.pageLen != 0 || buf.length % this.pageLen != 0) {
            throw new UnsupportedOperationException("Write " 
                    + buf.length + " at " + at
                    + " is unaligned with page length " + this.pageLen);
        }
        
        if(at + buf.length >= this.size) {
            synchronized(this) {
                this.syncMetaData(at + buf.length);
            }
        }
        if(at == 0) {
            System.arraycopy(this.metaData.array(), 0, buf, 0, META_DATA_LEN);
            this.base.write(at, buf);
            Arrays.fill(buf, 0, META_DATA_LEN, (byte) 0);
            return;
        }
        this.base.write(at, buf);
    }

    @Override
    public void close() {
        try(Repository repo = this.base){
            this.syncMetaData(this.size);
        }
    }
    
    protected void syncMetaData(long size) {
        this.size = size;
        this.metaData.putLong(OFFSET_SIZE, this.size);
        this.base.write(0, this.metaData.array());
    }
    
    private void init(Repository repo, int pageLen) {
        this.metaData = ByteBuffer.wrap(new byte[META_DATA_LEN]);
        repo.read(0, this.metaData.array());
        
        if(this.metaData.getInt(OFFSET_SIGN) == SIGNATURE) {
            this.pageLen = this.metaData.getInt(OFFSET_PAGE_LEN);
            if(this.pageLen != pageLen) {
                throw new IllegalArgumentException("Page length mismatch. Expected " 
                        + pageLen + ", actual " + this.pageLen);
            }
            this.size = this.metaData.getLong(OFFSET_SIZE);
            if(this.size < 0){
                throw new IllegalArgumentException("Invalid current size "
                        + this.size);
            }
            return;
        }
        
        this.metaData.putInt(OFFSET_SIGN, SIGNATURE);
        this.pageLen = pageLen;
        this.metaData.putInt(OFFSET_PAGE_LEN, this.pageLen);
        this.size = 0L;
        this.metaData.putLong(OFFSET_SIZE, this.size);
        repo.write(0, this.metaData.array());
    }
    
    private Repository base;
    private int pageLen;
    private volatile long size;
    private ByteBuffer metaData;
    
    protected static final int OFFSET_SIGN = 0;
    
    protected static final int OFFSET_PAGE_LEN = OFFSET_SIGN + 4;
    
    protected static final int OFFSET_SIZE = OFFSET_PAGE_LEN + 4;
    
    protected static final int SIGNATURE = 305992181;        
}
