package janus.core.page;

import java.util.Arrays;

public class Segments implements Page {

    public Segments(long addr, byte[] buffer, int segLen, Flush flush) {
        if(buffer == null || buffer.length % segLen > 0) {
            throw new IllegalArgumentException();
        }
        this.buffer = buffer;
        this.addr = addr; 
        this.dirty = new boolean[buffer.length / segLen];
        Arrays.fill(this.dirty, false);
        this.flush = flush;
    }

    @Override
    public long address() {
        return this.addr;
    }

    @Override
    public int length() {
        return this.buffer.length;
    }

    @Override
    public void write(int at, byte[] data, int off, int len) {
        int segLen = this.buffer.length / this.dirty.length;
        int begin = at / segLen;
        int end = at + len;
        for(int i = begin; i * segLen < end; i++) {
            dirty[i] = true;
        }
        System.arraycopy(data, off, this.buffer, at, len);
    }

    @Override
    public void read(int at, byte[] data, int off, int len) {
        System.arraycopy(this.buffer, at, data, off, len);
    }
    
    @Override
    public void close() {
        int segLen = this.buffer.length / this.dirty.length;
        for(int i = 0; i < this.dirty.length; i++) {
            if(!this.dirty[i]) {
                continue;
            }
            this.flush.commit(this.address() + i * segLen, this.buffer, i * segLen, segLen);
            this.dirty[i] = false;
        }
    }

    private byte[] buffer;    
    private long addr;
    private boolean[] dirty;
    private Flush flush;
    
    @FunctionalInterface
    public interface Flush {
        
        public void commit(long at, byte[] data, int off, int len);
        
    }
}
