package janus.core.storage;

import java.util.Arrays;

public class MemoryStorage implements Storage {    
    
    public static final int DEFAULT_SIZE = 64;
    
    protected static final int DEFAULT_STEP = 17;
    
    public MemoryStorage() {
        this(null);
    }

    public MemoryStorage(byte[] memory) {
        this.memory = memory == null ? new byte[DEFAULT_SIZE] : memory ;
        this.size = 0;
        this.step = DEFAULT_STEP;
    }

    @Override
    public synchronized void write(long at, byte[] data, int off, int len) {
        this.ensureCapacity((int) at + len);
        System.arraycopy(data, off, memory, (int) at, len);
        this.size = Math.max(this.size, (int) at + len);
    }

    @Override
    public synchronized void read(long at, byte[] data, int off, int len) {
        
        System.arraycopy(memory, (int) at, data, off, len);
    }
    
    @Override
    public void close() throws Exception {
        //this.memory = null;
    }
    
    public byte[] getBytes() {
        return Arrays.copyOf(this.memory, this.size);
    }
    
    protected void ensureCapacity(int target) {
        int capacity = this.memory.length;
        while(capacity < target) {
            capacity += this.step;
            this.step += 2;
        }
        if(capacity > this.memory.length) {
            this.memory = Arrays.copyOf(this.memory, capacity);
        }
    }

    private byte[] memory;
    private int size, step;
}
