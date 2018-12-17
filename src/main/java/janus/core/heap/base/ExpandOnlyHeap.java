package janus.core.heap.base;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import janus.core.heap.Heap;
import janus.core.heap.Page;
import janus.core.heap.Session;
import janus.core.repo.Repository;

public class ExpandOnlyHeap implements Heap {
    
    public static final int META_DATA_LEN = 256;
    
    public ExpandOnlyHeap(Repository repo, int blockLen, int pageSpan) {
        if(blockLen < META_DATA_LEN) {
            throw new IllegalArgumentException("Invalid block length " + blockLen);
        }
        if(pageSpan < 1) {
            throw new IllegalArgumentException("Invalid page span " + pageSpan);
        }
        this.blockLen = blockLen;
        this.repo = repo;
        this.open(blockLen * pageSpan);
    }

    @Override
    public Session newSession() {
        return new Session() {

            @Override
            public Page alloc() {
                return makePage(expand(), Arrays.asList(new byte[pageLen / blockLen][blockLen]));
            }

            @Override
            public void free(Page page) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Page fetch(long addr) {
                return makePage(addr, swapIn(addr));
            }

            @Override
            public void commit() {
                // rollback not supported
            }

            @Override
            public void close() {
                // close not needed stateless session
            }
            
        };
    }
    
    @Override
    public void close() throws Exception {
        try { 
            this.metaValues.put(0, FLAG);
            this.metaValues.put(IDX_PAGE_LEN, this.pageLen);
            this.metaValues.put(IDX_HEAP_SIZE, this.size);            
            this.repo.write(0L, this.metaData);
        } finally {
            this.repo.close();
        }
    }
    
    protected Page makePage(long addr, List<byte[]> blocks) {
        Page page = new BlockArrayPage(addr, blocks, this::swapOut);
        if(addr == 0){
            // ignore meta data
            return new Page() {

                @Override
                public long address() {
                    return addr;
                }

                @Override
                public int length() {
                    return pageLen - META_DATA_LEN;
                }

                @Override
                public void write(int at, byte[] buf, int off, int len) {
                    page.write(META_DATA_LEN + at, buf, off, len);
                }

                @Override
                public void read(int at, byte[] buf, int off, int len) {
                    page.read(META_DATA_LEN + at, buf, off, len);
                }

                @Override
                public void commit() {
                    page.commit();
                }

                @Override
                public void close() {
                    page.close();
                }
                
            };
        }
        return page;
    }
    
    protected long expand() {
        synchronized(this.repo) {
            long ptr = this.size;
            this.size += this.pageLen;
            this.metaValues.put(IDX_HEAP_SIZE, this.size);
            this.repo.write(0, this.metaData);
            return ptr;
        }
    }
    
    protected List<byte[]> swapIn(long addr) {
        synchronized(this.repo) {
            if(addr + this.pageLen > this.size) {
                throw new IllegalArgumentException("Page " + addr + " is not allocated.");
            }
            byte[][] blocks = new byte[this.pageLen / this.blockLen][this.blockLen];
            for(int i = 0; i < blocks.length; i++){
                this.repo.read(addr + i * this.blockLen, blocks[i]);
            }
            if(addr == 0L){
                Arrays.fill(blocks[0], 0, META_DATA_LEN, (byte) 0x00);
            }
            return Arrays.asList(blocks);
        }
    }
    
    protected void swapOut(long addr, byte[] data) {
        synchronized(this.repo) {
            if(addr + data.length > this.size) {
                throw new IllegalArgumentException("Block " + addr + " is not allocated.");
            }
            if(addr == 0) {
                System.arraycopy(this.metaData, 0, data, 0, META_DATA_LEN);
            }
            this.repo.write(addr, data);
        }
    }
    
    protected final void open(int pageLen) {
        if(pageLen < META_DATA_LEN) {
            throw new IllegalArgumentException("Page length must be at least " + META_DATA_LEN 
                    + ". (Supplied " + pageLen + ")");
        }
        byte[] initial = new byte[META_DATA_LEN];
        this.repo.read(0, initial);
        LongBuffer buf = ByteBuffer.wrap(initial).asLongBuffer();
        if(buf.get(0) != FLAG){
            this.init(pageLen);
            return;
        }
        if(buf.get(IDX_PAGE_LEN) != pageLen) {
            throw new IllegalArgumentException("Page length mismatch. Expected " + pageLen
                    + ", found " + initial[1]);
        }
        this.pageLen = pageLen;
        this.size = buf.get(IDX_HEAP_SIZE);
        this.metaData = initial;
        this.metaValues = buf;
    }
    
    protected final void init(int pageLen) {
        this.metaData = new byte[META_DATA_LEN];
        this.metaValues = ByteBuffer.wrap(this.metaData).asLongBuffer();
        this.pageLen = pageLen;
        this.size = 0L;
        this.metaValues.put(new long[] {
            FLAG, pageLen, this.size, Instant.now().getEpochSecond()
        });
        this.repo.write(0, this.metaData);
    }

    private int blockLen;
    private int pageLen;
    private long size;
    private byte[] metaData;
    private LongBuffer metaValues;
    private Repository repo;
    
    protected static final long FLAG = 216948097202835185L; 
    
    protected static final int IDX_PAGE_LEN = 1;
    
    protected static final int IDX_HEAP_SIZE = 2;
}
