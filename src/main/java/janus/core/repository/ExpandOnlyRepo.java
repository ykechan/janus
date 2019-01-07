package janus.core.repository;

import java.nio.ByteBuffer;

import janus.core.page.Page;
import janus.core.page.Segments;
import janus.core.storage.Storage;
import janus.core.util.SizeOf;

public class ExpandOnlyRepo implements Repository {
    
    public static final int FLAG = -1955238274;

    public ExpandOnlyRepo(Storage storage, int blockLen, int pageSpan) {
        this.storage = storage;
        this.blockLen = blockLen;
        this.pageSpan = pageSpan;
        this.init(blockLen, pageSpan);
    }

    @Override
    public Page alloc() {
        int pageLen = this.pageLength();
        long address = -1;
        synchronized(this) {
            address = this.size;            
            this.size += pageLen;
            this.commit();
        }
        return this.offset(
            new Segments(address, new byte[pageLen], this.blockLen, this.storage::write),
            address == 0L ? HEADER_LEN : 0);
    }

    @Override
    public void free(Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page fetch(long address) {
        byte[] buf = this.storage.read(address, this.pageLength());
        return this.offset(new Segments(address, buf, this.blockLen, this.storage::write),
            address == 0L ? HEADER_LEN : 0);
    }
    
    @Override
    public void close() throws Exception {
        this.storage.close();
    }       
    
    protected void commit() {
        ByteBuffer header = ByteBuffer.wrap(new byte[HEADER_LEN]);
        header.putInt(FLAG);
        header.putInt(this.blockLen * this.pageSpan);
        header.putLong(this.size);
        this.storage.write(0, header.array());
    }
    
    protected final void init(int blockLen, int pageSpan) {
        ByteBuffer header = ByteBuffer.wrap(this.storage.read(0, HEADER_LEN));
        if(header.getInt(0) != FLAG) {
            this.size = 0;
            this.commit();
            return;
        }
        
        if(header.getInt(SizeOf.INT.length) != blockLen * pageSpan) {
            throw new IllegalArgumentException("Page length mismatch. ("
                    + "Found " + header.getInt(SizeOf.INT.length)
                    + ", expected " + (blockLen * pageSpan));
        }
        
        this.size = header.getLong(2 * SizeOf.INT.length);
        int pageLen = blockLen * pageSpan;
        if(this.size % pageLen != 0) {
            throw new IllegalArgumentException("Invalid repository size. Size "
                    + this.size
                    + " is not a collection of " + pageLen + " page.");
        }
    }
    
    protected int pageLength() {
        return this.blockLen * this.pageSpan;
    }
    
    protected Page offset(Page base, int skip) {
        if(skip == 0) {
            return base;
        }
        return new Page() {

            @Override
            public long address() {
                return base.address();
            }

            @Override
            public int length() {
                return base.length() - skip;
            }

            @Override
            public void write(int at, byte[] data, int off, int len) {
                if(at < 0 || at >= base.length()) {
                    throw new IllegalArgumentException();
                }
                base.write(at + skip, data, off, len);
            }

            @Override
            public void read(int at, byte[] data, int off, int len) {
                if(at < 0 || at >= base.length()) {
                    throw new IllegalArgumentException();
                }
                base.read(at + skip, data, off, len);
            }

            @Override
            public void close() {
                base.close();
            }
            
        };
    }

    private Storage storage;
    private long size;
    private int blockLen, pageSpan;
    
    protected static final int HEADER_LEN = SizeOf.struct(SizeOf.INT, SizeOf.INT, SizeOf.LONG);
}
