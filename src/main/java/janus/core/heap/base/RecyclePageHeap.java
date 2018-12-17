package janus.core.heap.base;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import janus.core.heap.Heap;
import janus.core.heap.Page;
import janus.core.heap.Session;
import janus.core.util.SizeOf;

public class RecyclePageHeap implements Heap {         

    public RecyclePageHeap(Heap base, long root, int cacheLimit) {
        this.base = base;
        this.root = root;
        this.cacheLimit = cacheLimit;
        this.open(root);
    }

    @Override
    public Session newSession() {
        return new Session() {

            @Override
            public Page alloc() {
                return pageAlloc();
            }

            @Override
            public void free(Page page) {
                freePages(Collections.singletonList(page.address()));
            }

            @Override
            public Page fetch(long addr) {
                return session.fetch(addr);
            }

            @Override
            public void commit() {
                // rollback not supported
            }

            @Override
            public void close() {
                // 
            }
                        
        };
    }
    
    @Override
    public void close() throws Exception {
        try(Session sess = this.session){
            this.flush(sess, root);
        } finally {
            this.base.close();
        }
    }
    
    protected void open(long root) {
        this.session = this.base.newSession();
        try(Page page = this.session.fetch(root)){
            ByteBuffer header = ByteBuffer.wrap(new byte[HEADER_LEN]);
            page.read(0, header.array(), 0, HEADER_LEN);
            
            if(header.getInt(0) != FLAG) {
                this.init(page);
                return;
            }
            
            int count = header.getInt(IDX_COUNT);
            long next = header.getLong();
            if(count <= 0 && next > 0) {
                // not gracefully closed
            }
            
            
        }
    }
    
    protected void init(Page page) {
        byte[] header = new byte[HEADER_LEN];
        ByteBuffer buf = ByteBuffer.wrap(header);
        buf.putInt(0, FLAG);
        buf.putLong(SizeOf.INT.length, -1L);
        buf.putInt(SizeOf.INT.length + SizeOf.LONG.length, 0);
        page.write(0, header, 0, header.length);
        this.cache = new ArrayList<>();
        page.commit();
    }
    
    protected void flush(Session session, long root) {
        byte[] data = new byte[this.cache.size() * SizeOf.LONG.length];
        ByteBuffer.wrap(data).asLongBuffer()
            .put(this.cache.stream().mapToLong(v -> v).toArray());
        
        int done = 0;
        try(Page page = session.fetch(root)){
            ByteBuffer header = ByteBuffer.wrap(new byte[HEADER_LEN]);
            page.read(0, header.array(), 0, HEADER_LEN);
            int capacity = (page.length() - HEADER_LEN) / SizeOf.LONG.length;
            while(done < this.cache.size()) {
                int count = header.getInt(IDX_COUNT);
                if(count < capacity) {                   
                    int num = Math.min(capacity - count, this.cache.size() - done);                    
                    page.write(HEADER_LEN + count * SizeOf.LONG.length, 
                            data, done * SizeOf.LONG.length, num * SizeOf.LONG.length);
                    header.putInt(IDX_COUNT, count + num);                    
                    done += num;
                    continue;
                }
                long next = this.cache.get(count++);
                try(Page nextPage = session.fetch(next)){
                    byte[] temp = new byte[page.length()];
                    page.read(0, temp, 0, temp.length);
                    nextPage.write(0, temp, 0, temp.length);
                    header.putInt(IDX_COUNT, 0);
                    header.putLong(IDX_NEXT, next);
                    nextPage.commit();
                }
                done++;
            }
            page.write(0, header.array(), 0, HEADER_LEN);
            page.commit();
        }
        
        this.cache.clear();
    }

    protected int loadCache() {
        try(Page page = this.session.fetch(this.root)){
            ByteBuffer header = ByteBuffer.wrap(new byte[HEADER_LEN]);
            page.read(0, header.array(), 0, HEADER_LEN);
            
            int count = header.getInt(IDX_COUNT);
            int limit = Math.min(this.cacheLimit, count);
            
            int offset = HEADER_LEN + (count - limit) * SizeOf.LONG.length;
            byte[] data = new byte[limit * SizeOf.LONG.length];
            page.read(offset, data, 0, data.length);
            
            header.putInt(IDX_COUNT, count - limit);
            page.write(0, header.array(), 0, HEADER_LEN);
            page.commit();
        }
        return this.cache.size();
    }
    
    protected void freePages(Collection<Long> addresses) {        
        this.cache.addAll(addresses);
        if(this.cache.size() > this.cacheLimit){
            this.flush(session, root);
            this.cache.clear();
        }
    }
    
    protected Page pageAlloc() {
        if(this.cache.isEmpty()) {
            return this.session.alloc();
        }
        long last = this.cache.get(this.cache.size() - 1);
        this.cache.remove(this.cache.size() - 1);
        return session.fetch(last);
    }

    private Heap base;
    private Session session;
    private long root;
    private int cacheLimit;
    private List<Long> cache;
    
    protected static final int FLAG = -1292463666;        
    
    protected static final int HEADER_LEN = 2 * SizeOf.INT.length + SizeOf.LONG.length;
    
    protected static final int IDX_NEXT = SizeOf.INT.length;
    
    protected static final int IDX_COUNT = SizeOf.INT.length + SizeOf.LONG.length;
}
