package janus.core.heap.base;

import java.nio.ByteBuffer;

import janus.core.heap.Heap;
import janus.core.heap.Page;
import janus.core.heap.Session;
import janus.core.repo.MemoryRepo;
import janus.core.repo.Repository;
import janus.core.util.SizeOf;
import org.junit.Assert;
import org.junit.Test;

public class RecyclePageHeapTest {
    
    @Test
    public void shouldBeAbleToInitializeHeap() throws Exception {
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem); 
            Heap heap = this.newHeap(repo, 1024, 1);
            Session session = heap.newSession()){
            session.commit();
        }
        
        ByteBuffer buf = ByteBuffer.wrap(mem);
        Assert.assertEquals(RecyclePageHeap.FLAG, buf.getInt(ExpandOnlyHeap.META_DATA_LEN));
        Assert.assertEquals(-1L, buf.getLong(ExpandOnlyHeap.META_DATA_LEN + SizeOf.INT.length));
        Assert.assertEquals(0, buf.getInt(ExpandOnlyHeap.META_DATA_LEN 
                + SizeOf.INT.length 
                + SizeOf.LONG.length));
    }        
    
    @Test
    public void shouldBeAbleToFallBackToBaseAllocWhenNoRecycledPage() throws Exception {
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem); 
            Heap heap = this.newHeap(repo, 1024, 1);
            Session session = heap.newSession();
                
            Page page = session.alloc();
            Page page2 = session.alloc();
            ){
            
            Assert.assertEquals(1024L, page.address());
            Assert.assertEquals(2048L, page2.address());
            
            session.commit();
        }
    }
    
    @Test
    public void shouldBeFlushRecycledPageWhenClosing() throws Exception {
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem); 
            Heap heap = this.newHeap(repo, 1024, 16);
            Session session = heap.newSession();
                
            Page page = session.alloc();
            Page page2 = session.alloc();
            ){
            session.free(page);
            session.free(page2);
            
            session.commit();
        }
        
        ByteBuffer buf = ByteBuffer.wrap(mem);
        Assert.assertEquals(RecyclePageHeap.FLAG, buf.getInt(ExpandOnlyHeap.META_DATA_LEN));
        Assert.assertEquals(-1L, buf.getLong(ExpandOnlyHeap.META_DATA_LEN + SizeOf.INT.length));
        Assert.assertEquals(2, buf.getInt(ExpandOnlyHeap.META_DATA_LEN 
                + SizeOf.INT.length 
                + SizeOf.LONG.length));
        
        Assert.assertEquals(1024L, buf.getLong(
                ExpandOnlyHeap.META_DATA_LEN + RecyclePageHeap.HEADER_LEN));
        Assert.assertEquals(2048L, buf.getLong(
                ExpandOnlyHeap.META_DATA_LEN + RecyclePageHeap.HEADER_LEN + SizeOf.LONG.length));
    }
    
    private Heap newHeap(Repository repo, int pageLen, int cacheLimit) {
        Heap heap = new ExpandOnlyHeap(repo, pageLen / 4, 4);
        try(Session session = heap.newSession(); Page page = session.alloc()){
            return new RecyclePageHeap(heap, page.address(), cacheLimit);
        }
    }

}
