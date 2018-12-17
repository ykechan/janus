package janus.core.heap.base;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

import janus.core.heap.Heap;
import janus.core.heap.Page;
import janus.core.heap.Session;
import janus.core.repo.FileRepo;
import janus.core.repo.MemoryRepo;
import janus.core.repo.Repository;
import org.junit.Assert;
import org.junit.Test;

public class ExpandOnlyHeapTest {
    
    @Test
    public void shouldBeAbleToInitializeRepo() throws Exception {
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession()){
            session.commit();
        }
        
        LongBuffer buf = ByteBuffer.wrap(mem).asLongBuffer();
        Assert.assertEquals(ExpandOnlyHeap.FLAG, buf.get(0));
        Assert.assertEquals(0L, buf.get(ExpandOnlyHeap.IDX_HEAP_SIZE));
        Assert.assertEquals(1024L, buf.get(ExpandOnlyHeap.IDX_PAGE_LEN));
    }
    
    @Test
    public void shouldFirstPageBeAtAddress0() throws Exception {
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.alloc()){
            Assert.assertEquals(0L, page.address());
            session.commit();
        }
        
        LongBuffer buf = ByteBuffer.wrap(mem).asLongBuffer();
        Assert.assertEquals(ExpandOnlyHeap.FLAG, buf.get(0));
        Assert.assertEquals(1024L, buf.get(ExpandOnlyHeap.IDX_HEAP_SIZE));
        Assert.assertEquals(1024L, buf.get(ExpandOnlyHeap.IDX_PAGE_LEN));
    }
    
    @Test
    public void shouldMetaDataRegionIsNotVisibleInFirstPage() throws Exception {
        byte[] str = "This is a test.".getBytes();
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.alloc()){
            Assert.assertEquals(1024L - ExpandOnlyHeap.META_DATA_LEN, page.length());
            page.write(0, str, 0, str.length);
            page.commit();
            session.commit();
        }
        
        LongBuffer buf = ByteBuffer.wrap(mem).asLongBuffer();
        Assert.assertEquals(ExpandOnlyHeap.FLAG, buf.get(0));
        Assert.assertEquals(1024L, buf.get(ExpandOnlyHeap.IDX_HEAP_SIZE));
        Assert.assertEquals(1024L, buf.get(ExpandOnlyHeap.IDX_PAGE_LEN));
        
        int len = ExpandOnlyHeap.META_DATA_LEN;
        Assert.assertArrayEquals(str, Arrays.copyOfRange(mem, len, len + str.length));
    }
    
    @Test
    public void shouldAllocatePageByAppendingPageToTail() throws Exception {
        byte[] str = "This is a test.".getBytes();
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.alloc();
            Page page1 = session.alloc()){
            
            Assert.assertEquals(1024L, page1.address());
            Assert.assertEquals(1024L, page1.length());
            
            page1.write(0, str, 0, str.length);
            
            page1.commit();
            session.commit();
        }int at = 1024;
        Assert.assertArrayEquals(str, Arrays.copyOfRange(mem, at, at + str.length));
    }
    
    @Test
    public void shouldWriteDataOnlyAfterPageIsCommitted() throws Exception {
        byte[] str = "This is a test.".getBytes();
        byte[] mem = new byte[4096];
        try(Repository repo = new MemoryRepo(mem);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.alloc();
            Page page1 = session.alloc()){
            
            Assert.assertEquals(1024L, page1.address());
            Assert.assertEquals(1024L, page1.length());
            
            page.write(0, str, 0, str.length);
            page1.write(0, str, 0, str.length);
            
            page1.commit();
            session.commit();
        }
        int at = ExpandOnlyHeap.META_DATA_LEN;
        Assert.assertArrayEquals(new byte[str.length], Arrays.copyOfRange(mem, at, at + str.length));
        at = 1024;
        Assert.assertArrayEquals(str, Arrays.copyOfRange(mem, at, at + str.length));
    }
    
    @Test
    public void shouldBeAbleToReOpenARepository() throws Exception {
        byte[] str = "Testing string 012345".getBytes();
        File tempFile = File.createTempFile("tmp", ".bin");
        tempFile.deleteOnExit();
        try(Repository repo = FileRepo.of(tempFile);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.alloc()){
            
            page.write(0, str, 0, str.length);
            page.commit();
        }
        try(Repository repo = FileRepo.of(tempFile);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.fetch(0)){
            
            byte[] result = new byte[str.length];
            page.read(0, result, 0, result.length);
            
            Assert.assertArrayEquals(str, result);
        }
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToOpenARepositoryWithDifferentPageLength() throws Exception {
        byte[] str = "Testing string 012345".getBytes();
        File tempFile = File.createTempFile("tmp", ".bin");
        tempFile.deleteOnExit();
        try(Repository repo = FileRepo.of(tempFile);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.alloc()){
            
            page.write(0, str, 0, str.length);
            page.commit();
        }
        try(Repository repo = FileRepo.of(tempFile);
            Heap heap = new ExpandOnlyHeap(repo, 256, 6)){
        }
    }
    
    @Test
    public void shouldBeAbleToReOpenARepositoryByDifferentBlockLength() throws Exception {
        byte[] str = "Testing string 012345".getBytes();
        File tempFile = File.createTempFile("tmp", ".bin");
        tempFile.deleteOnExit();
        try(Repository repo = FileRepo.of(tempFile);
            Heap heap = new ExpandOnlyHeap(repo, 256, 4);
            Session session = heap.newSession();
            Page page = session.alloc()){
            
            page.write(0, str, 0, str.length);
            page.commit();
        }
        try(Repository repo = FileRepo.of(tempFile);
            Heap heap = new ExpandOnlyHeap(repo, 1024, 1);
            Session session = heap.newSession();
            Page page = session.fetch(0)){
            
            byte[] result = new byte[str.length];
            page.read(0, result, 0, result.length);
            
            Assert.assertArrayEquals(str, result);
        }
    }

}
