package janus.core.repository;

import java.nio.ByteBuffer;
import java.util.Random;

import janus.core.page.Page;
import janus.core.storage.MemoryStorage;
import janus.core.storage.Storage;
import janus.core.util.SizeOf;
import org.junit.Assert;
import org.junit.Test;

public class ExpandOnlyRepoTest {
    
    @Test
    public void shouldBeAbleToInitialize() {
        ByteBuffer image = ByteBuffer.wrap(this.afterTask(storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 1024, 1)){
                
            }
        }));
        
        Assert.assertEquals(ExpandOnlyRepo.FLAG, image.getInt(0));
        Assert.assertEquals(1024, image.getInt(SizeOf.INT.length));
        Assert.assertEquals(0L, image.getLong(2 * SizeOf.INT.length));
    }
    
    @Test
    public void shouldBeAbleToExpandWithPageLength() {
        ByteBuffer image = ByteBuffer.wrap(this.afterTask(storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 1024, 1)){
                Assert.assertEquals(0, repo.alloc().address());
                Assert.assertEquals(1024, repo.alloc().address());
                Assert.assertEquals(2048, repo.alloc().address());
            }
        }));
        
        Assert.assertEquals(3 * 1024L, image.getLong(2 * SizeOf.INT.length));
    }
    
    @Test
    public void shouldSyncSizeAfterEachAllocation() {
        byte[] mem = new byte[4096];
        ByteBuffer image = ByteBuffer.wrap(this.afterTask(mem, storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 1024, 1)){
                Assert.assertEquals(0, repo.alloc().address());
                Assert.assertEquals(1 * 1024L, ByteBuffer.wrap(mem).getLong(2 * SizeOf.INT.length));
                
                Assert.assertEquals(1024, repo.alloc().address());
                Assert.assertEquals(2 * 1024L, ByteBuffer.wrap(mem).getLong(2 * SizeOf.INT.length));
                
                Assert.assertEquals(2048, repo.alloc().address());
                Assert.assertEquals(3 * 1024L, ByteBuffer.wrap(mem).getLong(2 * SizeOf.INT.length));
            }
        }));                
        
        Assert.assertEquals(3 * 1024L, image.getLong(2 * SizeOf.INT.length));
    }
    
    @Test
    public void shouldBeAbleToLinkPageToStorage() {
        String str = "Pecunia non olet.";
        ByteBuffer image = ByteBuffer.wrap(this.afterTask(storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 1024, 1)){
                try(Page page = repo.alloc()){
                    Assert.assertEquals(0L, page.address());
                    page.write(0, str.getBytes());
                }
                
                try(Page page = repo.fetch(0)){
                    Assert.assertEquals(0L, page.address());
                    Assert.assertEquals(str, new String(page.read(0, str.length())));
                }
            }
        }));
    }
    
    @Test
    public void shouldBeAbleToReOpenRepo() {
        String str = "Pecunia non olet.";
        byte[] data = this.afterTask(storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 1024, 1)){
                try(Page page = repo.alloc()){
                    Assert.assertEquals(0L, page.address());
                    page.write(0, str.getBytes());
                }                               
            }
        });
        this.afterTask(data, storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 1024, 1)){
                try(Page page = repo.fetch(0)){
                    Assert.assertEquals(0L, page.address());
                    Assert.assertEquals(str, new String(page.read(0, str.length())));
                }                               
            }
        });
    }
    
    @Test
    public void shouldBeAbleToReOpenRepoWithDifferentBlockLength() {
        String str = "Pecunia non olet.";
        byte[] data = this.afterTask(storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 1024, 1)){
                try(Page page = repo.alloc()){
                    Assert.assertEquals(0L, page.address());
                    page.write(0, str.getBytes());
                }                               
            }
        });
        this.afterTask(data, storage -> {
            try(ExpandOnlyRepo repo = new ExpandOnlyRepo(storage, 256, 4)){
                try(Page page = repo.fetch(0)){
                    Assert.assertEquals(0L, page.address());
                    Assert.assertEquals(str, new String(page.read(0, str.length())));
                }                               
            }
        });
    }
    
    @Test
    public void shouldBeAbleToWriteBlobAcrossPages() {
        
    }
    
    protected byte[] afterTask(Task task) {
        return this.afterTask(null, task);
    }
    
    protected byte[] afterTask(byte[] buffer, Task task) {
        try(MemoryStorage storage = new MemoryStorage(buffer)){
            task.run(storage);
            return storage.getBytes();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }        
    }
    
    protected byte[] blob(int size) {
        Random rand = new Random(size ^ Double.doubleToLongBits(Math.PI));
        byte[] data = new byte[size];
        
        return data;
    }
    
    @FunctionalInterface
    public interface Task {
        
        public void run(Storage storage) throws Exception;
        
    }

}
