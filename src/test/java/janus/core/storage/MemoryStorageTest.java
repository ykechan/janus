package janus.core.storage;

import java.io.File;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class MemoryStorageTest {
    
    @Test
    public void shouldBeAbleToReadAndWriteData() throws Exception {
        String str = "This is a test string.";
        
        try(MemoryStorage storage = new MemoryStorage()){
            storage.write(0, str.getBytes());            
            Assert.assertArrayEquals(str.getBytes(), storage.read(0, str.length()));
            Assert.assertArrayEquals(str.getBytes(), storage.getBytes());
        }                
    }
    
    @Test
    public void shouldBeAbleToWriteBeyondEnd() throws Exception {
        String str = "This is a test string.";
        
        long mega = 1024 * 1024L;
        
        try(MemoryStorage storage = new MemoryStorage()){
            storage.write(mega, str.getBytes());
            Assert.assertArrayEquals(str.getBytes(), storage.read(mega, str.length()));
            Assert.assertEquals(mega + str.length(), storage.getBytes().length);
        }
        
    }
    
    @Test
    public void shouldBeAbleToReadAndWriteDataFragment() throws Exception {
        String str = "01234This is a test string.56789";
        
        try(Storage storage = new MemoryStorage()){
            storage.write(0, str.getBytes(), 5, str.length() - 10);
            Assert.assertArrayEquals(
                "This is a test string.".getBytes(), 
                storage.read(0, str.length() - 10));
        }
        
    }
    
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldNotBeAbleToReadBeyondEof() throws Exception {
        String str = "This is a test string.";
        
        long mega = 1024 * 1024L;
        
        try(Storage storage = new MemoryStorage()){
            storage.write(0, str.getBytes());
            Assert.assertArrayEquals(str.getBytes(), storage.read(mega, str.length()));
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void shouldNotBeAbleToWriteNegativePositon() throws Exception {
        try(Storage storage = new MemoryStorage()){
            storage.write(-1024, "0123456789".getBytes());
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void shouldNotBeAbleToWriteReadPositon() throws Exception {
        try(Storage storage = new MemoryStorage()){
            //storage.write(0, "0123456789".getBytes());
            storage.read(-1024, 10);
        }
    }

}
