package janus.core.storage;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class FileStorageTest {
    
    @Test
    public void shouldBeAbleToCreateFile() throws Exception {
        File file = new File("./" + UUID.randomUUID().toString() + ".tmp");
        Assert.assertFalse(file.exists());
        
        try(Storage storage = FileStorage.of(file)){
            
        }
        
        Assert.assertTrue(file.exists());
    }
    
    @Test
    public void shouldBeAbleToReadAndWriteData() throws Exception {
        File file = File.createTempFile("tmp", ".txt");
        
        String str = "This is a test string.";
        
        try(Storage storage = FileStorage.of(file)){
            storage.write(0, str.getBytes());
            Assert.assertArrayEquals(str.getBytes(), storage.read(0, str.length()));
        }
        
        Assert.assertEquals(str.length(), file.length());
    }
    
    @Test
    public void shouldBeAbleToWriteBeyondEof() throws Exception {
        File file = File.createTempFile("tmp", ".txt");
        
        String str = "This is a test string.";
        
        long mega = 1024 * 1024L;
        
        try(Storage storage = FileStorage.of(file)){
            storage.write(mega, str.getBytes());
            Assert.assertArrayEquals(str.getBytes(), storage.read(mega, str.length()));
        }
        
        Assert.assertEquals(mega + str.length(), file.length());
    }
    
    @Test
    public void shouldBeAbleToReadAndWriteDataFragment() throws Exception {
        File file = File.createTempFile("tmp", ".txt");
        
        String str = "01234This is a test string.56789";
        
        try(Storage storage = FileStorage.of(file)){
            storage.write(0, str.getBytes(), 5, str.length() - 10);
            Assert.assertArrayEquals(
                "This is a test string.".getBytes(), 
                storage.read(0, str.length() - 10));
        }
        
        Assert.assertEquals(str.length() - 10, file.length());
    }
    
    @Test(expected = IllegalStateException.class)
    public void shouldNotBeAbleToReadBeyondEof() throws Exception {
        File file = File.createTempFile("tmp", ".txt");
        
        String str = "This is a test string.";
        
        long mega = 1024 * 1024L;
        
        try(Storage storage = FileStorage.of(file)){
            storage.write(0, str.getBytes());
            Assert.assertArrayEquals(str.getBytes(), storage.read(mega, str.length()));
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void shouldNotBeAbleToWriteNegativePositon() throws Exception {
        File file = File.createTempFile("tmp", ".txt");
        
        try(Storage storage = FileStorage.of(file)){
            storage.write(-1024, "0123456789".getBytes());
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void shouldNotBeAbleToWriteReadPositon() throws Exception {
        File file = File.createTempFile("tmp", ".txt");
        
        try(Storage storage = FileStorage.of(file)){
            //storage.write(0, "0123456789".getBytes());
            storage.read(-1024, 10);
        }
    }

}
