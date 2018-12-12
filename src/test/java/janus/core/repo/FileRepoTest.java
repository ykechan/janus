package janus.core.repo;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class FileRepoTest {
    
    @Test
    public void shouldBeAbleToDoStandardReadAndWrite() throws IOException {
        File tempFile = File.createTempFile("tmp", ".bin");
        byte[] str = "This is a testing string.".getBytes();
        try(Repository repo = FileRepo.of(tempFile)){
            int len = str.length;
            repo.write(0, str);
            byte[] buf = new byte[len];
            repo.read(0, buf);
            
            Assert.assertArrayEquals(str, buf);
        } finally {
            tempFile.deleteOnExit();
        }
    }
    
    @Test
    public void shouldBeAbleToExpandDynamically() throws IOException {
        File tempFile = File.createTempFile("tmp", ".bin");
        byte[] str = "Test !@#$%.".getBytes();
        try(Repository repo = FileRepo.of(tempFile)){            
            for(int i = 0; i < 1024; i++) {                
                repo.write(i * str.length, str);
                byte[] tmp = new byte[str.length];
                repo.read(i * str.length, tmp);
                Assert.assertArrayEquals(str, tmp);
            }
        } finally {
            tempFile.deleteOnExit();
        }
    }
    
    @Test
    public void shouldBeAbleToExpandWithGapInWrite() throws IOException {
        File tempFile = File.createTempFile("tmp", ".bin");
        byte[] str = "It's my way, or the highway.".getBytes();
        try(Repository repo = FileRepo.of(tempFile)){
            repo.write(0, str);
            repo.write(1024, str);
            
            byte[] tmp = new byte[str.length];
            repo.read(1024, tmp);
            
            Assert.assertArrayEquals(str, tmp);
        } finally {
            tempFile.deleteOnExit();
        }
    }
    
    @Test
    public void shouldBeAbleToReadUnwrittenRegion() throws IOException {
        File tempFile = File.createTempFile("tmp", ".bin");
        byte[] str = "It's my way, or the highway.".getBytes();
        try(Repository repo = FileRepo.of(tempFile)){
            byte[] tmp = new byte[str.length];
            repo.read(1024, tmp);
            
            Assert.assertArrayEquals(new byte[tmp.length], tmp);
        } finally {
            tempFile.deleteOnExit();
        }
    }
    
    @Test
    public void shouldBeAbleToReadOverlapsWithOutOfBoundRegion() throws IOException {
        File tempFile = File.createTempFile("tmp", ".bin");
        byte[] str = "It's my way, or the highway.".getBytes();
        String lower = "or the highway.";
        try(Repository repo = FileRepo.of(tempFile)){
            byte[] tmp = new byte[str.length];
            repo.write(1024, str);
            repo.read(1024 + (str.length - lower.length()), tmp);
            
            Assert.assertArrayEquals(lower.getBytes(), Arrays.copyOf(tmp, lower.length()));
        } finally {
            tempFile.deleteOnExit();
        }
    }

}
