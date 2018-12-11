package janus.core.repo;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class MemoryRepoTest {
    
    @Test
    public void shouldBeAbleToDoStandardReadAndWrite() {
        byte[] str = "This is a testing string.".getBytes();
        try(Repository repo = new MemoryRepo()){
            int len = str.length;
            repo.write(0, str);
            byte[] buf = new byte[len];
            repo.read(0, buf);
            
            Assert.assertArrayEquals(str, buf);
        }
    }
    
    @Test
    public void shouldBeAbleToExpandDynamically() {
        byte[] str = "Test !@#$%.".getBytes();
        try(Repository repo = new MemoryRepo()){            
            for(int i = 0; i < 1024; i++) {                
                repo.write(i * str.length, str);
                byte[] tmp = new byte[str.length];
                repo.read(i * str.length, tmp);
                Assert.assertArrayEquals(str, tmp);
            }
        }
    }
    
    @Test
    public void shouldBeAbleToExpandWithGapInWrite() {
        byte[] str = "It's my way, or the highway.".getBytes();
        try(Repository repo = new MemoryRepo()){
            repo.write(0, str);
            repo.write(1024, str);
            
            byte[] tmp = new byte[str.length];
            repo.read(1024, tmp);
            
            Assert.assertArrayEquals(str, tmp);
        }
    }
    
    @Test
    public void shouldBeAbleToReadUnwrittenRegion() {
        byte[] str = "It's my way, or the highway.".getBytes();
        try(Repository repo = new MemoryRepo()){
            byte[] tmp = new byte[str.length];
            repo.read(1024, tmp);
            
            Assert.assertArrayEquals(new byte[tmp.length], tmp);
        }
    }
    
    @Test
    public void shouldBeAbleToReadOverlapsWithOutOfBoundRegion() {
        byte[] str = "It's my way, or the highway.".getBytes();
        String lower = "or the highway.";
        try(Repository repo = new MemoryRepo()){
            byte[] tmp = new byte[str.length];
            repo.write(1024, str);
            repo.read(1024 + (str.length - lower.length()), tmp);
            
            Assert.assertArrayEquals(lower.getBytes(), Arrays.copyOf(tmp, lower.length()));
        }
    }

}
