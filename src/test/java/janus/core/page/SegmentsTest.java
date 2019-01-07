package janus.core.page;

import java.util.concurrent.atomic.AtomicInteger;

import janus.core.page.Segments.Flush;
import org.junit.Assert;
import org.junit.Test;

public class SegmentsTest {
    
    @Test
    public void shouldBeAbleToReadAndWriteData() {
        String str = "This is a test string for shouldBeAbleToReadAndWriteData.";
        try(Page page = new Segments(Short.MAX_VALUE, new byte[1024], 1, this::mock)){
           page.write(0, str.getBytes());
           Assert.assertEquals(str, new String(page.read(0, str.length())));
        } 
    }
    
    @Test
    public void shouldFlushOnlyDirtySegments() {
        String str = "0123456789012345678901234567890123456789";
        AtomicInteger count = new AtomicInteger(0);
        Flush flush = (at, data, off, len) -> {
            Assert.assertTrue("Invalid flush position " + at, at == 0 || at == 16 || at == 32);
            count.incrementAndGet();
        };
        try(Page page = new Segments(0, new byte[32 * 16], 16, flush)){
           page.write(0, str.getBytes());
           Assert.assertEquals(str, new String(page.read(0, str.length())));
        }
        Assert.assertEquals(3, count.get());
    }
    
    @Test
    public void shouldClearDirtyFlagAfterFlush() {
        String str = "0123456789012345678901234567890123456789";
        AtomicInteger count = new AtomicInteger(0);
        Flush flush = (at, data, off, len) -> {
            Assert.assertTrue("Invalid flush position " + at, at == 0 || at == 16 || at == 32);
            count.incrementAndGet();
        };
        try(Page page = new Segments(0, new byte[32 * 16], 16, flush)){
           page.write(0, str.getBytes());
           page.close();
           Assert.assertEquals(3, count.get());
        }
        Assert.assertEquals(3, count.get());
    }

    protected void mock(long at, byte[] data, int off, int len) {
        
    }

}
