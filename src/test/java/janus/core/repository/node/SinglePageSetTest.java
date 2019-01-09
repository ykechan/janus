package janus.core.repository.node;

import java.nio.ByteBuffer;
import java.util.Arrays;

import janus.core.page.Page;
import janus.core.util.SizeOf;
import org.junit.Assert;
import org.junit.Test;

public class SinglePageSetTest {
    
    @Test
    public void shouldBeAbleToInitializeEmptyPage() {
        byte[] mem = new byte[1024];
        try(Page page = this.mockPage(Short.MAX_VALUE, mem)){
            LongSet set = new SinglePageSet(page, 0);
            Assert.assertTrue(set.isEmpty());
        }
        Assert.assertEquals(SinglePageSet.FLAG, ByteBuffer.wrap(mem).getInt(0));
    }
    
    @Test
    public void shouldBeAbleToAddItem() {
        byte[] mem = new byte[1024];
        try(Page page = this.mockPage(Short.MAX_VALUE, mem)){
            LongSet set = new SinglePageSet(page, 0);
            Assert.assertTrue(set.isEmpty());
            set.push(11L);
            set.push(13L);
            set.push(15L);
            set.push(17L);
            Assert.assertFalse(set.isEmpty());
            Assert.assertFalse(set.isFull());
        }
        Assert.assertEquals(SinglePageSet.FLAG, ByteBuffer.wrap(mem).getInt(0));
        Assert.assertEquals(4, ByteBuffer.wrap(mem).getInt(SizeOf.INT.length));
        
        long[] items = new long[4];
        ByteBuffer.wrap(mem, SinglePageSet.HEADER_LEN, mem.length - SinglePageSet.HEADER_LEN)
            .asLongBuffer()
            .get(items);
        Assert.assertArrayEquals(new long[]{11L, 13L, 15L, 17L}, items) ;
    }
    
    protected Page mockPage(long addr, byte[] mem) {        
        return new Page() {

            @Override
            public long address() {
                return addr;
            }

            @Override
            public int length() {
                return this.temp.length;
            }

            @Override
            public void write(int at, byte[] data, int off, int len) {
                System.arraycopy(data, off, temp, at, len);
            }

            @Override
            public void read(int at, byte[] data, int off, int len) {
                System.arraycopy(temp, at, data, off, len);
            }

            @Override
            public void close() {
                System.arraycopy(this.temp, 0, mem, 0, this.length());
            }
            
            private byte[] temp = Arrays.copyOf(mem, mem.length);
        };
    }

}
