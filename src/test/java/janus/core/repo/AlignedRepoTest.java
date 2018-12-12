package janus.core.repo;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class AlignedRepoTest {
    
    @Test
    public void shouldBeAbleToPerformStandardReadWrite() {
        try(Repository repo = new AlignedRepo(new MemoryRepo(), 1024)){
            byte[] buf = new byte[1024];
            for(int i = AlignedRepo.META_DATA_LEN; i < buf.length; i++){
                buf[i] = (byte) (i % 256);
            }
            
            repo.write(0, buf);
            
            byte[] temp = new byte[buf.length];
            repo.read(0, temp);
            for(int i = AlignedRepo.META_DATA_LEN; i < buf.length; i++){
                Assert.assertEquals((byte) (i % 256), temp[i]);
            }
        }
    }
    
    @Test
    public void shouldNotOverwriteMetaDataRegion() {
        byte[] mem = new byte[4096];
        try(Repository repo = new AlignedRepo(new MemoryRepo(mem), 1024)){
            byte[] buf = new byte[1024];
            for(int i = 0; i < buf.length; i++){
                buf[i] = (byte) (i % 256);
            }
            
            repo.write(0, buf);            
        }
        ByteBuffer buf = ByteBuffer.wrap(mem);
        //System.out.println(buf.getInt(0));
        Assert.assertEquals(AlignedRepo.SIGNATURE, buf.getInt(AlignedRepo.OFFSET_SIGN));
        Assert.assertEquals(1024, buf.getInt(AlignedRepo.OFFSET_PAGE_LEN));
        Assert.assertEquals(1024, buf.getLong(AlignedRepo.OFFSET_SIZE));
    }

}
