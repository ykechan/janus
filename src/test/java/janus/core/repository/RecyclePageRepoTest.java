package janus.core.repository;

import org.junit.Test;

public class RecyclePageRepoTest {
    
    @Test
    public void test() {
        System.out.println(RecyclePageRepo.SingleNodeSet.serialVersionUID % Integer.MAX_VALUE);
    }

}
