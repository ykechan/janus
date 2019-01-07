package janus.core.repository;

import java.io.Serializable;
import java.util.Set;

import janus.core.page.Page;
import janus.core.util.SizeOf;

public class RecyclePageRepo implements Repository {    

    @Override
    public Page alloc() {
        return null;
    }

    @Override
    public void free(Page page) {
        // ...
    }

    @Override
    public Page fetch(long address) {
        return this.repo.fetch(address);
    }
    
    @Override
    public void close() throws Exception {
        try {
            this.longSet.close();
        } finally {
            this.repo.close();
        }
    }

    private Repository repo; 
    private LongSet longSet;
    
    protected interface LongSet extends AutoCloseable {
        
        public boolean isEmpty();
        
        public boolean isFull();
        
        public void push(long addr);
        
        public long pop();
        
    }
    
    protected static class SingleNodeSet implements LongSet, Serializable {
        
        /**
         * 
         */
        public static final long serialVersionUID = 3861068807341562878L;
        
        public static final int FLAG = 102360894;
        
        public SingleNodeSet(Page page, Set<Long> set) {
            this.page = page;
            this.set = set;
        }

        @Override
        public boolean isEmpty() {
            return this.set.isEmpty();
        }

        @Override
        public boolean isFull() {
            int capacity = (this.page.length() - HEADER_LEN) / SizeOf.LONG.length;
            return capacity <= this.set.size();
        }

        @Override
        public void push(long addr) {
            if(addr <= 0){
                throw new IllegalArgumentException("Unable to store address " + addr);
            }
            if(this.isFull()) {
                throw new UnsupportedOperationException();
            }
            
            this.set.add(addr);
        }

        @Override
        public long pop() {
            long addr = this.set.iterator().next();
            this.set.remove(addr);
            // ...
            return addr;
        }
        
        @Override
        public void close() throws Exception {
            this.page.close();
        }
        
        private Page page;
        private Set<Long> set; 
        
        protected static final int HEADER_LEN = SizeOf.struct(SizeOf.INT, SizeOf.INT);
    }
}
