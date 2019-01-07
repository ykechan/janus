package janus.core.repository;

import janus.core.page.Page;

public interface Repository extends AutoCloseable {

    public Page alloc();
    
    public void free(Page page);
    
    public Page fetch(long address);
    
}
