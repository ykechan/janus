package janus.core.repository.node;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import janus.core.page.Page;
import janus.core.util.SizeOf;

public class SinglePageSet implements LongSet, Serializable {

    private static final long serialVersionUID = 6810613454638879014L;
    
    public SinglePageSet(Page page, int limit) {
        this.page = page;
        this.set = this.open(page);
    }    

    @Override
    public boolean isEmpty() {
        return this.set.isEmpty();
    }

    @Override
    public boolean isFull() {
        int capacity = (this.page.length() - HEADER_LEN) / SizeOf.LONG.length;
        return this.set.size() >= capacity;
    }

    @Override
    public void push(long item) {
        if(item == this.page.address()){
            throw new IllegalArgumentException("Item reserved.");
        }
        if(item <= 0) {
            throw new IllegalArgumentException("Invalid item value " + item);
        }
        if(this.set.contains(item)) {
            throw new IllegalArgumentException("Duplicated item " + item);
        }
        this.set.add(item);
        this.log(item);
    }

    @Override
    public long pop() {
        if(this.isEmpty()) {
            return -1;
        }
        long item = this.set.first();
        this.set.remove(item);
        this.log(-item);
        return item;
    }
    
    protected void log(long entry) {
        int capacity = (page.length() - HEADER_LEN) / SizeOf.LONG.length;
        
        ByteBuffer header = ByteBuffer.wrap(page.read(0, HEADER_LEN));
        int count = header.getInt(SizeOf.INT.length);
        
        try(Page p = this.page){
            if(count < capacity) {            
                header.putInt(0, FLAG).putInt(SizeOf.INT.length, count + 1);
                p.write(0, header.array());
                p.write(
                    HEADER_LEN + count * SizeOf.LONG.length, 
                    ByteBuffer.wrap(new byte[SizeOf.LONG.length])
                        .putLong(entry)
                        .array()
                );                
                return;
            }
            
            header.putInt(0, FLAG).putInt(SizeOf.INT.length, this.set.size());
            p.write(0, header.array());
            p.write(
                HEADER_LEN + count * SizeOf.INT.length, 
                ByteBuffer.wrap(new byte[SizeOf.LONG.length])
                    .putLong(entry)                    
                    .array()
            );
        }
    }
    
    protected SortedSet<Long> open(Page page) {
        ByteBuffer header = ByteBuffer.wrap(page.read(0, HEADER_LEN));
        if(header.getInt(0) != FLAG){
            header.putInt(0, FLAG).putInt(SizeOf.INT.length, 0);
            page.write(0, header.array());
            return new TreeSet<>();
        }
        int count = header.getInt(SizeOf.INT.length);
        SortedSet<Long> items = new TreeSet<>();
        long[] entries = new long[count];
        ByteBuffer.wrap(page.read(HEADER_LEN, count * SizeOf.LONG.length))
            .asLongBuffer()
            .get(entries);
        for(long entry : entries) {
            if(entry > 0){
                items.add(entry);
            }else if(entry < 0) {
                items.remove(-entry);
            }else {
                throw new IllegalStateException("Invalid format.");
            }
        }
        return items;
    }
    
    private Page page;
    private SortedSet<Long> set;
    
    protected static final int HEADER_LEN = SizeOf.struct(SizeOf.INT, SizeOf.INT);
    
    public static final int FLAG = 638879014;
}
