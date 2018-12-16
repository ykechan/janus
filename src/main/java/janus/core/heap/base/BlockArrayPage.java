/*
 * The MIT License
 *
 * Copyright 2018 Y.K. Chan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package janus.core.heap.base;

import janus.core.heap.Page;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 *
 * @author Y.K. Chan
 */
public class BlockArrayPage implements Page {

    public BlockArrayPage(long addr, List<byte[]> arrayList, BiConsumer<Long, byte[]> flush) {        
        this.addr = addr;
        this.arrayList = arrayList;
        this.flush = flush;
        this.blockLen = arrayList.stream()
                .mapToInt(b -> b.length)
                .reduce((a, b) -> {
                    if(a != b){
                        throw new IllegalArgumentException("Block length mismatch.");
                    }
                    return a;
                }) 
                .orElseThrow(() -> new IllegalArgumentException());
        this.pageLen = arrayList.size() * arrayList.get(0).length;
        this.dirty = new boolean[arrayList.size()];
        Arrays.fill(this.dirty, false);
    }
    
    @Override
    public long address() {
        return this.addr;
    }

    @Override
    public int length() {
        return this.pageLen;
    }

    @Override
    public void write(int at, byte[] buf, int off, int len) {
        if(at + len > this.pageLen){
            throw new ArrayIndexOutOfBoundsException("Range [" + at + "," + (at + len) +") is out of bound "
                + this.pageLen);
        }
        int done = 0;
        while(done < len){
            int pos = at + done;
            int block = pos / this.blockLen;
            int start = pos % this.blockLen;
            
            int limit = Math.min(len - done, this.blockLen - start); 
            System.arraycopy(buf, off + done, this.arrayList.get(block), start, limit);
            done += limit;
            
            dirty[block] = true;
        }
    }

    @Override
    public void read(int at, byte[] buf, int off, int len) {
        int done = 0;
        while(done < len){
            int pos = at + done;
            int block = pos / this.blockLen;
            int start = pos % this.blockLen;
            
            int limit = Math.min(len - done, this.blockLen - start); 
            System.arraycopy(this.arrayList.get(block), start, buf, off + done, limit);
            done += limit;            
        }
    }

    @Override
    public void commit() {
        for(int i = 0; i < this.dirty.length; i++){
            if(this.dirty[i]){
                this.flush.accept(this.addr + i * this.blockLen, this.arrayList.get(i));
            }
        }
    }

    @Override
    public void close() {
        
    }

    private long addr;
    private int pageLen, blockLen;
    private List<byte[]> arrayList;
    private boolean[] dirty;
    private BiConsumer<Long, byte[]> flush;
}
