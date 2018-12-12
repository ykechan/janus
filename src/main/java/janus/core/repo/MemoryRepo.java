/*
 * MIT License
 * 
 * Copyright (c) 2018 Y.K. Chan
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 */
package janus.core.repo;

import java.util.Arrays;

/**
 * In memory repository.
 * 
 * @author Y.K. Chan
 *
 */
public class MemoryRepo implements Repository {
    
    /**
     * Constructor.
     */
    public MemoryRepo() {
        this(new byte[DEFAULT_INIT_LEN]);
    }
    
    /**
     * Constructor.
     * @param array  Backing array
     */
    protected MemoryRepo(byte[] array) {
        this.array = array;
        this.step = DEFAULT_INIT_STEP;
    }

    @Override
    public void read(long at, byte[] buf) {
        int length = at + buf.length > this.array.length 
                ? this.array.length - (int) at 
                : buf.length;
        if(length > 0) {
            System.arraycopy(this.array, (int) at, buf, 0, length);
        }
    }

    @Override
    public void write(long at, byte[] buf) {
        this.ensureCapacity((int) at + buf.length);
        System.arraycopy(buf, 0, this.array, (int) at, buf.length);
    }

    @Override
    public void close() {
        this.array = null;
    }
    
    /**
     * Ensure have enough capacity to a given target.
     * @param target  Target capacity
     */
    protected void ensureCapacity(int target) {
        if(this.array.length >= target) {
            return;
        }
        int capacity = this.array.length;
        while(capacity < target) {
            capacity += this.step;
            this.step += 2;
        }
        capacity += capacity % DEFAULT_ALIGN == 0 ? 0 : DEFAULT_ALIGN - (capacity % DEFAULT_ALIGN);
        this.array = Arrays.copyOf(this.array, capacity);
    }

    private byte[] array;
    private int step;
    
    /**
     * Default initial array size
     */
    protected static final int DEFAULT_INIT_LEN = 64;
    
    /**
     * Default initial increase amount.
     */
    protected static final int DEFAULT_INIT_STEP = 17;
    
    /**
     * Default value for memory alignment
     */
    protected static final int DEFAULT_ALIGN = 8;
}
