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

/**
 * Common interface for Data storage.
 * 
 * <p>
 * This is an abstraction of a data storage with infinite capacity. Data canbe read/write at
 * any location. Such operation may not succeed however. In case of failure an UnsupportedOperationException
 * is thrown.
 * </p>
 * 
 * <p>Implementations in general are NOT thread-safe.</p>
 * 
 * @author Y.K. Chan
 *
 */
public interface Repository extends AutoCloseable {
    
    /**
     * Read data starting at the given position. The amount of data read is equals to the size
     * of the buffer.
     * @param at  Start position
     * @param buf  Data buffers
     */
    public void read(long at, byte[] buf);
    
    /**
     * Write data starting at the given position. The amount of data written is equals to the size
     * of the buffer.
     * @param at  Start position
     * @param buf  Data buffers
     */
    public void write(long at, byte[] buf);
    
    /**
     * Close this repository. 
     * Any operation performed on a closed repository will result in an exception.
     */
    public void close();

}
