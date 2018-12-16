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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Y.K. Chan
 */
public class BlockArrayPageTest {
    
    @Test
    public void shouldBeAbleToReadAndWriteInAnyBlock() {
        byte[] str = "This is a string.".getBytes();
        try(Page page = new BlockArrayPage(1024L, Arrays.asList(new byte[4][256]), (ptr, data) -> {})){
            for(int i = 0; i < 4; i++){
                page.write(i * 256, str, 0, str.length);
            }
            for(int i = 0; i < 4; i++){
                byte[] result = new byte[str.length];
                page.read(i * 256, result, 0, result.length);
                Assert.assertArrayEquals(str, result);
            }
        }
    }
    
    @Test
    public void shouldBeAbleToReadAndWriteAtTailInAnyBlock() {
        byte[] str = "This is a string.".getBytes();
        try(Page page = new BlockArrayPage(1024L, Arrays.asList(new byte[4][256]), (ptr, data) -> {})){
            for(int i = 0; i < 4; i++){
                page.write((i + 1) * 256 - str.length, str, 0, str.length);
            }
            for(int i = 0; i < 4; i++){
                byte[] result = new byte[str.length];
                page.read((i + 1) * 256 - str.length, result, 0, result.length);
                Assert.assertArrayEquals(str, result);
            }
        }
    }
    
    @Test
    public void shouldBeAbleToReadAndWriteAcrossBlocks() {
        byte[] str = "This is a string.".getBytes();
        try(Page page = new BlockArrayPage(1024L, Arrays.asList(new byte[4][256]), (ptr, data) -> {})){
            page.write(250, str, 0, str.length);
            byte[] result = new byte[str.length];
            page.read(250, result, 0, result.length);
            Assert.assertArrayEquals(str, result);
        }
    }
    
    @Test
    public void shouldBeAbleToReadAndWriteWithLargeDataThanSpanAcrossBlocks() {
        byte[] str = ("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
                    + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
                    + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
                    + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
                    + "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=-")
                .getBytes();
        try(Page page = new BlockArrayPage(1024L, Arrays.asList(new byte[4][256]), (ptr, data) -> {})){
            page.write(250, str, 0, str.length);
            byte[] result = new byte[str.length];
            page.read(250, result, 0, result.length);
            Assert.assertArrayEquals(str, result);
        }
    }
    
    @Test
    public void shouldOnlyCommitDirtyBlocks() {
        byte[] str = "This is a string.".getBytes();
        AtomicInteger count = new AtomicInteger(0);
        try(Page page = new BlockArrayPage(1024L, Arrays.asList(new byte[4][256]), (ptr, data) -> {
            count.incrementAndGet();
            Assert.assertEquals(1024L, ptr.longValue());
        })){
            page.write(128, str, 0, str.length);
            byte[] result = new byte[str.length];
            page.read(128, result, 0, result.length);
            Assert.assertArrayEquals(str, result);
            
            page.commit();
        }
        Assert.assertEquals(1, count.get());
    }
    
    @Test
    public void shouldOnlyCommitTheDirty2ndBlock() {
        byte[] str = "This is a string.".getBytes();
        AtomicInteger count = new AtomicInteger(0);
        try(Page page = new BlockArrayPage(1024L, Arrays.asList(new byte[4][256]), (ptr, data) -> {
            count.incrementAndGet();
            Assert.assertEquals(1024L + 256L, ptr.longValue());
        })){
            page.write((256 + 512) / 2, str, 0, str.length);
            byte[] result = new byte[str.length];
            page.read((256 + 512) / 2, result, 0, result.length);
            Assert.assertArrayEquals(str, result);
            
            page.commit();
        }
        Assert.assertEquals(1, count.get());
    }
    
    @Test
    public void shouldOnlyCommitTheDirty3rdBlock() {
        byte[] str = "This is a string.".getBytes();
        AtomicInteger count = new AtomicInteger(0);
        try(Page page = new BlockArrayPage(1024L, Arrays.asList(new byte[4][256]), (ptr, data) -> {
            count.incrementAndGet();
            Assert.assertEquals(1024L + 2 * 256L, ptr.longValue());
        })){
            page.write(512, str, 0, str.length);
            byte[] result = new byte[str.length];
            page.read(512, result, 0, result.length);
            Assert.assertArrayEquals(str, result);
            
            page.commit();
        }
        Assert.assertEquals(1, count.get());
    }

}
