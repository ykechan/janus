package janus.core.util;

import java.util.Arrays;

public enum SizeOf {        
    
    SHORT(2), 
    INT(4), 
    LONG(8),
    BLOCK(4096);
    
    private SizeOf(int length) {
        this.length = length;
    }
    
    public static int struct(SizeOf... args) {
        return Arrays.stream(args).mapToInt(f -> f.length).sum();
    }

    public final int length;
}
