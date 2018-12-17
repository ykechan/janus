package janus.core.util;

public enum SizeOf {        
    
    SHORT(2), 
    INT(4), 
    LONG(8);
    
    private SizeOf(int length) {
        this.length = length;
    }

    public final int length;
}
