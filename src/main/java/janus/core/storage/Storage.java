package janus.core.storage;

public interface Storage extends AutoCloseable {    
    
    public default void write(long at, byte[] data) {
        this.write(at, data, 0, data.length);
    }
    
    public default byte[] read(long at, int len) {
        byte[] data = new byte[len];
        this.read(at, data);
        return data;
    }
    
    public default void read(long at, byte[] data) {
        this.read(at, data, 0, data.length);
    }
    
    public void write(long at, byte[] data, int off, int len);
    
    public void read(long at, byte[] data, int off, int len);

}
