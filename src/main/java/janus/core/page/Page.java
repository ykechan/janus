package janus.core.page;

public interface Page extends AutoCloseable {
    
    public long address();
    
    public int length();
    
    public default void write(int at, byte[] data) {
        this.write(at, data, 0, data.length);
    }
    
    public default byte[] read(int at, int len) {
        byte[] data = new byte[len];
        this.read(at, data);
        return data;
    }
    
    public default void read(int at, byte[] data) {
        this.read(at, data, 0, data.length);
    }
    
    public void write(int at, byte[] data, int off, int len);
    
    public void read(int at, byte[] data, int off, int len);

    @Override
    public void close();
    
}
