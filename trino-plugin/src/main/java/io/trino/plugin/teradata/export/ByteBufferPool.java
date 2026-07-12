package io.trino.plugin.teradata.export;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Thread-safe pool of reusable ByteBuffers to reduce GC pressure.
 * Option E: Zero-Copy ByteBuffer Pooling
 */
public final class ByteBufferPool {
    private final ArrayBlockingQueue<ByteBuffer> pool;
    private final int bufferSize;

    public ByteBufferPool(int poolSize, int bufferSize) {
        this.pool = new ArrayBlockingQueue<>(poolSize);
        this.bufferSize = bufferSize;
        // Pre-allocate buffers
        for (int i = 0; i < poolSize; i++) {
            pool.offer(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    public ByteBuffer acquire() {
        ByteBuffer buffer = pool.poll();
        if (buffer == null) {
            // Pool exhausted, allocate new buffer (will be GC'd)
            return ByteBuffer.allocateDirect(bufferSize);
        }
        buffer.clear();
        return buffer;
    }

    public void release(ByteBuffer buffer) {
        if (buffer != null && buffer.capacity() == bufferSize) {
            buffer.clear();
            pool.offer(buffer); // May fail if pool is full, that's OK
        }
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Release all DirectByteBuffers in the pool.
     * Must be called when the pool is no longer needed to prevent native memory leaks.
     * 
     * Uses JDK 9+ compatible approach: sun.misc.Unsafe.invokeCleaner()
     * Fallback chain: Unsafe.invokeCleaner() -> JDK 8 cleaner() -> GC
     */
    public void close() {
        ByteBuffer buffer;
        while ((buffer = pool.poll()) != null) {
            if (buffer.isDirect()) {
                cleanDirectBuffer(buffer);
            }
        }
    }

    /**
     * Clean a DirectByteBuffer's native memory using the best available method.
     * Supports both JDK 8 (sun.misc.Cleaner) and JDK 9+ (Unsafe.invokeCleaner).
     */
    private static void cleanDirectBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }

        try {
            // JDK 9+ approach: Use Unsafe.invokeCleaner(buffer)
            // This is the officially supported way to clean DirectByteBuffers in modern JDKs
            java.lang.reflect.Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            sun.misc.Unsafe unsafe = (sun.misc.Unsafe) unsafeField.get(null);
            java.lang.reflect.Method invokeCleaner = unsafe.getClass().getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
            return;
        } catch (NoSuchMethodException e) {
            // JDK 8: invokeCleaner doesn't exist, fall through to legacy approach
        } catch (Exception e) {
            // invokeCleaner failed (e.g., buffer was sliced), fall through to legacy
        }

        try {
            // JDK 8 fallback: Use cleaner() method on DirectByteBuffer
            java.lang.reflect.Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            if (cleaner != null) {
                java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            // All cleanup attempts failed - buffer will be cleaned when GC runs
            // This is acceptable as it's the normal JVM behavior for DirectByteBuffers
        }
    }
}
