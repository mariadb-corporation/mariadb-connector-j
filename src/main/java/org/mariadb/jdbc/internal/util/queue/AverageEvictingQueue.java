package org.mariadb.jdbc.internal.util.queue;

import java.util.NoSuchElementException;

public class AverageEvictingQueue {

    private transient long[] elements;

    private transient int start = 0;
    private transient int end = 0;
    private transient boolean full = false;
    private final int maxElements;
    private long average = -1;

    /**
     * Store long value, evicting result to give average value.
     * @param size number of element to keep.
     */
    public AverageEvictingQueue(final int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("The size must be greater than 0");
        }
        elements = new long[size];
        maxElements = elements.length;
    }

    private int size() {
        int size;

        if (end < start) {
            size = maxElements - start + end;
        } else if (end == start) {
            size = full ? maxElements : 0;
        } else {
            size = end - start;
        }

        return size;
    }

    /**
     * Add value.
     * @param element value
     */
    public synchronized void add(final long element) {
        average = -1;
        if (size() == maxElements) remove();
        elements[end++] = element;
        if (end >= maxElements) end = 0;
        if (end == start) full = true;
    }

    private long remove() {
        if (size() == 0) throw new NoSuchElementException("queue is empty");

        final long element = elements[start];
        elements[start++] = 0;

        if (start >= maxElements) start = 0;
        full = false;
        return element;
    }

    private int increment(int index) {
        index++;
        if (index >= maxElements) index = 0;
        return index;
    }

    private int decrement(int index) {
        index--;
        if (index < 0) index = maxElements - 1;
        return index;
    }

    /**
     * Average content value.
     * @return Average content value
     */
    public long averageMs() {
        if (average != -1) return average;
        long total = 0;
        for (int i = 0; i < maxElements; i++) {
            total += elements[0];
        }
        average = size() == 0 ? 0 : total / (size() * 1000000);
        return average;
    }

}
