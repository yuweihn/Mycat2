package io.mycat.hbt4;

import com.google.common.collect.Iterators;
import io.mycat.mpp.Row;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class MycatMergeSortExecutor implements Executor {
    private final Comparator<Row> comparator;
    private final Executor[] executors;
    private Iterator<Row> iterator;

    public MycatMergeSortExecutor(Comparator<Row> comparator,  Executor[] executors) {
        this.comparator = comparator;
        this.executors = executors;
    }

    @Override
    public void open() {
        ArrayList<Iterator<Row>> iterators = new ArrayList<>(executors.length);
        for (Executor executor : executors) {
            executor.open();
            iterators.add(executor.iterator());
        }
        iterator = Iterators.mergeSorted(iterators, comparator);
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }


    @Override
    public void close() {
        for (Executor executor : executors) {
            executor.close();
        }

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}