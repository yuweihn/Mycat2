package io.mycat.util;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.*;

/**
 * A hash table supporting full concurrency of retrievals, adjustable expected
 * concurrency for updates, and a maximum capacity to bound the map by. This
 * implementation differs from {@link ConcurrentHashMap} in that it maintains a
 * page replacement algorithm that is used to evict an entry when the map has
 * exceeded its capacity. Unlike the <tt>Java Collections Framework</tt>, this
 * map does not have a publicly visible constructor and instances are created
 * through a {@link Builder}.
 * <p>
 * An entry is evicted from the map when the <tt>weighted capacity</tt> exceeds
 * its <tt>maximum weighted capacity</tt> threshold. A {@link Weigher} instance
 * determines how many units of capacity that a value consumes. The default
 * weigher assigns each value a weight of <tt>1</tt> to bound the map by the
 * total number of key-value pairs. A map that holds collections may choose to
 * weigh values by the number of elements in the collection and bound the map
 * by the total number of elements that it contains. A change to a value that
 * modifies its weight requires that an update operation is performed on the
 * map.
 * <p>
 * An {@link EvictionListener} may be supplied for notification when an entry
 * is evicted from the map. This listener is invoked on a caller's thread and
 * will not block other threads from operating on the map. An implementation
 * should be aware that the caller's thread will not expect long execution
 * times or failures as a side effect of the listener being notified. Execution
 * safety and a fast turn around time can be achieved by performing the
 * operation asynchronously, such as by submitting a task to an
 * {@link ExecutorService}.
 * <p>
 * The <tt>concurrency level</tt> determines the number of threads that can
 * concurrently modify the table. Using a significantly higher or lower value
 * than needed can waste space or lead to thread contention, but an estimate
 * within an order of magnitude of the ideal value does not usually have a
 * noticeable impact. Because placement in hash tables is essentially random,
 * the actual concurrency will vary.
 * <p>
 * This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces.
 * <p>
 * Like {@link Hashtable} but unlike {@link HashMap}, this class
 * does <em>not</em> allow <tt>null</tt> to be used as a key or value. Unlike
 * {@link LinkedHashMap}, this class does <em>not</em> provide
 * predictable iteration order. A snapshot of the keys and entries may be
 * obtained in ascending and descending order of retention.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @see <a href="http://code.google.com/p/concurrentlinkedhashmap/">
 *      http://code.google.com/p/concurrentlinkedhashmap/</a>
 */
public final class ConcurrentLinkedHashMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V>, Serializable {

    /*
     * This class performs a best-effort bounding of a ConcurrentHashMap using a
     * page-replacement algorithm to determine which entries to evict when the
     * capacity is exceeded.
     *
     * The page replacement algorithm's data structures are kept eventually
     * consistent with the map. An update to the map and recording of reads may
     * not be immediately reflected on the algorithm's data structures. These
     * structures are guarded by a lock and operations are applied in batches to
     * avoid lock contention. The penalty of applying the batches is spread across
     * threads so that the amortized cost is slightly higher than performing just
     * the ConcurrentHashMap operation.
     *
     * A memento of the reads and writes that were performed on the map are
     * recorded in buffers. These buffers are drained at the first opportunity
     * after a write or when the read buffer exceeds a threshold size. The reads
     * are recorded in a lossy buffer, allowing the reordering operations to be
     * discarded if the draining process cannot keep up. Due to the concurrent
     * nature of the read and write operations a strict policy ordering is not
     * possible, but is observably strict when single threaded.
     *
     * Due to a lack of a strict ordering guarantee, a task can be executed
     * out-of-order, such as a removal followed by its addition. The state of the
     * entry is encoded within the value's weight.
     *
     * Alive: The entry is in both the hash-table and the page replacement policy.
     * This is represented by a positive weight.
     *
     * Retired: The entry is not in the hash-table and is pending removal from the
     * page replacement policy. This is represented by a negative weight.
     *
     * Dead: The entry is not in the hash-table and is not in the page replacement
     * policy. This is represented by a weight of zero.
     *
     * The Least Recently Used page replacement algorithm was chosen due to its
     * simplicity, high hit rate, and ability to be implemented with O(1) time
     * complexity.
     */

    /** The number of CPUs */
    static final int NCPU = Runtime.getRuntime().availableProcessors();

    /** The maximum weighted capacity of the map. */
    static final long MAXIMUM_CAPACITY = Long.MAX_VALUE - Integer.MAX_VALUE;

    /** The number of read buffers to use. */
    static final int NUMBER_OF_READ_BUFFERS = ceilingNextPowerOfTwo(NCPU);

    /** Mask value for indexing into the read buffers. */
    static final int READ_BUFFERS_MASK = NUMBER_OF_READ_BUFFERS - 1;

    /** The number of pending read operations before attempting to drain. */
    static final int READ_BUFFER_THRESHOLD = 32;

    /** The maximum number of read operations to perform per amortized drain. */
    static final int READ_BUFFER_DRAIN_THRESHOLD = 2 * READ_BUFFER_THRESHOLD;

    /** The maximum number of pending reads per buffer. */
    static final int READ_BUFFER_SIZE = 2 * READ_BUFFER_DRAIN_THRESHOLD;

    /** Mask value for indexing into the read buffer. */
    static final int READ_BUFFER_INDEX_MASK = READ_BUFFER_SIZE - 1;

    /** The maximum number of write operations to perform per amortized drain. */
    static final int WRITE_BUFFER_DRAIN_THRESHOLD = 16;

    /** A queue that discards all entries. */
    static final Queue<?> DISCARDING_QUEUE = new DiscardingQueue();

    static int ceilingNextPowerOfTwo(int x) {
        // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(x - 1));
    }

    // The backing data store holding the key-value associations
    final ConcurrentMap<K, Node<K, V>> data;
    final int concurrencyLevel;

    // These fields provide support to bound the map by a maximum capacity
    // @GuardedBy("evictionLock")
    final long[] readBufferReadCount;
    // @GuardedBy("evictionLock")
    final LinkedDeque<Node<K, V>> evictionDeque;

    // @GuardedBy("evictionLock") // must write under lock
    final AtomicLong weightedSize;
    // @GuardedBy("evictionLock") // must write under lock
    final AtomicLong capacity;

    final Lock evictionLock;
    final Queue<Runnable> writeBuffer;
    final AtomicLong[] readBufferWriteCount;
    final AtomicLong[] readBufferDrainAtWriteCount;
    final AtomicReference<Node<K, V>>[][] readBuffers;

    final AtomicReference<DrainStatus> drainStatus;
    final EntryWeigher<? super K, ? super V> weigher;

    // These fields provide support for notifying a listener.
    final Queue<Node<K, V>> pendingNotifications;
    final EvictionListener<K, V> listener;

    transient Set<K> keySet;
    transient Collection<V> values;
    transient Set<Entry<K, V>> entrySet;

    public ConcurrentLinkedHashMap() {
        this(128,Long.MAX_VALUE);
    }

    public ConcurrentLinkedHashMap(int initialCapacity, long maximumWeightedCapacity) {
        this(new Builder<K,V>().initialCapacity(initialCapacity).maximumWeightedCapacity(maximumWeightedCapacity));
    }
    /**
     * Creates an instance based on the builder's configuration.
     */
    @SuppressWarnings({"unchecked", "cast"})
    private ConcurrentLinkedHashMap(Builder<K, V> builder) {
        // The data store and its maximum capacity
        concurrencyLevel = builder.concurrencyLevel;
        capacity = new AtomicLong(Math.min(builder.capacity, MAXIMUM_CAPACITY));
        if(builder.referenceType == WeakReference.class){
            data = new ConcurrentReferenceHashMap<>(builder.initialCapacity, 0.75f, concurrencyLevel,
                    ConcurrentReferenceHashMap.ReferenceType.WEAK);
        }else if(builder.referenceType == SoftReference.class){
            data = new ConcurrentReferenceHashMap<>(builder.initialCapacity, 0.75f, concurrencyLevel,
                    ConcurrentReferenceHashMap.ReferenceType.SOFT);
        }else {
            data = new ConcurrentHashMap<>(builder.initialCapacity, 0.75f, concurrencyLevel);
        }

        // The eviction support
        weigher = builder.weigher;
        evictionLock = new ReentrantLock();
        weightedSize = new AtomicLong();
        evictionDeque = new LinkedDeque<Node<K, V>>();
        writeBuffer = new ConcurrentLinkedQueue<Runnable>();
        drainStatus = new AtomicReference<DrainStatus>(DrainStatus.IDLE);

        readBufferReadCount = new long[NUMBER_OF_READ_BUFFERS];
        readBufferWriteCount = new AtomicLong[NUMBER_OF_READ_BUFFERS];
        readBufferDrainAtWriteCount = new AtomicLong[NUMBER_OF_READ_BUFFERS];
        readBuffers = new AtomicReference[NUMBER_OF_READ_BUFFERS][READ_BUFFER_SIZE];
        for (int i = 0; i < NUMBER_OF_READ_BUFFERS; i++) {
            readBufferWriteCount[i] = new AtomicLong();
            readBufferDrainAtWriteCount[i] = new AtomicLong();
            readBuffers[i] = new AtomicReference[READ_BUFFER_SIZE];
            for (int j = 0; j < READ_BUFFER_SIZE; j++) {
                readBuffers[i][j] = new AtomicReference<Node<K, V>>();
            }
        }

        // The notification queue and listener
        listener = builder.listener;
        pendingNotifications = (listener == DiscardingListener.INSTANCE)
                ? (Queue<Node<K, V>>) DISCARDING_QUEUE
                : new ConcurrentLinkedQueue<Node<K, V>>();
    }

    /** Ensures that the object is not null. */
    static void checkNotNull(Object o) {
        if (o == null) {
            throw new NullPointerException();
        }
    }

    /** Ensures that the argument expression is true. */
    static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    /** Ensures that the state expression is true. */
    static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    /* ---------------- Eviction Support -------------- */

    /**
     * Retrieves the maximum weighted capacity of the map.
     *
     * @return the maximum weighted capacity
     */
    public long capacity() {
        return capacity.get();
    }

    /**
     * Sets the maximum weighted capacity of the map and eagerly evicts entries
     * until it shrinks to the appropriate size.
     *
     * @param capacity the maximum weighted capacity of the map
     * @throws IllegalArgumentException if the capacity is negative
     */
    public void setCapacity(long capacity) {
        checkArgument(capacity >= 0);
        evictionLock.lock();
        try {
            this.capacity.lazySet(Math.min(capacity, MAXIMUM_CAPACITY));
            drainBuffers();
            evict();
        } finally {
            evictionLock.unlock();
        }
        notifyListener();
    }

    /** Determines whether the map has exceeded its capacity. */
    // @GuardedBy("evictionLock")
    boolean hasOverflowed() {
        return weightedSize.get() > capacity.get();
    }

    /**
     * Evicts entries from the map while it exceeds the capacity and appends
     * evicted entries to the notification queue for processing.
     */
    // @GuardedBy("evictionLock")
    void evict() {
        // Attempts to evict entries from the map if it exceeds the maximum
        // capacity. If the eviction fails due to a concurrent removal of the
        // victim, that removal may cancel out the addition that triggered this
        // eviction. The victim is eagerly unlinked before the removal task so
        // that if an eviction is still required then a new victim will be chosen
        // for removal.
        while (hasOverflowed()) {
            final Node<K, V> node = evictionDeque.poll();

            // If weighted values are used, then the pending operations will adjust
            // the size to reflect the correct weight
            if (node == null) {
                return;
            }

            // Notify the listener only if the entry was evicted
            if (data.remove(node.key, node)) {
                pendingNotifications.add(node);
            }

            makeDead(node);
        }
    }

    /**
     * Performs the post-processing work required after a read.
     *
     * @param node the entry in the page replacement policy
     */
    void afterRead(Node<K, V> node) {
        final int bufferIndex = readBufferIndex();
        final long writeCount = recordRead(bufferIndex, node);
        drainOnReadIfNeeded(bufferIndex, writeCount);
        notifyListener();
    }

    /** Returns the index to the read buffer to record into. */
    static int readBufferIndex() {
        // A buffer is chosen by the thread's id so that tasks are distributed in a
        // pseudo evenly manner. This helps avoid hot entries causing contention
        // due to other threads trying to append to the same buffer.
        return ((int) Thread.currentThread().getId()) & READ_BUFFERS_MASK;
    }

    /**
     * Records a read in the buffer and return its write count.
     *
     * @param bufferIndex the index to the chosen read buffer
     * @param node the entry in the page replacement policy
     * @return the number of writes on the chosen read buffer
     */
    long recordRead(int bufferIndex, Node<K, V> node) {
        // The location in the buffer is chosen in a racy fashion as the increment
        // is not atomic with the insertion. This means that concurrent reads can
        // overlap and overwrite one another, resulting in a lossy buffer.
        final AtomicLong counter = readBufferWriteCount[bufferIndex];
        final long writeCount = counter.get();
        counter.lazySet(writeCount + 1);

        final int index = (int) (writeCount & READ_BUFFER_INDEX_MASK);
        readBuffers[bufferIndex][index].lazySet(node);

        return writeCount;
    }

    /**
     * Attempts to drain the buffers if it is determined to be needed when
     * post-processing a read.
     *
     * @param bufferIndex the index to the chosen read buffer
     * @param writeCount the number of writes on the chosen read buffer
     */
    void drainOnReadIfNeeded(int bufferIndex, long writeCount) {
        final long pending = (writeCount - readBufferDrainAtWriteCount[bufferIndex].get());
        final boolean delayable = (pending < READ_BUFFER_THRESHOLD);
        final DrainStatus status = drainStatus.get();
        if (status.shouldDrainBuffers(delayable)) {
            tryToDrainBuffers();
        }
    }

    public DrainStatus getDrainStatus() {
        return drainStatus.get();
    }

    /**
     * Performs the post-processing work required after a write.
     *
     * @param task the pending operation to be applied
     */
    void afterWrite(Runnable task) {
        writeBuffer.add(task);
        drainStatus.lazySet(DrainStatus.REQUIRED);
        tryToDrainBuffers();
        notifyListener();
    }

    /**
     * Attempts to acquire the eviction lock and apply the pending operations, up
     * to the amortized threshold, to the page replacement policy.
     */
    void tryToDrainBuffers() {
        if (evictionLock.tryLock()) {
            try {
                drainStatus.lazySet(DrainStatus.PROCESSING);
                drainBuffers();
            } finally {
                drainStatus.compareAndSet(DrainStatus.PROCESSING, DrainStatus.IDLE);
                evictionLock.unlock();
            }
        }
    }

    /** Drains the read and write buffers up to an amortized threshold. */
    // @GuardedBy("evictionLock")
    void drainBuffers() {
        drainReadBuffers();
        drainWriteBuffer();
    }

    /** Drains the read buffers, each up to an amortized threshold. */
    // @GuardedBy("evictionLock")
    void drainReadBuffers() {
        final int start = (int) Thread.currentThread().getId();
        final int end = start + NUMBER_OF_READ_BUFFERS;
        for (int i = start; i < end; i++) {
            drainReadBuffer(i & READ_BUFFERS_MASK);
        }
    }

    /** Drains the read buffer up to an amortized threshold. */
    // @GuardedBy("evictionLock")
    void drainReadBuffer(int bufferIndex) {
        final long writeCount = readBufferWriteCount[bufferIndex].get();
        for (int i = 0; i < READ_BUFFER_DRAIN_THRESHOLD; i++) {
            final int index = (int) (readBufferReadCount[bufferIndex] & READ_BUFFER_INDEX_MASK);
            final AtomicReference<Node<K, V>> slot = readBuffers[bufferIndex][index];
            final Node<K, V> node = slot.get();
            if (node == null) {
                break;
            }

            slot.lazySet(null);
            applyRead(node);
            readBufferReadCount[bufferIndex]++;
        }
        readBufferDrainAtWriteCount[bufferIndex].lazySet(writeCount);
    }

    /** Updates the node's location in the page replacement policy. */
    // @GuardedBy("evictionLock")
    void applyRead(Node<K, V> node) {
        // An entry may be scheduled for reordering despite having been removed.
        // This can occur when the entry was concurrently read while a writer was
        // removing it. If the entry is no longer linked then it does not need to
        // be processed.
        if (evictionDeque.contains(node)) {
            evictionDeque.moveToBack(node);
        }
    }

    /** Drains the read buffer up to an amortized threshold. */
    // @GuardedBy("evictionLock")
    void drainWriteBuffer() {
        for (int i = 0; i < WRITE_BUFFER_DRAIN_THRESHOLD; i++) {
            final Runnable task = writeBuffer.poll();
            if (task == null) {
                break;
            }
            task.run();
        }
    }

    /**
     * Attempts to transition the node from the <tt>alive</tt> state to the
     * <tt>retired</tt> state.
     *
     * @param node the entry in the page replacement policy
     * @param expect the expected weighted value
     * @return if successful
     */
    boolean tryToRetire(Node<K, V> node, WeightedValue<V> expect) {
        if (expect.isAlive()) {
            final WeightedValue<V> retired = new WeightedValue<V>(expect.value, -expect.weight);
            return node.compareAndSet(expect, retired);
        }
        return false;
    }

    /**
     * Atomically transitions the node from the <tt>alive</tt> state to the
     * <tt>retired</tt> state, if a valid transition.
     *
     * @param node the entry in the page replacement policy
     */
    void makeRetired(Node<K, V> node) {
        for (;;) {
            final WeightedValue<V> current = node.get();
            if (!current.isAlive()) {
                return;
            }
            final WeightedValue<V> retired = new WeightedValue<V>(current.value, -current.weight);
            if (node.compareAndSet(current, retired)) {
                return;
            }
        }
    }

    /**
     * Atomically transitions the node to the <tt>dead</tt> state and decrements
     * the <tt>weightedSize</tt>.
     *
     * @param node the entry in the page replacement policy
     */
    // @GuardedBy("evictionLock")
    void makeDead(Node<K, V> node) {
        for (;;) {
            WeightedValue<V> current = node.get();
            WeightedValue<V> dead = new WeightedValue<V>(current.value, 0);
            if (node.compareAndSet(current, dead)) {
                weightedSize.lazySet(weightedSize.get() - Math.abs(current.weight));
                return;
            }
        }
    }

    /** Notifies the listener of entries that were evicted. */
    void notifyListener() {
        Node<K, V> node;
        while ((node = pendingNotifications.poll()) != null) {
            listener.onEviction(node.key, node.getValue());
        }
    }

    /** Adds the node to the page replacement policy. */
    final class AddTask implements Runnable {
        final Node<K, V> node;
        final int weight;

        AddTask(Node<K, V> node, int weight) {
            this.weight = weight;
            this.node = node;
        }

        @Override
        // @GuardedBy("evictionLock")
        public void run() {
            weightedSize.lazySet(weightedSize.get() + weight);

            // ignore out-of-order write operations
            if (node.get().isAlive()) {
                evictionDeque.add(node);
                evict();
            }
        }
    }

    /** Removes a node from the page replacement policy. */
    final class RemovalTask implements Runnable {
        final Node<K, V> node;

        RemovalTask(Node<K, V> node) {
            this.node = node;
        }

        @Override
        // @GuardedBy("evictionLock")
        public void run() {
            // add may not have been processed yet
            evictionDeque.remove(node);
            makeDead(node);
        }
    }

    /** Updates the weighted size and evicts an entry on overflow. */
    final class UpdateTask implements Runnable {
        final int weightDifference;
        final Node<K, V> node;

        public UpdateTask(Node<K, V> node, int weightDifference) {
            this.weightDifference = weightDifference;
            this.node = node;
        }

        @Override
        // @GuardedBy("evictionLock")
        public void run() {
            weightedSize.lazySet(weightedSize.get() + weightDifference);
            applyRead(node);
            evict();
        }
    }

    /* ---------------- Concurrent Map Support -------------- */

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public int size() {
        return data.size();
    }

    /**
     * Returns the weighted size of this map.
     *
     * @return the combined weight of the values in this map
     */
    public long weightedSize() {
        return Math.max(0, weightedSize.get());
    }

    @Override
    public void clear() {
        evictionLock.lock();
        try {
            // Discard all entries
            Node<K, V> node;
            while ((node = evictionDeque.poll()) != null) {
                data.remove(node.key, node);
                makeDead(node);
            }

            // Discard all pending reads
            for (AtomicReference<Node<K, V>>[] buffer : readBuffers) {
                for (AtomicReference<Node<K, V>> slot : buffer) {
                    slot.lazySet(null);
                }
            }

            // Apply all pending writes
            Runnable task;
            while ((task = writeBuffer.poll()) != null) {
                task.run();
            }
        } finally {
            evictionLock.unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value);

        for (Node<K, V> node : data.values()) {
            if (node.getValue().equals(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        final Node<K, V> node = data.get(key);
        if (node == null) {
            return null;
        }
        afterRead(node);
        return node.getValue();
    }

    /**
     * Returns the value to which the specified key is mapped, or {@code null}
     * if this map contains no mapping for the key. This method differs from
     * {@link #get(Object)} in that it does not record the operation with the
     * page replacement policy.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or
     *     {@code null} if this map contains no mapping for the key
     * @throws NullPointerException if the specified key is null
     */
    public V getQuietly(Object key) {
        final Node<K, V> node = data.get(key);
        return (node == null) ? null : node.getValue();
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, false);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return put(key, value, true);
    }

    /**
     * Adds a node to the list and the data store. If an existing node is found,
     * then its value is updated if allowed.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param onlyIfAbsent a write is performed only if the key is not already
     *     associated with a value
     * @return the prior value in the data store or null if no mapping was found
     */
    V put(K key, V value, boolean onlyIfAbsent) {
        checkNotNull(key);
        checkNotNull(value);

        final int weight = weigher.weightOf(key, value);
        final WeightedValue<V> weightedValue = new WeightedValue<V>(value, weight);
        final Node<K, V> node = new Node<K, V>(key, weightedValue);

        for (;;) {
            final Node<K, V> prior = data.putIfAbsent(node.key, node);
            if (prior == null) {
                afterWrite(new AddTask(node, weight));
                return null;
            } else if (onlyIfAbsent) {
                afterRead(prior);
                return prior.getValue();
            }
            for (;;) {
                final WeightedValue<V> oldWeightedValue = prior.get();
                if (!oldWeightedValue.isAlive()) {
                    break;
                }

                if (prior.compareAndSet(oldWeightedValue, weightedValue)) {
                    final int weightedDifference = weight - oldWeightedValue.weight;
                    if (weightedDifference == 0) {
                        afterRead(prior);
                    } else {
                        afterWrite(new UpdateTask(prior, weightedDifference));
                    }
                    return oldWeightedValue.value;
                }
            }
        }
    }

    @Override
    public V remove(Object key) {
        final Node<K, V> node = data.remove(key);
        if (node == null) {
            return null;
        }

        makeRetired(node);
        afterWrite(new RemovalTask(node));
        return node.getValue();
    }

    @Override
    public boolean remove(Object key, Object value) {
        final Node<K, V> node = data.get(key);
        if ((node == null) || (value == null)) {
            return false;
        }

        WeightedValue<V> weightedValue = node.get();
        for (;;) {
            if (weightedValue.contains(value)) {
                if (tryToRetire(node, weightedValue)) {
                    if (data.remove(key, node)) {
                        afterWrite(new RemovalTask(node));
                        return true;
                    }
                } else {
                    weightedValue = node.get();
                    if (weightedValue.isAlive()) {
                        // retry as an intermediate update may have replaced the value with
                        // an equal instance that has a different reference identity
                        continue;
                    }
                }
            }
            return false;
        }
    }

    @Override
    public V replace(K key, V value) {
        checkNotNull(key);
        checkNotNull(value);

        final int weight = weigher.weightOf(key, value);
        final WeightedValue<V> weightedValue = new WeightedValue<V>(value, weight);

        final Node<K, V> node = data.get(key);
        if (node == null) {
            return null;
        }
        for (;;) {
            final WeightedValue<V> oldWeightedValue = node.get();
            if (!oldWeightedValue.isAlive()) {
                return null;
            }
            if (node.compareAndSet(oldWeightedValue, weightedValue)) {
                final int weightedDifference = weight - oldWeightedValue.weight;
                if (weightedDifference == 0) {
                    afterRead(node);
                } else {
                    afterWrite(new UpdateTask(node, weightedDifference));
                }
                return oldWeightedValue.value;
            }
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkNotNull(key);
        checkNotNull(oldValue);
        checkNotNull(newValue);

        final int weight = weigher.weightOf(key, newValue);
        final WeightedValue<V> newWeightedValue = new WeightedValue<V>(newValue, weight);

        final Node<K, V> node = data.get(key);
        if (node == null) {
            return false;
        }
        for (;;) {
            final WeightedValue<V> weightedValue = node.get();
            if (!weightedValue.isAlive() || !weightedValue.contains(oldValue)) {
                return false;
            }
            if (node.compareAndSet(weightedValue, newWeightedValue)) {
                final int weightedDifference = weight - weightedValue.weight;
                if (weightedDifference == 0) {
                    afterRead(node);
                } else {
                    afterWrite(new UpdateTask(node, weightedDifference));
                }
                return true;
            }
        }
    }

    @Override
    public Set<K> keySet() {
        final Set<K> ks = keySet;
        return (ks == null) ? (keySet = new KeySet()) : ks;
    }

    /**
     * Returns a unmodifiable snapshot {@link Set} view of the keys contained in
     * this map. The set's iterator returns the keys whose order of iteration is
     * the ascending order in which its entries are considered eligible for
     * retention, from the least-likely to be retained to the most-likely.
     * <p>
     * Beware that, unlike in {@link #keySet()}, obtaining the set is <em>NOT</em>
     * a constant-time operation. Because of the asynchronous nature of the page
     * replacement policy, determining the retention ordering requires a traversal
     * of the keys.
     *
     * @return an ascending snapshot view of the keys in this map
     */
    public Set<K> ascendingKeySet() {
        return ascendingKeySetWithLimit(Integer.MAX_VALUE);
    }

    /**
     * Returns an unmodifiable snapshot {@link Set} view of the keys contained in
     * this map. The set's iterator returns the keys whose order of iteration is
     * the ascending order in which its entries are considered eligible for
     * retention, from the least-likely to be retained to the most-likely.
     * <p>
     * Beware that, unlike in {@link #keySet()}, obtaining the set is <em>NOT</em>
     * a constant-time operation. Because of the asynchronous nature of the page
     * replacement policy, determining the retention ordering requires a traversal
     * of the keys.
     *
     * @param limit the maximum size of the returned set
     * @return a ascending snapshot view of the keys in this map
     * @throws IllegalArgumentException if the limit is negative
     */
    public Set<K> ascendingKeySetWithLimit(int limit) {
        return orderedKeySet(true, limit);
    }

    /**
     * Returns an unmodifiable snapshot {@link Set} view of the keys contained in
     * this map. The set's iterator returns the keys whose order of iteration is
     * the descending order in which its entries are considered eligible for
     * retention, from the most-likely to be retained to the least-likely.
     * <p>
     * Beware that, unlike in {@link #keySet()}, obtaining the set is <em>NOT</em>
     * a constant-time operation. Because of the asynchronous nature of the page
     * replacement policy, determining the retention ordering requires a traversal
     * of the keys.
     *
     * @return a descending snapshot view of the keys in this map
     */
    public Set<K> descendingKeySet() {
        return descendingKeySetWithLimit(Integer.MAX_VALUE);
    }

    /**
     * Returns an unmodifiable snapshot {@link Set} view of the keys contained in
     * this map. The set's iterator returns the keys whose order of iteration is
     * the descending order in which its entries are considered eligible for
     * retention, from the most-likely to be retained to the least-likely.
     * <p>
     * Beware that, unlike in {@link #keySet()}, obtaining the set is <em>NOT</em>
     * a constant-time operation. Because of the asynchronous nature of the page
     * replacement policy, determining the retention ordering requires a traversal
     * of the keys.
     *
     * @param limit the maximum size of the returned set
     * @return a descending snapshot view of the keys in this map
     * @throws IllegalArgumentException if the limit is negative
     */
    public Set<K> descendingKeySetWithLimit(int limit) {
        return orderedKeySet(false, limit);
    }

    Set<K> orderedKeySet(boolean ascending, int limit) {
        checkArgument(limit >= 0);
        evictionLock.lock();
        try {
            drainBuffers();

            final int initialCapacity = (weigher == Weighers.entrySingleton())
                    ? Math.min(limit, (int) weightedSize())
                    : 16;
            final Set<K> keys = new LinkedHashSet<K>(initialCapacity);
            final Iterator<Node<K, V>> iterator = ascending
                    ? evictionDeque.iterator()
                    : evictionDeque.descendingIterator();
            while (iterator.hasNext() && (limit > keys.size())) {
                keys.add(iterator.next().key);
            }
            return unmodifiableSet(keys);
        } finally {
            evictionLock.unlock();
        }
    }

    @Override
    public Collection<V> values() {
        final Collection<V> vs = values;
        return (vs == null) ? (values = new Values()) : vs;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        final Set<Entry<K, V>> es = entrySet;
        return (es == null) ? (entrySet = new EntrySet()) : es;
    }

    /**
     * Returns an unmodifiable snapshot {@link Map} view of the mappings contained
     * in this map. The map's collections return the mappings whose order of
     * iteration is the ascending order in which its entries are considered
     * eligible for retention, from the least-likely to be retained to the
     * most-likely.
     * <p>
     * Beware that obtaining the mappings is <em>NOT</em> a constant-time
     * operation. Because of the asynchronous nature of the page replacement
     * policy, determining the retention ordering requires a traversal of the
     * entries.
     *
     * @return a ascending snapshot view of this map
     */
    public Map<K, V> ascendingMap() {
        return ascendingMapWithLimit(Integer.MAX_VALUE);
    }

    /**
     * Returns an unmodifiable snapshot {@link Map} view of the mappings contained
     * in this map. The map's collections return the mappings whose order of
     * iteration is the ascending order in which its entries are considered
     * eligible for retention, from the least-likely to be retained to the
     * most-likely.
     * <p>
     * Beware that obtaining the mappings is <em>NOT</em> a constant-time
     * operation. Because of the asynchronous nature of the page replacement
     * policy, determining the retention ordering requires a traversal of the
     * entries.
     *
     * @param limit the maximum size of the returned map
     * @return a ascending snapshot view of this map
     * @throws IllegalArgumentException if the limit is negative
     */
    public Map<K, V> ascendingMapWithLimit(int limit) {
        return orderedMap(true, limit);
    }

    /**
     * Returns an unmodifiable snapshot {@link Map} view of the mappings contained
     * in this map. The map's collections return the mappings whose order of
     * iteration is the descending order in which its entries are considered
     * eligible for retention, from the most-likely to be retained to the
     * least-likely.
     * <p>
     * Beware that obtaining the mappings is <em>NOT</em> a constant-time
     * operation. Because of the asynchronous nature of the page replacement
     * policy, determining the retention ordering requires a traversal of the
     * entries.
     *
     * @return a descending snapshot view of this map
     */
    public Map<K, V> descendingMap() {
        return descendingMapWithLimit(Integer.MAX_VALUE);
    }

    /**
     * Returns an unmodifiable snapshot {@link Map} view of the mappings contained
     * in this map. The map's collections return the mappings whose order of
     * iteration is the descending order in which its entries are considered
     * eligible for retention, from the most-likely to be retained to the
     * least-likely.
     * <p>
     * Beware that obtaining the mappings is <em>NOT</em> a constant-time
     * operation. Because of the asynchronous nature of the page replacement
     * policy, determining the retention ordering requires a traversal of the
     * entries.
     *
     * @param limit the maximum size of the returned map
     * @return a descending snapshot view of this map
     * @throws IllegalArgumentException if the limit is negative
     */
    public Map<K, V> descendingMapWithLimit(int limit) {
        return orderedMap(false, limit);
    }

    Map<K, V> orderedMap(boolean ascending, int limit) {
        checkArgument(limit >= 0);
        evictionLock.lock();
        try {
            drainBuffers();

            final int initialCapacity = (weigher == Weighers.entrySingleton())
                    ? Math.min(limit, (int) weightedSize())
                    : 16;
            final Map<K, V> map = new LinkedHashMap<K, V>(initialCapacity);
            final Iterator<Node<K, V>> iterator = ascending
                    ? evictionDeque.iterator()
                    : evictionDeque.descendingIterator();
            while (iterator.hasNext() && (limit > map.size())) {
                Node<K, V> node = iterator.next();
                map.put(node.key, node.getValue());
            }
            return unmodifiableMap(map);
        } finally {
            evictionLock.unlock();
        }
    }

    /** The draining status of the buffers. */
    public enum DrainStatus {

        /** A drain is not taking place. */
        IDLE {
            @Override boolean shouldDrainBuffers(boolean delayable) {
                return !delayable;
            }
        },

        /** A drain is required due to a pending write modification. */
        REQUIRED {
            @Override boolean shouldDrainBuffers(boolean delayable) {
                return true;
            }
        },

        /** A drain is in progress. */
        PROCESSING {
            @Override boolean shouldDrainBuffers(boolean delayable) {
                return false;
            }
        };

        /**
         * Determines whether the buffers should be drained.
         *
         * @param delayable if a drain should be delayed until required
         * @return if a drain should be attempted
         */
        abstract boolean shouldDrainBuffers(boolean delayable);
    }

    /** A value, its weight, and the entry's status. */
    // @Immutable
    static final class WeightedValue<V> {
        final int weight;
        final V value;

        WeightedValue(V value, int weight) {
            this.weight = weight;
            this.value = value;
        }

        boolean contains(Object o) {
            return (o == value) || value.equals(o);
        }

        /**
         * If the entry is available in the hash-table and page replacement policy.
         */
        boolean isAlive() {
            return weight > 0;
        }

        /**
         * If the entry was removed from the hash-table and is awaiting removal from
         * the page replacement policy.
         */
        boolean isRetired() {
            return weight < 0;
        }

        /**
         * If the entry was removed from the hash-table and the page replacement
         * policy.
         */
        boolean isDead() {
            return weight == 0;
        }
    }

    /**
     * A node contains the key, the weighted value, and the linkage pointers on
     * the page-replacement algorithm's data structures.
     */
    @SuppressWarnings("serial")
    static final class Node<K, V> extends AtomicReference<WeightedValue<V>>
            implements Linked<Node<K, V>> {
        final K key;
        // @GuardedBy("evictionLock")
        Node<K, V> prev;
        // @GuardedBy("evictionLock")
        Node<K, V> next;

        /** Creates a new, unlinked node. */
        Node(K key, WeightedValue<V> weightedValue) {
            super(weightedValue);
            this.key = key;
        }

        @Override
        // @GuardedBy("evictionLock")
        public Node<K, V> getPrevious() {
            return prev;
        }

        @Override
        // @GuardedBy("evictionLock")
        public void setPrevious(Node<K, V> prev) {
            this.prev = prev;
        }

        @Override
        // @GuardedBy("evictionLock")
        public Node<K, V> getNext() {
            return next;
        }

        @Override
        // @GuardedBy("evictionLock")
        public void setNext(Node<K, V> next) {
            this.next = next;
        }

        /** Retrieves the value held by the current <tt>WeightedValue</tt>. */
        V getValue() {
            return get().value;
        }
    }

    /** An adapter to safely externalize the keys. */
    final class KeySet extends AbstractSet<K> {
        final ConcurrentLinkedHashMap<K, V> map = ConcurrentLinkedHashMap.this;

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public Iterator<K> iterator() {
            return new KeyIterator();
        }

        @Override
        public boolean contains(Object obj) {
            return containsKey(obj);
        }

        @Override
        public boolean remove(Object obj) {
            return (map.remove(obj) != null);
        }

        @Override
        public Object[] toArray() {
            return map.data.keySet().toArray();
        }

        @Override
        public <T> T[] toArray(T[] array) {
            return map.data.keySet().toArray(array);
        }
    }

    /** An adapter to safely externalize the key iterator. */
    final class KeyIterator implements Iterator<K> {
        final Iterator<K> iterator = data.keySet().iterator();
        K current;

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public K next() {
            current = iterator.next();
            return current;
        }

        @Override
        public void remove() {
            checkState(current != null);
            ConcurrentLinkedHashMap.this.remove(current);
            current = null;
        }
    }

    /** An adapter to safely externalize the values. */
    final class Values extends AbstractCollection<V> {

        @Override
        public int size() {
            return ConcurrentLinkedHashMap.this.size();
        }

        @Override
        public void clear() {
            ConcurrentLinkedHashMap.this.clear();
        }

        @Override
        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        @Override
        public boolean contains(Object o) {
            return containsValue(o);
        }
    }

    /** An adapter to safely externalize the value iterator. */
    final class ValueIterator implements Iterator<V> {
        final Iterator<Node<K, V>> iterator = data.values().iterator();
        Node<K, V> current;

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public V next() {
            current = iterator.next();
            return current.getValue();
        }

        @Override
        public void remove() {
            checkState(current != null);
            ConcurrentLinkedHashMap.this.remove(current.key);
            current = null;
        }
    }

    /** An adapter to safely externalize the entries. */
    final class EntrySet extends AbstractSet<Entry<K, V>> {
        final ConcurrentLinkedHashMap<K, V> map = ConcurrentLinkedHashMap.this;

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public boolean contains(Object obj) {
            if (!(obj instanceof Entry<?, ?>)) {
                return false;
            }
            Entry<?, ?> entry = (Entry<?, ?>) obj;
            Node<K, V> node = map.data.get(entry.getKey());
            return (node != null) && (node.getValue().equals(entry.getValue()));
        }

        @Override
        public boolean add(Entry<K, V> entry) {
            return (map.putIfAbsent(entry.getKey(), entry.getValue()) == null);
        }

        @Override
        public boolean remove(Object obj) {
            if (!(obj instanceof Entry<?, ?>)) {
                return false;
            }
            Entry<?, ?> entry = (Entry<?, ?>) obj;
            return map.remove(entry.getKey(), entry.getValue());
        }
    }

    /** An adapter to safely externalize the entry iterator. */
    final class EntryIterator implements Iterator<Entry<K, V>> {
        final Iterator<Node<K, V>> iterator = data.values().iterator();
        Node<K, V> current;

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            current = iterator.next();
            return new WriteThroughEntry(current);
        }

        @Override
        public void remove() {
            checkState(current != null);
            ConcurrentLinkedHashMap.this.remove(current.key);
            current = null;
        }
    }

    /** An entry that allows updates to write through to the map. */
    final class WriteThroughEntry extends SimpleEntry<K, V> {
        static final long serialVersionUID = 1;

        WriteThroughEntry(Node<K, V> node) {
            super(node.key, node.getValue());
        }

        @Override
        public V setValue(V value) {
            put(getKey(), value);
            return super.setValue(value);
        }

        Object writeReplace() {
            return new SimpleEntry<K, V>(this);
        }
    }

    /** A weigher that enforces that the weight falls within a valid range. */
    static final class BoundedEntryWeigher<K, V> implements EntryWeigher<K, V>, Serializable {
        static final long serialVersionUID = 1;
        final EntryWeigher<? super K, ? super V> weigher;

        BoundedEntryWeigher(EntryWeigher<? super K, ? super V> weigher) {
            checkNotNull(weigher);
            this.weigher = weigher;
        }

        @Override
        public int weightOf(K key, V value) {
            int weight = weigher.weightOf(key, value);
            checkArgument(weight >= 1);
            return weight;
        }

        Object writeReplace() {
            return weigher;
        }
    }

    /** A queue that discards all additions and is always empty. */
    static final class DiscardingQueue extends AbstractQueue<Object> {
        @Override public boolean add(Object e) { return true; }
        @Override public boolean offer(Object e) { return true; }
        @Override public Object poll() { return null; }
        @Override public Object peek() { return null; }
        @Override public int size() { return 0; }
        @Override public Iterator<Object> iterator() { return emptyList().iterator(); }
    }

    /** A listener that ignores all notifications. */
    enum DiscardingListener implements EvictionListener<Object, Object> {
        INSTANCE;

        @Override public void onEviction(Object key, Object value) {}
    }

    /* ---------------- Serialization Support -------------- */

    static final long serialVersionUID = 1;

    Object writeReplace() {
        return new SerializationProxy<K, V>(this);
    }

    private void readObject(ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }

    /**
     * A proxy that is serialized instead of the map. The page-replacement
     * algorithm's data structures are not serialized so the deserialized
     * instance contains only the entries. This is acceptable as caches hold
     * transient data that is recomputable and serialization would tend to be
     * used as a fast warm-up process.
     */
    static final class SerializationProxy<K, V> implements Serializable {
        final EntryWeigher<? super K, ? super V> weigher;
        final EvictionListener<K, V> listener;
        final int concurrencyLevel;
        final Map<K, V> data;
        final long capacity;

        SerializationProxy(ConcurrentLinkedHashMap<K, V> map) {
            concurrencyLevel = map.concurrencyLevel;
            data = new HashMap<K, V>(map);
            capacity = map.capacity.get();
            listener = map.listener;
            weigher = map.weigher;
        }

        Object readResolve() {
            ConcurrentLinkedHashMap<K, V> map = new Builder<K, V>()
                    .concurrencyLevel(concurrencyLevel)
                    .maximumWeightedCapacity(capacity)
                    .listener(listener)
                    .weigher(weigher)
                    .build();
            map.putAll(data);
            return map;
        }

        static final long serialVersionUID = 1;
    }

    /* ---------------- Builder -------------- */

    /**
     * A builder that creates {@link ConcurrentLinkedHashMap} instances. It
     * provides a flexible approach for constructing customized instances with
     * a named parameter syntax. It can be used in the following manner:
     * <pre>{@code
     * ConcurrentMap<Vertex, Set<Edge>> graph = new Builder<Vertex, Set<Edge>>()
     *     .maximumWeightedCapacity(5000)
     *     .weigher(Weighers.<Edge>set())
     *     .build();
     * }</pre>
     */
    public static final class Builder<K, V> {
        static final int DEFAULT_CONCURRENCY_LEVEL = 16;
        static final int DEFAULT_INITIAL_CAPACITY = 16;

        EvictionListener<K, V> listener;
        EntryWeigher<? super K, ? super V> weigher;

        int concurrencyLevel;
        int initialCapacity;
        long capacity;
        /**
         * null is FinalReference
         * {@link WeakReference}
         * {@link SoftReference}
         */
        Class<? extends Reference> referenceType;

        @SuppressWarnings("unchecked")
        public Builder() {
            capacity = -1;
            weigher = Weighers.entrySingleton();
            initialCapacity = DEFAULT_INITIAL_CAPACITY;
            concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
            listener = (EvictionListener<K, V>) DiscardingListener.INSTANCE;
        }

        public Builder<K, V> referenceType(Class<? extends Reference> referenceType) {
            this.referenceType = referenceType;
            return this;
        }

        /**
         * Specifies the initial capacity of the hash table (default <tt>16</tt>).
         * This is the number of key-value pairs that the hash table can hold
         * before a resize operation is required.
         *
         * @param initialCapacity the initial capacity used to size the hash table
         *     to accommodate this many entries.
         * @throws IllegalArgumentException if the initialCapacity is negative
         * @return Builder
         */
        public Builder<K, V> initialCapacity(int initialCapacity) {
            checkArgument(initialCapacity >= 0);
            this.initialCapacity = initialCapacity;
            return this;
        }

        /**
         * Specifies the maximum weighted capacity to coerce the map to and may
         * exceed it temporarily.
         *
         * @param capacity the weighted threshold to bound the map by
         * @throws IllegalArgumentException if the maximumWeightedCapacity is
         *     negative
         * @return Builder
         */
        public Builder<K, V> maximumWeightedCapacity(long capacity) {
            checkArgument(capacity >= 0);
            this.capacity = capacity;
            return this;
        }

        /**
         * Specifies the estimated number of concurrently updating threads. The
         * implementation performs internal sizing to try to accommodate this many
         * threads (default <tt>16</tt>).
         *
         * @param concurrencyLevel the estimated number of concurrently updating
         *     threads
         * @throws IllegalArgumentException if the concurrencyLevel is less than or
         *     equal to zero
         * @return Builder
         */
        public Builder<K, V> concurrencyLevel(int concurrencyLevel) {
            checkArgument(concurrencyLevel > 0);
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        /**
         * Specifies an optional listener that is registered for notification when
         * an entry is evicted.
         *
         * @param listener the object to forward evicted entries to
         * @throws NullPointerException if the listener is null
         * @return Builder
         */
        public Builder<K, V> listener(EvictionListener<K, V> listener) {
            checkNotNull(listener);
            this.listener = listener;
            return this;
        }

        /**
         * Specifies an algorithm to determine how many the units of capacity a
         * value consumes. The default algorithm bounds the map by the number of
         * key-value pairs by giving each entry a weight of <tt>1</tt>.
         *
         * @param weigher the algorithm to determine a value's weight
         * @throws NullPointerException if the weigher is null
         * @return Builder
         */
        public Builder<K, V> weigher(Weigher<? super V> weigher) {
            this.weigher = (weigher == Weighers.singleton())
                    ? Weighers.<K, V>entrySingleton()
                    : new BoundedEntryWeigher<K, V>(Weighers.asEntryWeigher(weigher));
            return this;
        }

        /**
         * Specifies an algorithm to determine how many the units of capacity an
         * entry consumes. The default algorithm bounds the map by the number of
         * key-value pairs by giving each entry a weight of <tt>1</tt>.
         *
         * @param weigher the algorithm to determine a entry's weight
         * @throws NullPointerException if the weigher is null
         * @return Builder
         */
        public Builder<K, V> weigher(EntryWeigher<? super K, ? super V> weigher) {
            this.weigher = (weigher == Weighers.entrySingleton())
                    ? Weighers.<K, V>entrySingleton()
                    : new BoundedEntryWeigher<K, V>(weigher);
            return this;
        }

        /**
         * Creates a new {@link ConcurrentLinkedHashMap} instance.
         *
         * @throws IllegalStateException if the maximum weighted capacity was
         *     not set
         * @return ConcurrentLinkedHashMap
         */
        public ConcurrentLinkedHashMap<K, V> build() {
            checkState(capacity >= 0);
            return new ConcurrentLinkedHashMap<K, V>(this);
        }
    }
    

    /**
     * Linked list implementation of the the JDK6 Deque interface where the link
     * pointers are tightly integrated with the element. Linked deques have no
     * capacity restrictions; they grow as necessary to support usage. They are not
     * thread-safe; in the absence of external synchronization, they do not support
     * concurrent access by multiple threads. Null elements are prohibited.
     * <p>
     * Most <tt>LinkedDeque</tt> operations run in constant time by assuming that
     * the {@link Linked} parameter is associated with the deque instance. Any usage
     * that violates this assumption will result in non-deterministic behavior.
     * <p>
     * The iterators returned by this class are <em>not</em> <i>fail-fast</i>: If
     * the deque is modified at any time after the iterator is created, the iterator
     * will be in an unknown state. Thus, in the face of concurrent modification,
     * the iterator risks arbitrary, non-deterministic behavior at an undetermined
     * time in the future.
     *
     * @author ben.manes@gmail.com (Ben Manes)
     * @param <E> the type of elements held in this collection
     * @see <a href="http://code.google.com/p/concurrentlinkedhashmap/">
     *      http://code.google.com/p/concurrentlinkedhashmap/</a>
     */
    // @NotThreadSafe
    public final static class LinkedDeque<E extends Linked<E>> extends AbstractCollection<E> {

        // This class provides a doubly-linked list that is optimized for the virtual
        // machine. The first and last elements are manipulated instead of a slightly
        // more convenient sentinel element to avoid the insertion of null checks with
        // NullPointerException throws in the byte code. The links to a removed
        // element are cleared to help a generational garbage collector if the
        // discarded elements inhabit more than one generation.

        /**
         * Pointer to first node.
         * Invariant: (first == null && last == null) ||
         *            (first.prev == null)
         */
        E   first;

        /**
         * Pointer to last node.
         * Invariant: (first == null && last == null) ||
         *            (last.next == null)
         */
        E   last;

        int size;

        /**
         * Links the element to the front of the deque so that it becomes the first
         * element.
         *
         * @param e the unlinked element
         */
        void linkFirst(final E e) {
            final E f = first;
            first = e;

            if (f == null) {
                last = e;
            } else {
                f.setPrevious(e);
                e.setNext(f);
            }
        }

        /**
         * Links the element to the back of the deque so that it becomes the last
         * element.
         *
         * @param e the unlinked element
         */
        void linkLast(final E e) {
            final E previousLast = last;
            last = e;

            if (previousLast == null) {
                first = e;
            } else {
                previousLast.setNext(e);
                e.setPrevious(previousLast);
            }
        }

        /** Unlinks the non-null first element. */
        E unlinkFirst() {
            final E f = first;
            final E next = f.getNext();
            f.setNext(null);

            first = next;
            if (next == null) {
                last = null;
            } else {
                next.setPrevious(null);
            }
            return f;
        }

        /** Unlinks the non-null last element. */
        E unlinkLast() {
            final E l = last;
            final E prev = l.getPrevious();
            l.setPrevious(null);
            last = prev;
            if (prev == null) {
                first = null;
            } else {
                prev.setNext(null);
            }
            return l;
        }

        /** Unlinks the non-null element. */
        void unlink(E e) {
            final E prev = e.getPrevious();
            final E next = e.getNext();

            if (prev == null) {
                first = next;
            } else {
                prev.setNext(next);
                e.setPrevious(null);
            }

            if (next == null) {
                last = prev;
            } else {
                next.setPrevious(prev);
                e.setNext(null);
            }
        }

        @Override
        public boolean isEmpty() {
            return (first == null);
        }

        void checkNotEmpty() {
            if (isEmpty()) {
                throw new NoSuchElementException();
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public void clear() {
            for (E e = first; e != null;) {
                E next = e.getNext();
                e.setPrevious(null);
                e.setNext(null);
                e = next;
            }
            first = last = null;
            size = 0;
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Linked<?>)) {
                return false;
            }
            Linked<?> e = (Linked<?>) o;
            return (e.getPrevious() != null)
                    || (e.getNext() != null)
                    || (e == first);
        }

        /**
         * Moves the element to the front of the deque so that it becomes the first
         * element.
         *
         * @param e the linked element
         */
        public void moveToFront(E e) {
            if (e != first) {
                unlink(e);
                linkFirst(e);
            }
        }

        /**
         * Moves the element to the back of the deque so that it becomes the last
         * element.
         *
         * @param e the linked element
         */
        public void moveToBack(E e) {
            if (e != last) {
                unlink(e);
                linkLast(e);
            }
        }

        public E peek() {
            return peekFirst();
        }

        public E peekFirst() {
            return first;
        }

        public E peekLast() {
            return last;
        }

        public E getFirst() {
            checkNotEmpty();
            return peekFirst();
        }

        public E getLast() {
            checkNotEmpty();
            return peekLast();
        }

        public E element() {
            return getFirst();
        }

        public boolean offer(E e) {
            return offerLast(e);
        }

        public boolean offerFirst(E e) {
            if (contains(e)) {
                return false;
            }
            size++;
            linkFirst(e);
            return true;
        }

        public boolean offerLast(E e) {
            if (contains(e)) {
                return false;
            }
            size++;
            linkLast(e);
            return true;
        }

        @Override
        public boolean add(E e) {
            return offerLast(e);
        }

        public void addFirst(E e) {
            if (!offerFirst(e)) {
                throw new IllegalArgumentException();
            }
        }

        public void addLast(E e) {
            if (!offerLast(e)) {
                throw new IllegalArgumentException();
            }
        }

        public E poll() {
            return pollFirst();
        }

        public E pollFirst() {
            if (isEmpty()) {
                return null;
            }
            size--;
            return unlinkFirst();
        }

        public E pollLast() {
            if (isEmpty()) {
                return null;
            }
            size--;
            return unlinkLast();
        }

        public E remove() {
            return removeFirst();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean remove(Object o) {
            if (contains(o)) {
                size--;
                unlink((E) o);
                return true;
            }
            return false;
        }

        public E removeFirst() {
            checkNotEmpty();
            return pollFirst();
        }

        public boolean removeFirstOccurrence(Object o) {
            return remove(o);
        }

        public E removeLast() {
            checkNotEmpty();
            return pollLast();
        }

        public boolean removeLastOccurrence(Object o) {
            return remove(o);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            boolean modified = false;
            for (Object o : c) {
                modified |= remove(o);
            }
            return modified;
        }

        public void push(E e) {
            addFirst(e);
        }

        public E pop() {
            return removeFirst();
        }

        @Override
        public Iterator<E> iterator() {
            return new AbstractLinkedIterator(first) {
                @Override
                E computeNext() {
                    return cursor.getNext();
                }
            };
        }

        public Iterator<E> descendingIterator() {
            return new AbstractLinkedIterator(last) {
                @Override
                E computeNext() {
                    return cursor.getPrevious();
                }
            };
        }

        abstract class AbstractLinkedIterator implements Iterator<E> {
            E cursor;

            /**
             * Creates an iterator that can can traverse the deque.
             *
             * @param start the initial element to begin traversal from
             */
            AbstractLinkedIterator(E start) {
                cursor = start;
            }
            @Override
            public boolean hasNext() {
                return (cursor != null);
            }
            @Override
            public E next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                E e = cursor;
                cursor = computeNext();
                return e;
            }
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            /**
             * Retrieves the next element to traverse to or <tt>null</tt> if there are
             * no more elements.
             */
            abstract E computeNext();
        }
    }

    /**
     * An element that is linked on the {@link Deque}.
     */
    public interface Linked<T extends Linked<T>> {

        /**
         * Retrieves the previous element or <tt>null</tt> if either the element is
         * unlinked or the first element on the deque.
         * @return previous
         */
        T getPrevious();

        /**
         * Sets the previous element or <tt>null</tt> if there is no link.
         * @param prev prev
         */
        void setPrevious(T prev);

        /**
         * Retrieves the next element or <tt>null</tt> if either the element is
         * unlinked or the last element on the deque.
         * @return Next
         */
        T getNext();

        /**
         * Sets the next element or <tt>null</tt> if there is no link.
         * @param next next
         */
        void setNext(T next);
    }
    
    public interface EvictionListener<K, V> {

        /**
         * A call-back notification that the entry was evicted.
         *
         * @param key the entry's key
         * @param value the entry's value
         */
        void onEviction(K key, V value);
    }
    public interface Weigher<V> {

        /**
         * Measures an object's weight to determine how many units of capacity that
         * the value consumes. A value must consume a minimum of one unit.
         *
         * @param value the object to weigh
         * @return the object's weight
         */
        int weightOf(V value);
    }

    /**
     * A common set of {@link Weigher} implementations.
     *
     * @author ben.manes@gmail.com (Ben Manes)
     * @see <a href="http://code.google.com/p/concurrentlinkedhashmap/">
     *      http://code.google.com/p/concurrentlinkedhashmap/</a>
     */
    public final static class Weighers {

        /**
         * A entry weigher backed by the specified weigher. The weight of the value
         * determines the weight of the entry.
         * @param <K> key
         * @param <V> value
         * @param weigher the weigher to be "wrapped" in a entry weigher.
         * @return A entry weigher view of the specified weigher.
         */
        public static <K, V> EntryWeigher<K, V> asEntryWeigher(
                final Weigher<? super V> weigher) {
            return (weigher == singleton())
                    ? Weighers.<K, V>entrySingleton()
                    : new EntryWeigherView<K, V>(weigher);
        }

        /**
         * A weigher where an entry has a weight of <tt>1</tt>. A map bounded with
         * this weigher will evict when the number of key-value pairs exceeds the
         * capacity.
         * @param <K> key
         * @param <V> value
         * @return A weigher where a value takes one unit of capacity.
         */
        @SuppressWarnings({"cast", "unchecked"})
        public static <K, V> EntryWeigher<K, V> entrySingleton() {
            return (EntryWeigher<K, V>) SingletonEntryWeigher.INSTANCE;
        }

        /**
         * A weigher where a value has a weight of <tt>1</tt>. A map bounded with
         * this weigher will evict when the number of key-value pairs exceeds the
         * capacity.
         * @param <V> value
         * @return A weigher where a value takes one unit of capacity.
         */
        @SuppressWarnings({"cast", "unchecked"})
        public static <V> Weigher<V> singleton() {
            return (Weigher<V>) SingletonWeigher.INSTANCE;
        }

        /**
         * A weigher where the value is a byte array and its weight is the number of
         * bytes. A map bounded with this weigher will evict when the number of bytes
         * exceeds the capacity rather than the number of key-value pairs in the map.
         * This allows for restricting the capacity based on the memory-consumption
         * and is primarily for usage by dedicated caching servers that hold the
         * serialized data.
         * <p>
         * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
         * with this weight can occur then the caller should eagerly evaluate the
         * value and treat it as a removal operation. Alternatively, a custom weigher
         * may be specified on the map to assign an empty value a positive weight.
         *
         * @return A weigher where each byte takes one unit of capacity.
         */
        public static Weigher<byte[]> byteArray() {
            return ByteArrayWeigher.INSTANCE;
        }

        /**
         * A weigher where the value is a {@link Iterable} and its weight is the
         * number of elements. This weigher only should be used when the alternative
         * {@link #collection()} weigher cannot be, as evaluation takes O(n) time. A
         * map bounded with this weigher will evict when the total number of elements
         * exceeds the capacity rather than the number of key-value pairs in the map.
         * <p>
         * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
         * with this weight can occur then the caller should eagerly evaluate the
         * value and treat it as a removal operation. Alternatively, a custom weigher
         * may be specified on the map to assign an empty value a positive weight.
         * @param <E> element
         * @return A weigher where each element takes one unit of capacity.
         */
        @SuppressWarnings({"cast", "unchecked"})
        public static <E> Weigher<? super Iterable<E>> iterable() {
            return (Weigher<Iterable<E>>) (Weigher<?>) IterableWeigher.INSTANCE;
        }

        /**
         * A weigher where the value is a {@link Collection} and its weight is the
         * number of elements. A map bounded with this weigher will evict when the
         * total number of elements exceeds the capacity rather than the number of
         * key-value pairs in the map.
         * <p>
         * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
         * with this weight can occur then the caller should eagerly evaluate the
         * value and treat it as a removal operation. Alternatively, a custom weigher
         * may be specified on the map to assign an empty value a positive weight.
         * @param <E> element
         * @return A weigher where each element takes one unit of capacity.
         */
        @SuppressWarnings({"cast", "unchecked"})
        public static <E> Weigher<? super Collection<E>> collection() {
            return (Weigher<Collection<E>>) (Weigher<?>) CollectionWeigher.INSTANCE;
        }

        /**
         * A weigher where the value is a {@link List} and its weight is the number
         * of elements. A map bounded with this weigher will evict when the total
         * number of elements exceeds the capacity rather than the number of
         * key-value pairs in the map.
         * <p>
         * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
         * with this weight can occur then the caller should eagerly evaluate the
         * value and treat it as a removal operation. Alternatively, a custom weigher
         * may be specified on the map to assign an empty value a positive weight.
         * @param <E> element
         * @return A weigher where each element takes one unit of capacity.
         */
        @SuppressWarnings({"cast", "unchecked"})
        public static <E> Weigher<? super List<E>> list() {
            return (Weigher<List<E>>) (Weigher<?>) ListWeigher.INSTANCE;
        }

        /**
         * A weigher where the value is a {@link Set} and its weight is the number
         * of elements. A map bounded with this weigher will evict when the total
         * number of elements exceeds the capacity rather than the number of
         * key-value pairs in the map.
         * <p>
         * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
         * with this weight can occur then the caller should eagerly evaluate the
         * value and treat it as a removal operation. Alternatively, a custom weigher
         * may be specified on the map to assign an empty value a positive weight.
         * @param <E> element
         * @return A weigher where each element takes one unit of capacity.
         */
        @SuppressWarnings({"cast", "unchecked"})
        public static <E> Weigher<? super Set<E>> set() {
            return (Weigher<Set<E>>) (Weigher<?>) SetWeigher.INSTANCE;
        }

        /**
         * A weigher where the value is a {@link Map} and its weight is the number of
         * entries. A map bounded with this weigher will evict when the total number of
         * entries across all values exceeds the capacity rather than the number of
         * key-value pairs in the map.
         * <p>
         * A value with a weight of <tt>0</tt> will be rejected by the map. If a value
         * with this weight can occur then the caller should eagerly evaluate the
         * value and treat it as a removal operation. Alternatively, a custom weigher
         * may be specified on the map to assign an empty value a positive weight.
         * @param <A> KEY
         * @param <B> VALUE
         * @return A weigher where each entry takes one unit of capacity.
         */
        @SuppressWarnings({"cast", "unchecked"})
        public static <A, B> Weigher<? super Map<A, B>> map() {
            return (Weigher<Map<A, B>>) (Weigher<?>) MapWeigher.INSTANCE;
        }

        static final class EntryWeigherView<K, V> implements EntryWeigher<K, V>, Serializable {
            static final long serialVersionUID = 1;
            final Weigher<? super V> weigher;

            EntryWeigherView(Weigher<? super V> weigher) {
                checkNotNull(weigher);
                this.weigher = weigher;
            }

            @Override
            public int weightOf(K key, V value) {
                return weigher.weightOf(value);
            }
        }

        enum SingletonEntryWeigher implements EntryWeigher<Object, Object> {
            INSTANCE;

            @Override
            public int weightOf(Object key, Object value) {
                return 1;
            }
        }

        enum SingletonWeigher implements Weigher<Object> {
            INSTANCE;

            @Override
            public int weightOf(Object value) {
                return 1;
            }
        }

        enum ByteArrayWeigher implements Weigher<byte[]> {
            INSTANCE;

            @Override
            public int weightOf(byte[] value) {
                return value.length;
            }
        }

        enum IterableWeigher implements Weigher<Iterable<?>> {
            INSTANCE;

            @Override
            public int weightOf(Iterable<?> values) {
                if (values instanceof Collection<?>) {
                    return ((Collection<?>) values).size();
                }
                int size = 0;
                for (Iterator<?> i = values.iterator(); i.hasNext();) {
                    i.next();
                    size++;
                }
                return size;
            }
        }

        enum CollectionWeigher implements Weigher<Collection<?>> {
            INSTANCE;

            @Override
            public int weightOf(Collection<?> values) {
                return values.size();
            }
        }

        enum ListWeigher implements Weigher<List<?>> {
            INSTANCE;

            @Override
            public int weightOf(List<?> values) {
                return values.size();
            }
        }

        enum SetWeigher implements Weigher<Set<?>> {
            INSTANCE;

            @Override
            public int weightOf(Set<?> values) {
                return values.size();
            }
        }

        enum MapWeigher implements Weigher<Map<?, ?>> {
            INSTANCE;

            @Override
            public int weightOf(Map<?, ?> values) {
                return values.size();
            }
        }
    }
    public interface EntryWeigher<K, V> {

        /**
         * Measures an entry's weight to determine how many units of capacity that
         * the key and value consumes. An entry must consume a minimum of one unit.
         *
         * @param key the key to weigh
         * @param value the value to weigh
         * @return the entry's weight
         */
        int weightOf(K key, V value);
    }


    public static void main(String[] args) {
        ConcurrentLinkedHashMap map = new ConcurrentLinkedHashMap<>();
//        ConcurrentHashMap map = new ConcurrentHashMap();
        for (int i = 0; i < 100; i++) {
            map.put(i,i);
        }
        map.put("1","2");
        map.put("3","2");

//        LoadingCache graphs = Caffeine.newBuilder()
//                .maximumSize(10_000)
//                .expireAfterWrite(5, TimeUnit.MINUTES)
//                .refreshAfterWrite(1, TimeUnit.MINUTES)
//                .build(key -> createExpensiveGraph(key));

        System.out.println("map = " + map);
    }
}
