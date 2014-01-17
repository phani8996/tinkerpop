package com.tinkerpop.gremlin.pipes.named;

import com.tinkerpop.gremlin.pipes.FilterPipe;
import com.tinkerpop.gremlin.pipes.Pipeline;
import com.tinkerpop.gremlin.pipes.util.FastNoSuchElementException;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RangePipe<E> extends FilterPipe<E> {

    public int low;
    public int high;

    public RangePipe(final Pipeline<?, E> pipeline, final int low, final int high) {
        super(pipeline, null);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + "]");
        }
        this.low = low;
        this.high = high;

        final AtomicInteger counter = new AtomicInteger(-1);
        this.predicate = o -> {
            counter.incrementAndGet();
            if ((low == -1 || counter.get() >= low) && (high == -1 || counter.get() <= high))
                return true;
            else if (high != -1 && counter.get() > high)
                throw FastNoSuchElementException.instance();
            else
                return false;
        };


    }
}
