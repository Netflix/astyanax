package com.netflix.astyanax.recipes.functions;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.netflix.astyanax.model.Row;

/**
 * Very basic function to count the total number of columns
 * 
 * @author elandau
 *
 * @param <K>
 * @param <C>
 */
public class ColumnCounterFunction<K,C> implements Function<Row<K,C>, Boolean> {

    private final AtomicLong counter = new AtomicLong(0);
    
    @Override
    public Boolean apply(Row<K,C> input) {
        counter.addAndGet(input.getColumns().size());
        return true;
    }
    
    public long getCount() {
        return counter.get();
    }

    public void reset() {
        counter.set(0);
    }

}
