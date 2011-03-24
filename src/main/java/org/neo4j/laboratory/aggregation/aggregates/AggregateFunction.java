package org.neo4j.laboratory.aggregation.aggregates;

public interface AggregateFunction<T>
{
    void accumulate(Object obj);

    T result();
}
