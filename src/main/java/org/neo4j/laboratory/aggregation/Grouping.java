/*
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.laboratory.aggregation;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.laboratory.aggregation.aggregates.AggregateFunction;
import org.neo4j.laboratory.aggregation.aggregates.AggregateFunctionFactory;
import org.neo4j.laboratory.aggregation.aggregates.AggregateNodeFunction;
import org.neo4j.laboratory.aggregation.aggregates.AggregateNodeFunctionFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Grouping
{
    private final Map<Key, List<Path>> groupings = new HashMap<Key, List<Path>>();

    /**
     * Creates a new GroupingDescription.
     *
     * @return a new GroupingDescription.
     */
    public static GroupingDescription description()
    {
        return new GroupingDescription();
    }


    public Grouping( GroupingDescription groupingDescription,
                     Traverser traverser )
    {
        for ( Path p : traverser )
        {
            Key key = groupingDescription.getGroupingKey( p );
            if ( !groupings.containsKey( key ) )
            {
                groupings.put( key, new ArrayList<Path>() );
            }

            groupings.get( key ).add( p );
        }
    }

    public <T> Map<Key, T> aggregateNode( int offset,
                                          AggregateNodeFunctionFactory<T> functionFactory )
    {
        Map<Key, T> resultMap = new HashMap<Key, T>( groupings.size() );
        for ( Key key : groupings.keySet() )
        {
            AggregateNodeFunction<T> aggregateFunction = functionFactory.newGrouping();
            List<Path> groupedPaths = groupings.get( key );
            for ( Path path : groupedPaths )
            {
                Node valueNode = getNodeByOffset( path, offset );
                aggregateFunction.accumulate( valueNode );
            }

            resultMap.put( key, aggregateFunction.result() );
        }
        return resultMap;
    }

    public <T> Map<Key, T> aggregateNodeProperty( int offset, String key,
                                                  AggregateFunctionFactory<T> functionFactory )
    {
        Map<Key, T> resultMap = new HashMap<Key, T>( groupings.size() );

        for ( Key groupingKey : groupings.keySet() )
        {
            AggregateFunction<T> aggregateFunction = functionFactory.newGrouping();
            List<Path> groupedPaths = groupings.get( groupingKey );
            for ( Path path : groupedPaths )
            {
                Node valueNode = getNodeByOffset( path, offset );
                Object value = valueNode.getProperty( key );
                aggregateFunction.accumulate( value );
            }
            resultMap.put( groupingKey, aggregateFunction.result() );
        }

        return resultMap;
    }

    public static Node getNodeByOffset( Path path, int offset )
    {
        if ( offset == 0 )
        {
            return path.endNode();
        }

        if ( offset > 0 )
        {
            return getNodeFromTheStart( path, offset );
        }

        ArrayList<Node> list = copyNodesToList( path );
        offset = path.length() + offset;

        return list.get( offset );
    }

    private static ArrayList<Node> copyNodesToList( Path path )
    {
        ArrayList<Node> list = new ArrayList<Node>( path.length() );
        for ( Node node : path.nodes() )
        {
            list.add( node );
        }
        return list;
    }

    private static Node getNodeFromTheStart( Path path, int offset )
    {
        int i = 0;
        for ( Node node : path.nodes() )
        {
            if ( i == offset )
            {
                return node;
            }
            i++;
        }
        throw new NotFoundException( "Offset points to outside the path" );
    }
}
