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

package org.neo4j.laboratory.aggregation.aggregates;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.List;

public class CollectFunction implements AggregateNodeFunction<List<Node>>
{
    public ArrayList<Node> collection = new ArrayList<Node>();
    public void accumulate( Node obj )
    {
        collection.add( obj );
    }

    public List<Node> result()
    {
        return collection;
    }
}
