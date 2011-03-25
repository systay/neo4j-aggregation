/**
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

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.laboratory.aggregation.keymakers.*;

import java.util.HashMap;
import java.util.Map;

public class GroupingDescription
{
    final private Map<String, KeyMaker> keyMakers;

    public GroupingDescription()
    {
        keyMakers = new HashMap<String, KeyMaker>();
    }

    private GroupingDescription( Map<String, KeyMaker> keyMakers,
                                 String keyName, KeyMaker newKeyMaker )
    {
        this.keyMakers = new HashMap<String, KeyMaker>( keyMakers );
        this.keyMakers.put( keyName, newKeyMaker );
    }

    /**
     * Groups by a node.
     *
     * @param offset  If the offset is 0, it groups by the last node in the path.
     *                If the offset is negative, it points out how many steps from the end
     *                to walk the path to find the grouping node.
     *                If the offset is positive, it points out how many steps from the beginning
     *                of the path to walk to find the grouping node.
     * @param keyName The name to use to refer to this grouping.
     * @return An aggredescription that contains this grouping.
     */
    public GroupingDescription groupByNode( int offset, String keyName )
    {
        return new GroupingDescription( keyMakers, keyName, new NodeKeyMaker( offset ) );
    }

    /**
     * Groups by the property of a node
     *
     * @param offset   If the offset is 0, it groups by the last node in the path.
     *                 If the offset is negative, it points out how many steps from the end
     *                 to walk the path to find the grouping node.
     *                 If the offset is positive, it points out how many steps from the beginning
     *                 of the path to walk to find the grouping node.
     * @param property In the node, us this property as the grouping value.
     * @return An aggredescription that contains this grouping.
     */
    public GroupingDescription groupByNodeProperty( int offset,
                                                    String property )
    {
        return new GroupingDescription( keyMakers, property, new NodePropertyKeyMaker( offset, property ) );
    }

    /**
     * Groups by the property in a relationship
     *
     * @param relationshipType Use the first relation of this type, starting from the beginning.
     * @param property         On the relationship, use this property to group by
     * @return An aggregation description that contains this grouping.
     */
    public GroupingDescription groupByRelationProperty(
            RelationshipType relationshipType, String property )
    {
        return new GroupingDescription( keyMakers, property, new RelationPropertyKeyMaker( relationshipType, property ) );
    }

    /**
     * Groups by the end node of a relationship type. Use this if your paths are variable
     * in structure
     *
     * @param relationshipType This is the relationship type to find
     * @param keyName          The name of the key
     * @return An aggregation description that contains this grouping.
     */
    public GroupingDescription groupByRelationEndNode(
            RelationshipType relationshipType, String keyName )
    {
        return new GroupingDescription( keyMakers, keyName, new RelationShipEndNodeKeyMaker( relationshipType ) );
    }


    /**
     * Groups by the end node of a relationship type. Use this if your paths are variable
     * in structure
     *
     * @param relationshipType This is the relationship type to find
     * @param keyName          The name of the key
     * @return An aggregation description that contains this grouping.
     */
    public GroupingDescription groupByRelationStartNode(
            RelationshipType relationshipType, String keyName )
    {
        return new GroupingDescription( keyMakers, keyName, new RelationShipStartNodeKeyMaker( relationshipType ) );
    }

    /**
     * Creates a grouping from the paths contained in the traverser.
     *
     * @param traverser The traverser contains the paths to be grouped.
     * @return A grouping object, that can be used to calculate aggregates.
     */
    public Grouping groupFrom( Traverser traverser )
    {
        return new Grouping( this, traverser );
    }

    Key getGroupingKey( Path path )
    {
        Key key = new Key();
        for ( String keyName : keyMakers.keySet() )
        {
            KeyMaker keyMaker = keyMakers.get( keyName );
            Object keyValue = keyMaker.getKeyValue( path );
            key.addKey( keyName, keyValue );
        }
        return key;
    }
}
