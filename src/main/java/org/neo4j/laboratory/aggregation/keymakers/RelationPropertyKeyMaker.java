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
package org.neo4j.laboratory.aggregation.keymakers;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class RelationPropertyKeyMaker implements KeyMaker
{
    private RelationshipType relationshipType;
    private String property;

    public RelationPropertyKeyMaker( RelationshipType relationshipType,
                                     String property )
    {
        this.relationshipType = relationshipType;
        this.property = property;
    }

    static Relationship findRelationshipTypeInPath(
            RelationshipType relationshipType, Path path )
    {
        for ( Relationship relationship : path.relationships() )
        {
            if ( relationship.isType( relationshipType ) )
            {
                return relationship;
            }
        }

        throw new NotFoundException( "Did not find relationship of type " + relationshipType );
    }

    public Object getKeyValue( Path path )
    {
        return findRelationshipTypeInPath( relationshipType, path ).getProperty( property );
    }
}
