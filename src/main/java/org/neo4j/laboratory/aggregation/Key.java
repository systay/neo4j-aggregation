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

import java.util.HashMap;
import java.util.Set;

public class Key
{
    private HashMap<String, Object> keys = new HashMap<String, Object>();

    public Key()
    {
    }

    public void addKey( String keyName, Object key)
    {
        keys.put( keyName, key );
    }

    public Object getKey( String key )
    {
        return keys.get( key );
    }

    public Set<String> getKeyNames()
    {
        return keys.keySet();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Key key = (Key)o;

        if ( !getKeyNames().equals( ( (Key)o ).getKeyNames() ) )
        {
            return false;
        }

        for ( String keyName : getKeyNames() )
        {
            if ( !getKey( keyName ).equals( ( (Key)o ).getKey( keyName ) ) )
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        for ( Object o : keys.values() )
        {
            result = 31 * result + o.hashCode();
        }
        for ( String keyName : keys.keySet() )
        {
            result = 31 * result + keyName.hashCode();
        }

        return result;
    }
}
