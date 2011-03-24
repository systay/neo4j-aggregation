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


import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class AggregatorTest
{
    private EmbeddedGraphDatabase db;
    private Node company;

    @After
    public void destroy()
    {
        db.shutdown();
    }


    @Before
    public void init()
    {
        File directory = new File( "target/aggregation" );
        if ( directory.exists() )
        {
            delete( directory );
        }
        db = new EmbeddedGraphDatabase( "target/aggregation" );

        createData();
    }

    @Test
    public void groupByNodeAggregateOnNodeProperty()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT countryId, SUM(salary)
        // FROM employees
        // GROUP BY countryId

        Traverser employees = getTraversalDescription();

        Grouping grouping = Grouping.description().
                groupByNode( 0, "country" ).
                groupFrom( employees );

        Map<Key, Double> result = grouping.aggregateNodeProperty( -1, "salary", Aggregate.Sum() );

        assertResultContains( result, "country", "Finland", equalTo( (double)186666 ) );
        assertResultContains( result, "country", "Sweden", equalTo( (double)160000 ) );
    }

    @Test
    public void groupByNodeAggregateOnNodeProperty2()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departmentId, SUM(salary)
        // FROM employees
        // GROUP BY departmentId

        Traverser employees = getTraversalDescription();

        Grouping grouping = Grouping.description().
                groupByNode( 1, "department" ).
                groupFrom( employees );

        Map<Key, Double> result = grouping.aggregateNodeProperty( -1, "salary", Aggregate.Sum() );

        assertResultContains( result, "department", "C", equalTo( (double)280000 ) );
        assertResultContains( result, "department", "D", equalTo( (double)66666 ) );
    }

    @Test
    public void groupByNodeAggregateNode()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departmentId, COUNT(*)
        // FROM employees
        // GROUP BY departmentId

        Grouping grouping = Grouping.description().
                groupByNode( 1, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Integer> result = grouping.aggregateNode( -1, Aggregate.Count() );

        assertResultContains( result, "department", "D", equalTo( 2 ) );
        assertResultContains( result, "department", "C", equalTo( 3 ) );
    }

    @Test
    public void groupByTwoNodesAggregateNode()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departmentId, countryId, COUNT(*)
        // FROM employees
        // GROUP BY departmentId, countryId

        Grouping grouping = Grouping.description().
                groupByNode( 1, "department" ).
                groupByNode( 0, "country" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Integer> result = grouping.aggregateNode( -1, Aggregate.Count() );

        assertThat( result.size(), equalTo( 3 ) );
    }


    @Test
    public void groupByNodeAttribute()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departments.department, SUM(salary)
        // FROM employees JOIN departments ON departmentId = departments.id
        // GROUP BY departments.department

        Grouping grouping = Grouping.description().
                groupByNodeProperty( 1, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Integer> result = grouping.aggregateNode( -1, Aggregate.Count() );
        assertThat( result.size(), equalTo( 2 ) );
    }

    @Test
    public void groupByRelationProperty()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departments.department, SUM(salary)
        // FROM employees JOIN employeeDepartment ON employees.employeeId = employeeDepartment.employeeId
        // GROUP BY employeeDepartment.position

        Grouping grouping = Grouping.description().
                groupByRelationProperty( RelTypes.WORKS_FOR, "position" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Integer> result = grouping.aggregateNode( -1, Aggregate.Count() );
        assertThat( result.size(), equalTo( 2 ) );
    }

    @Test
    public void groupByNodeAvgAggregation()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departmentId, AVG(salary)
        // FROM employees
        // GROUP BY departmentId

        Grouping grouping = Grouping.description().
                groupByNode( 1, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Double> result = grouping.aggregateNodeProperty( -1, "salary", Aggregate.Avg() );

        assertResultContains( result, "department", "D", equalTo( (double)33333 ) );
    }

    @Test
    public void groupByNodeMaxAggregation()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departmentId, Max(salary)
        // FROM employees
        // GROUP BY departmentId

        Grouping grouping = Grouping.description().
                groupByNode( 1, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Double> result = grouping.aggregateNodeProperty( -1, "salary", Aggregate.Max() );

        assertResultContains( result, "department", "D", equalTo( (double)54321 ) );
    }

    @Test
    public void groupByNodeMinAggregation()
    {
        // This is the eqvivalent of this SQL query:
        // SELECT departmentId, MIN(salary)
        // FROM employees
        // GROUP BY departmentId

        Grouping grouping = Grouping.description().
                groupByNode( 1, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Double> result = grouping.aggregateNodeProperty( -1, "salary", Aggregate.Min() );

        assertResultContains( result, "department", "D", equalTo( (double)12345 ) );
    }

    @Test
    public void groupByNodeJoinAggregation()
    {
        // This is the eqvivalent of this MySQL query:
        // SELECT departmentId, GROUP_CONCAT(employee SEPARATOR ', ')
        // FROM employees
        // GROUP BY departmentId

        Grouping grouping = Grouping.description().
                groupByNode( 1, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, String> result = grouping.aggregateNodeProperty( -1, "employee", Aggregate.Join( ", " ) );

        String stringResult = getResult( result, "department", "D" );

        Assert.assertTrue( stringResult.contains( "David" ));
        Assert.assertTrue( stringResult.contains( "Emil" ));
    }

    private String getResult( Map<Key, String> result, String key,
                              String keyValue )
    {
        for ( Key groupingKey : result.keySet() )
        {
            Node groupingNode = (Node)groupingKey.getKey( key );
            String nodeKeyValue = (String)groupingNode.getProperty( key );
            if ( nodeKeyValue.equals( keyValue ) )
            {
                return result.get( groupingKey );
            }
        }

        throw new IllegalArgumentException( "No such result found" );
    }


    @Test
    public void groupByRelationShipEndNode()
    {
        // This is another way of doing the same SQL query:
        // SELECT departmentId, COUNT(*)
        // FROM employees
        // GROUP BY departmentId
        //
        // Instead of using an node offset for the grouping, we use the end node
        // of the first relationship found of the given type. Use this if your paths
        // are variable in structure.

        Grouping grouping = Grouping.description().
                groupByRelationEndNode( RelTypes.WORKS_FOR, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Integer> result = grouping.aggregateNode( -1, Aggregate.Count() );

        assertResultContains( result, "department", "D", equalTo( 2 ) );
    }

    @Test
    public void groupByRelationShipStartNode()
    {
        // This is another way of doing the same SQL query:
        // SELECT departmendId, COUNT(*)
        // FROM employees
        // GROUP BY departmendId
        //
        // Instead of using an node offset for the grouping, we use the end node
        // of the first relationship found of the given type. Use this if your paths
        // are variable in structure.

        Grouping grouping = Grouping.description().
                groupByRelationStartNode( RelTypes.DEPARTMENT_OF, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, Integer> result = grouping.aggregateNode( -1, Aggregate.Count() );

        assertResultContains( result, "department", "D", equalTo( 2 ) );
    }

    @Test
    public void getAllGroupedNodes()
    {
        // In SQL it's impossible to have a select-query that returns a set in a set,
        // so this is harder to explain. This query collects all matching employee-nodes
        // in a List<Node>, grouped by the department the work in.

        Grouping grouping = Grouping.description().
                groupByNodeProperty( 1, "department" ).
                groupFrom( getTraversalDescription() );

        Map<Key, List<Node>> result = grouping.aggregateNode( -1, Aggregate.Collect() );

        int cSize = result.get( createKey( "C" ) ).size();
        int dSize = result.get( createKey( "D" ) ).size();

        assertThat( cSize, equalTo( 3 ) );
        assertThat( dSize, equalTo( 2 ) );
    }

    private Key createKey( String value )
    {
        Key key = new Key();
        String property = "department";
        key.addKey( property, value );
        return key;
    }

    private <T> void assertResultContains( Map<Key, T> result,
                                           String key,
                                           String keyValue,
                                           Matcher<T> integerMatcher )
    {
        for ( Key groupingKey : result.keySet() )
        {
            Node groupingNode = (Node)groupingKey.getKey( key );
            String nodeKeyValue = (String)groupingNode.getProperty( key );
            if ( nodeKeyValue.equals( keyValue ) )
            {
                assertThat( result.get( groupingKey ), integerMatcher );
                return;
            }
        }

        fail( "No matching node found" );
    }

    private Traverser getTraversalDescription()
    {
        return Traversal.description()
                .relationships( RelTypes.DEPARTMENT_OF, Direction.INCOMING )
                .relationships( RelTypes.WORKS_FOR, Direction.INCOMING )
                .relationships( RelTypes.LIVES_IN, Direction.OUTGOING )
                .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL )
                .filter( Traversal.returnWhereLastRelationshipTypeIs( RelTypes.LIVES_IN ) )
                .traverse( company );
    }

    private void addEmployee( String name, float salary, Node country,
                              Node department, String position )
    {
        Node employee = db.createNode();
        employee.setProperty( "employee", name );
        employee.setProperty( "salary", salary );
        employee.createRelationshipTo( country, RelTypes.LIVES_IN );
        Relationship worksFor = employee.createRelationshipTo( department, RelTypes.WORKS_FOR );
        worksFor.setProperty( "position", position );
    }

    private Node addDepartment( Node company, String departmentName )
    {
        Node department = db.createNode();
        department.setProperty( "department", departmentName );
        department.createRelationshipTo( company, RelTypes.DEPARTMENT_OF );
        return department;
    }


    private Node createCountry( String countryName )
    {
        Node country = db.createNode();
        country.setProperty( "country", countryName );
        return country;
    }

    private void createData()
    {
        Transaction transaction = db.beginTx();
        company = db.createNode();
        Node sweden = createCountry( "Sweden" );
        Node finland = createCountry( "Finland" );

        Node departmentC = addDepartment( company, "C" );
        Node departmentD = addDepartment( company, "D" );

        addEmployee( "Anders", 10000, sweden, departmentC, "boss" );
        addEmployee( "Ceasar", 150000, sweden, departmentC, "dev" );
        addEmployee( "Emil", 54321, finland, departmentD, "dev" );
        addEmployee( "Bertil", 120000, finland, departmentC, "dev" );
        addEmployee( "David", 12345, finland, departmentD, "boss" );
        transaction.success();
        transaction.finish();
    }


    private static boolean delete( File dir )
    {
        if ( dir.isDirectory() )
        {
            for ( String childDir : dir.list() )
            {
                boolean success = delete( new File( dir, childDir ) );
                if ( !success )
                {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }
}
