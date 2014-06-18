//
//   Copyright 2013 The Palantir Corporation
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//


package com.xylocore.cassandra.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.cassandraunit.spring.CassandraDataSet;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.xylocore.cassandra.test.AbstractCassandraUnitTest;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

@CassandraDataSet( value = { "PagedQueryUnitTest-dataset.cql" } )
public class PagedQueryUnitTest
    extends
        AbstractCassandraUnitTest
{
    //
    // Members
    //
    
    
    private PreparedStatement       firstQuery;
    private List<PreparedStatement> nextQueries;

    
    
    
    //
    // Class implementation
    //
    
    
    @Override
    public void setup()
    {
        super.setup();
        
        firstQuery  = session.prepare( "select distinct partition_key, token(partition_key) from paged_query_1 limit :queryLimit" );
        nextQueries = Arrays.asList( session.prepare( "select distinct partition_key, token(partition_key) from paged_query_1 where token(partition_key) > :lastToken limit :queryLimit" ) );
        
        PreparedStatement myPS = session.prepare( "insert into paged_query_1 ( partition_key, cluster_key, val ) values ( ?, ?, ? )" );
        
        for ( long i = 0 ; i < 100 ; i++ )
        {
            BatchStatement myBS = new BatchStatement();
            
            for ( long j = 0 ; j < 10 ; j++ )
            {
                myBS.add( myPS.bind( i, j, i+"-"+j ) );
            }
            
            session.execute( myBS );
        }
    }

    
    static class Entity
    {
        private long    token;
        private long    partitionKey;
        private long    clusterKey;
        private String  val;
        
        public Entity() {}
        public long getToken() { return token; }
        public void setToken( long aToken ) { token = aToken; }
        public long getPartitionKey() { return partitionKey; }
        public void setPartitionKey( long aPartitionKey ) { partitionKey = aPartitionKey; }
        public long getClusterKey() { return clusterKey; }
        public void setClusterKey( long aClusterKey ) { clusterKey = aClusterKey; }
        public String getVal() { return val; }
        public void setVal( String aVal ) { val = aVal; }
        
        @Override
        public String toString()
        {
            StringBuilder myBuilder = new StringBuilder();
            myBuilder.append( super.toString()   )
                     .append( " { token: "       )
                     .append( token              )
                     .append( ", partitionKey: " )
                     .append( partitionKey       )
                     .append( ", clusterKey: "   )
                     .append( clusterKey         )
                     .append( ", val: "          )
                     .append( val                )
                     .append( " }"               )
                     ;
            
            return myBuilder.toString();
        }
    }
    
    
    @Test
    public void doit()
            throws ExecutionException,
                   InterruptedException
    {
        Map<String,String> myKeyColumnNames = new HashMap<>();
        myKeyColumnNames.put( "token(partition_key)", "lastToken" );
        
        List<Entity> myEntities = new ArrayList<Entity>();
        
        Supplier<Entity> myEntityCreator =
                () ->
                    {
                        return new Entity();
                    };
        
        BiConsumer<Row,Entity> myEntityExtractor =
                ( myRow, myEntity ) ->
                    {
                        myEntity.setPartitionKey( myRow.getLong( "partition_key"        ) );
                        myEntity.setToken       ( myRow.getLong( "token(partition_key)" ) );
                    };
                    
        Consumer<List<Entity>> myEntityProcessor =
                ( myPageEntities ) ->
                    {
                        myEntities.addAll( myPageEntities );
                    };
        
        PagedQueryExecutionContext<Entity> myExecutionContext =
                PagedQueryExecutionContextBuilder.builder        ( Entity.class      )
                                                 .entityCreator  ( myEntityCreator   )
                                                 .entityExtractor( myEntityExtractor )
                                                 .entityProcessor( myEntityProcessor )
                                                 .build();
        
        PagedQuery<Entity> myPagedQuery =
                new PagedQuery<>( session,
                                  ForkJoinPool.commonPool(),
                                  firstQuery,
                                  nextQueries,
                                  myKeyColumnNames           );

        CompletableFuture<Void> myFuture = myPagedQuery.execute( myExecutionContext );
        
        myFuture.get();
        
        assertThat( myEntities.size(), equalTo( 100 ) );
    }
}
