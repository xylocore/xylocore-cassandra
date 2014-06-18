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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.xylocore.cassandra.query.SharedResultSetProcessor.ResultSetCompletionNotifier;
import com.xylocore.cassandra.test.TestResultSet;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class SharedResultSetProcessorUnitTest
{
    //
    // Nested classes
    //
    
    
    static class Entity
    {
        private int a;
        private int b;
        private int c;
        private int d;

        public int getA() { return a; }
        public void setA( int aA ) { a = aA; }

        public int getB() { return b; }
        public void setB( int aB ) { b = aB; }

        public int getC() { return c; }
        public void setC( int aC ) { c = aC; }

        public int getD() { return d; }
        public void setD( int aD ) { d = aD; }

        @Override
        public String toString()
        {
            return super.toString() + "[a: " + a + ", b: " + b + ", c: " + c + ", d: " + d + "]";
        }
    }
    
    static class ResultSetSupplier
        implements
            ResultSetCompletionNotifier<Entity>
    {
        private final int               resultSetCount;
        private final int               rowsPerResultSet;
        private final int               fetchSize;
        private final long              delay;
        private CompletableFuture<Void> future  = new CompletableFuture<Void>();
        private int                     count   = 0;

        
        public ResultSetSupplier( int    aResultSetCount,
                                  int    aRowsPerResultSet,
                                  int    aFetchSize,
                                  long   aDelay             )
        {
            resultSetCount   = aResultSetCount;
            rowsPerResultSet = aRowsPerResultSet;
            fetchSize        = aFetchSize;
            delay            = aDelay;
            future           = new CompletableFuture<Void>();
            count            = 0;
        }
        
        public void start( SharedResultSetProcessor<Entity> aProcessor )
        {
            setResultSet( aProcessor );
        }
        
        public void join()
        {
            try
            {
                future.get();
            }
            catch ( Exception myException )
            {
            }
        }
        
        private void setResultSet( SharedResultSetProcessor<Entity> aProcessor )
        {
            ResultSet myResultSet =
                    new TestResultSet( count*rowsPerResultSet,
                                       rowsPerResultSet,
                                       fetchSize,
                                       delay                   );
            
            aProcessor.setResultSet( myResultSet );
        }
        
        @Override
        public void notify( SharedResultSetProcessor<Entity>   aProcessor,
                            boolean                            aComplete   )
        {
            if ( ! aComplete )
            {
                count++;
                
                if ( count < resultSetCount )
                {
                    setResultSet( aProcessor );
                }
                else
                {
                    aProcessor.complete();
                }
            }
            else
            {
                future.complete( null );
            }
        }
    }

    

    
    //
    // Class implementation
    //
    
    
    @Test
    public void testResultSetProcessing()
    {
        List<Entity> myCollectedEntities = new ArrayList<>();
        
        BiConsumer<Row,Entity> myEntityExtractor =
                ( myRow, myEntity ) ->
                    {
                        myEntity.setA( myRow.getInt( "a" ) );
                        myEntity.setB( myRow.getInt( "b" ) );
                        myEntity.setC( myRow.getInt( "c" ) );
                        myEntity.setD( myRow.getInt( "d" ) );
                    };
                    
        Consumer<List<Entity>> myEntityProcessor =
                ( myEntities ) ->
                    {
                        myCollectedEntities.addAll( myEntities );
                    };
        
        PagedQueryExecutionContext<Entity> myExecutionContext =
                PagedQueryExecutionContextBuilder.builder( Entity.class )
                                                 .entityCreator   ( () -> { return new Entity(); } )
                                                 .reuseEntity     ( false                          )
                                                 .entityExtractor ( myEntityExtractor              )
                                                 .entityProcessor ( myEntityProcessor              )
                                                 .sliceSize       ( 100                            )
                                                 .concurrencyLevel( 8                              )
                                                 .build();
                
        ResultSetSupplier mySupplier = new ResultSetSupplier( 10, 1_000, 91, 20 );
                
        SharedResultSetProcessor<Entity> myProcessor =
                new SharedResultSetProcessor<>
                    (
                        myExecutionContext,
                        ForkJoinPool.commonPool(),
                        mySupplier
                    );

        mySupplier.start( myProcessor );
        mySupplier.join();
        
        assertThat( myCollectedEntities.size(), equalTo( 10_000 ) );
        
        for ( int i = 0 ; i < 10_000 ; i++ )
        {
            assertThat( myCollectedEntities.get( i ).getA(), equalTo( i ) );
        }
    }
}
