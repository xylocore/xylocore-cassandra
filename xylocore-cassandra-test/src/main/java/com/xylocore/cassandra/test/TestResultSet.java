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


package com.xylocore.cassandra.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class TestResultSet
    implements
        ResultSet
{
    //
    // Nested classes
    //
    
    
    private static class FetchingState
    {
        public final Integer                nextStart;
        public final ListenableFuture<Void> inProgress;

        FetchingState( Integer                  aNextStart,
                       ListenableFuture<Void>   aInProgress )
        {
            assert ( aNextStart == null ) != ( aInProgress == null );
            
            nextStart  = aNextStart;
            inProgress = aInProgress;
        }
    }
    
    
    
    
    //
    // Members
    //
    
    
    private Queue<Row>              currentPage;
    private final Queue<Queue<Row>> nextPages;
    private volatile FetchingState  fetchState;
    private final int               totalRows;
    private final int               fetchSize;
    private final long              delay;
    private int                     nextRowId;
    
    
    
    
    //
    // Class implementation
    //
    

    public static void main( String[] args )
    {
        try
        {
            int  myLoop  = 0;
            long myIndex = 0;
            
            while ( true )
            {
                int           myCount     = 100_000_000;
                TestResultSet myResultSet = new TestResultSet( myLoop*myCount, myCount, 347, 0 );
                
                while( myResultSet.one() != null )
                {
                    myIndex++;
                    
                    if ( myIndex % 1_000_000 == 0 )
                    {
                        System.err.println( myIndex );
                    }
                    
                    myResultSet.fetchMoreResults();
                }
            }
        }
        catch ( Exception myException )
        {
            myException.printStackTrace();
        }
    }
    
    
    
    public TestResultSet( int    aStartingRowId,
                          int    aTotalRows,
                          int    aFetchSize,
                          long   aDelay          )
    {
        nextRowId = aStartingRowId;
        totalRows = aTotalRows;
        fetchSize = aFetchSize;
        delay     = aDelay;
        
        currentPage = createPage( Math.min( aTotalRows, aFetchSize ) );
        nextPages   = new ConcurrentLinkedQueue<>();
        
        if ( ! currentPage.isEmpty() )
        {
            fetchState = new FetchingState( currentPage.size(), null );
        }
    }

    
    
    private Queue<Row> createPage( int aPageSize )
    {
        Queue<Row> myPage = new LinkedBlockingQueue<>();
        
        for ( int i = 0 ; i < aPageSize ; i++ )
        {
            myPage.offer( new TestRow( nextRowId++ ) );
        }
        
        return myPage;
    }
    
    
    @Override
    public void forEach( Consumer<? super Row> aAction )
    {
        Row myRow;
        
        while ( (myRow = one()) != null )
        {
            aAction.accept( myRow );
        }
    }


    @Override
    public Spliterator<Row> spliterator()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public ColumnDefinitions getColumnDefinitions()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isExhausted()
    {
        prepareNextRow();
        return currentPage.isEmpty();
    }


    @Override
    public Row one()
    {
        prepareNextRow();
        return currentPage.poll();
    }


    @Override
    public List<Row> all()
    {
        if ( isExhausted() )
        {
            return Collections.emptyList();
        }
        
        List<Row> myRows = new ArrayList<>( getAvailableWithoutFetching() );
        Row       myRow;
        
        while ( (myRow = one()) != null )
        {
            myRows.add( myRow );
        }
        
        return myRows;
    }


    @Override
    public Iterator<Row> iterator()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public int getAvailableWithoutFetching()
    {
        int myAvailable = currentPage.size();
        for ( Queue<Row> myPage : nextPages )
        {
            myAvailable += myPage.size();
        }
        
        return myAvailable;
    }


    @Override
    public boolean isFullyFetched()
    {
        return ( fetchState == null );
    }


    @Override
    public ListenableFuture<Void> fetchMoreResults()
    {
        FetchingState myFetchState = fetchState;
        
        if ( myFetchState == null )
        {
            return Futures.immediateFuture( null );
        }
        
        if ( myFetchState.inProgress != null )
        {
            return myFetchState.inProgress;
        }

        if ( myFetchState.nextStart == null )
        {
            System.err.println();
        }
        assert myFetchState.nextStart != null;
        
        SettableFuture<Void> myFuture = SettableFuture.create();
        Integer              myState  = myFetchState.nextStart;
        
        fetchState = new FetchingState( null, myFuture );
        
        return queryNextPage( myState, myFuture );
    }


    @Override
    public ExecutionInfo getExecutionInfo()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<ExecutionInfo> getAllExecutionInfo()
    {
        throw new UnsupportedOperationException();
    }
    
    
    private void prepareNextRow()
    {
        while ( currentPage.isEmpty() )
        {
            // Grab the current state now to get a consistent view in this iteration.
            FetchingState myFetchingState = fetchState;

            Queue<Row> myNextPage = nextPages.poll();
            if ( myNextPage != null )
            {
                currentPage = myNextPage;
                continue;
            }
            
            if ( myFetchingState == null )
            {
                return;
            }
            
            try
            {
                Uninterruptibles.getUninterruptibly( fetchMoreResults() );
            }
            catch ( ExecutionException e )
            {
                throw new RuntimeException( e.getCause() );
            }
        }
    }



    private ListenableFuture<Void> queryNextPage( Integer                aNextStart,
                                                  SettableFuture<Void>   aFuture     )
    {
        int myPageSize = Math.min( totalRows - aNextStart, fetchSize );
        
        CompletableFuture.supplyAsync( () -> { return createPage( myPageSize ); } )
                         .whenComplete
                          (
                              ( myData, myException ) ->
                                  {
                                      if ( delay != 0 )
                                      {
                                          sleep( delay );
                                      }
                                      
                                      if ( myData != null )
                                      {
                                          if ( ! myData.isEmpty() )
                                          {
                                              nextPages.offer( myData );
                                              fetchState = new FetchingState( aNextStart+myPageSize, null );
                                          }
                                          else
                                          {
                                              fetchState = null;
                                          }
                                          
                                          aFuture.set( null );
                                      }
                                      else
                                      {
                                          aFuture.setException( myException );
                                      }
                                  }
                          );
        
        return aFuture;
    }
    
    
    private static void sleep( long aDelay )
    {
        try
        {
            Thread.sleep( aDelay );
        }
        catch ( InterruptedException myInterruptedException )
        {
        }
    }
}
