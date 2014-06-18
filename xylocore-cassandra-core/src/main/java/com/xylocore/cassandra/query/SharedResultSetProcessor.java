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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;



/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

class SharedResultSetProcessor<T>
{
    //
    // Constants
    //
    
    
    private enum State
    {
        Inactive,
        ProcessingResults,
        AwaitingFetch,
        AwaitingTask,
        Completing,
        Complete
    };
    
    
    
    
    //
    // Nested interfaces
    //
    
    
    public interface ResultSetCompletionNotifier<T>
    {
        public void notify( SharedResultSetProcessor<T>   aProcessor,
                            boolean                       aComplete   );
    }
    
    
    
    
    //
    // Nested classes
    //
    
    
    private class WorkTask
    {
        private final int   id;
        private List<Row>   rows;
        private List<T>     entities;
        
        public WorkTask( int aId )
        {
            id       = aId;
            rows     = new ArrayList<>( executionContext.getSliceSize() );
            entities = new ArrayList<>( executionContext.getSliceSize() );
        }
        
        public List<Row> getRows()
        {
            return rows;
        }
        
        public List<T> getEntities()
        {
            return entities;
        }
        
        public void clean()
        {
            rows.clear();
            
            if ( ! executionContext.isReuseEntity() )
            {
                entities.clear();
            }
        }
        
        @Override
        public String toString()
        {
            return "WorkTask[id: " + id + ", rowCount: " + rows.size() + "]";
        }
    }
    
    
    
    
    //
    // Members
    //
    

    private static final Logger                     logger;
    private static final Set<State>                 setResultSetExpectedStates;
    
    private final PagedQueryExecutionContext<T>     executionContext;
    private final Executor                          executor;
    private final Queue<WorkTask>                   availableTasks;
    private final AtomicInteger                     availableTaskCount;
    private final AtomicReference<State>            state;
    private final AtomicBoolean                     workTasksLocked;
    private volatile ListenableFuture<Void>         fetchFuture;
    private volatile boolean                        completed;
    private volatile WorkTask                       currentWorkTask;
    private volatile ResultSet                      resultSet;
    private volatile ResultSetCompletionNotifier<T> completionNotifier;
    
    
    
    
    //
    // Static initializer
    //
    
    static
    {
        logger = LoggerFactory.getLogger( SharedResultSetProcessor.class );
        
        Set<State> myStateSet;
        
        myStateSet = new HashSet<State>();
        myStateSet.add( State.Inactive   );
        myStateSet.add( State.Completing );
        
        setResultSetExpectedStates = Collections.unmodifiableSet( myStateSet );
    }
    
    
    
    
    //
    // Class implementation
    //
    
    
    /**
     * FILLIN
     * 
     * @param       aExecutionContext
     * @param       aExecutor
     * @param       aCompletionNotifier
     */
    SharedResultSetProcessor( PagedQueryExecutionContext<T>   aExecutionContext,
                              Executor                        aExecutor,
                              ResultSetCompletionNotifier<T>  aCompletionNotifier )
    {
        Validate.notNull( aExecutionContext   );
        Validate.notNull( aCompletionNotifier );
        
        executionContext   = aExecutionContext;
        executor           = aExecutor;
        completionNotifier = aCompletionNotifier;
        availableTasks     = new ConcurrentLinkedQueue<>();
        availableTaskCount = new AtomicInteger( aExecutionContext.getConcurrencyLevel() );
        state              = new AtomicReference<>( State.Inactive );
        workTasksLocked    = new AtomicBoolean( false );
        fetchFuture        = null;
        completed          = false;
        resultSet          = null;
        currentWorkTask    = null;
        
        for ( int i = 0, ci = aExecutionContext.getConcurrencyLevel() ; i < ci ; i++ )
        {
            WorkTask myWorkTask = new WorkTask( i );
            
            availableTasks.offer( myWorkTask );
        }
    }
    
    
    /**
     * Sets the next result set for the processor to process. This method should only
     * be called to initially start processing or once a result set completion
     * notification has been received. Additionally, this method should not be called
     * once {@link #complete()} has been called.
     * 
     * @param       aResultSet
     *                  The result set to process.
     */
    public void setResultSet( ResultSet aResultSet )
    {
        logger.debug( "setResultSet() entered" );

        try
        {
            Validate.notNull( aResultSet, "result set missing" );
            
            // There cannot be an active result set
            if ( resultSet != null )
            {
                String myMessage = "the processor currently has an active result set";
                
                logger.error( myMessage );
                
                throw new IllegalStateException( myMessage );
            }
            
            // The processor cannot be marked as completed
            if ( completed )
            {
                String myMessage = "the result set processor has already been marked as completed";
                
                logger.error( myMessage );
                
                throw new IllegalStateException( myMessage );
            }

            transitionState( setResultSetExpectedStates, State.ProcessingResults );

            // Set the new active result set
            resultSet = aResultSet;
            
            // start processing the active result set
            processResults();
        }
        finally
        {
            logger.debug( "setResultSet() exited" );
        }
    }
    
    
    /**
     * 
     */
    private void processResults()
    {
        assert resultSet  != null;
        assert getState() == State.ProcessingResults;
        
        do
        {
            generateWorkTasks();
            
        } while ( fetchFuture              == null                                  &&
                  availableTaskCount.get() != 0                                     &&
                  compareAndSetState( State.AwaitingTask, State.ProcessingResults )    );
        
        // Has a fetch been initiated?
        if ( fetchFuture != null )
        {
            // Make the transition to the AwaitingFetch state
            transitionToFetchState();
        }
        else
        {
            // Release the active result set
            resultSet = null;

            // Determine the next state. If all of the work tasks are back in queue, the
            // processor should be in the Inactive state; otherwise, the processor should
            // be in the Completing state, waiting for the remainder of the at-large work
            // tasks to complete.
            
            State myNextState =
                    ( availableTaskCount.get() == executionContext.getConcurrencyLevel() )
                            ? State.Inactive
                            : State.Completing;

            // Only make the state transition if the current state is ProcessingResults. It
            // is possible that the state could be AwaitingTask
            if ( compareAndSetState( State.ProcessingResults, myNextState ) )
            {
                logger.debug( "result set processing finished - moving to '{}' state", myNextState );

                performCompletionNotification();
            }
        }
    }
    
    
    /**
     * FILLIN
     */
    private void transitionToFetchState()
    {
        // Switch the state to AwaitingFetch
        State myOldState = setState( State.AwaitingFetch );
        
        // The old state must be ProcessingResults
        assert myOldState == State.ProcessingResults
             : String.format( "expecting state '%s',  found '%s'",
                              State.ProcessingResults,
                              myOldState                           );
        
        // Add a listener for processing the completion of the fetch. This is done
        // at the end of the processing loop so that the state can be coorect
        // (AwaitingFetch).
        
        fetchFuture.addListener
        (
            () -> { handleFetchCompletion( fetchFuture ); },
            executor
        );
    }
    

    /**
     * FILLIN
     */
    private void generateWorkTasks()
    {
        boolean myWorkTaskAssigned  = false;
        boolean myMoreRowsAvailable = false;
        
        do
        {
            myWorkTaskAssigned = assignWorkTask();
            if ( myWorkTaskAssigned )
            {
                final boolean   myFullyFetched   = resultSet.isFullyFetched();
                final int       myAvailableRows  = resultSet.getAvailableWithoutFetching();
                final int       mySliceSize      = executionContext.getSliceSize();
                final int       myFetchThreshold = executionContext.getFetchThreshold();
                final List<Row> myRows           = currentWorkTask.getRows();
                final int       myStartRowCount  = myRows.size();
                final int       myNewRowCount    = Math.min( myAvailableRows, mySliceSize-myStartRowCount );
                
                for ( int i = 0 ; i < myNewRowCount ; i++ )
                {
                    if
                    (
                        myFetchThreshold != 0                 &&
                        myFetchThreshold == myAvailableRows-i &&
                        ! myFullyFetched
                    )
                    {
                        initiateFetch();
                    }
                    
                    myRows.add( resultSet.one() );
                }
                
                final int myTotalRowCount = myStartRowCount+myNewRowCount;
                
                if ( logger.isDebugEnabled() )
                {
                    logger.debug( "fullyFetched: {}, availRows: {}, sliceSize: {}, fetchThr: {}, src: {}, nrc: {}, trc: {}",
                                  myFullyFetched,
                                  myAvailableRows,
                                  mySliceSize,
                                  myFetchThreshold,
                                  myStartRowCount,
                                  myNewRowCount,
                                  myTotalRowCount   );
                }
                
                if
                (
                    myTotalRowCount == mySliceSize ||
                    (
                        myNewRowCount == myAvailableRows &&
                        (
                            myFullyFetched                        ||
                            ! executionContext.isForceFullSlice()
                        )
                    )
                )
                {
                    scheduleCurrentWorkTask();
                }
                
                if
                (
                    myFetchThreshold == 0               &&
                    myNewRowCount    == myAvailableRows &&
                    ! myFullyFetched
                )
                {
                    initiateFetch();
                }
                
                myMoreRowsAvailable = ( ! myFullyFetched || myNewRowCount != myAvailableRows );
            }
            
        } while ( myWorkTaskAssigned && myMoreRowsAvailable );
    }
    

    /**
     * FILLIN
     * 
     * @return
     */
    private boolean assignWorkTask()
    {
        boolean myDebug = logger.isDebugEnabled();
        
        if ( currentWorkTask == null )
        {
            if ( ! resultSet.isExhausted() )
            {
                lockWorkTasks();
                
                try
                {
                    // Are there any work tasks available?
                    if ( availableTaskCount.get() == 0 )
                    {
                        //
                        // Move the state to 'AwaitingTask' if still in 'ProcessingResults' state.
                        // State could have already transitioned to 'AwaitingFetch' if a fetch has
                        // been issued.
                        //
                        
                        if
                        (
                            fetchFuture == null                                               &&
                            compareAndSetState( State.ProcessingResults, State.AwaitingTask )
                        )
                        {
                            if ( myDebug )
                            {
                                logger.debug( "no work tasks are available - transitioning to '{}' state",
                                              State.AwaitingTask                                           );
                            }
                        }
                        else
                        {
                            if ( myDebug )
                            {
                                logger.debug( "no work tasks are available but fetch is pending" );
                            }
                        }
                    }
                    else
                    {
                        currentWorkTask = availableTasks.poll();
                        availableTaskCount.decrementAndGet();
                    }
                }
                finally
                {
                    unlockWorkTasks();
                }
            }
        }
        
        return ( currentWorkTask != null );
    }
    

    /**
     * Initiates an asynchronous fetch of results from Cassandra.
     */
    private void initiateFetch()
    {
        logger.debug( "initiating fetch" );
        
        try
        {
            fetchFuture = resultSet.fetchMoreResults();
        }
        finally
        {
            logger.debug( "fetch initiation complete" );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aFuture
     *                  The future that represents the asynchronous request
     *                  to Cassandra. This request will be complete at this
     *                  point, either successfully or exceptionally.
     */
    private void handleFetchCompletion( ListenableFuture<Void> aFuture )
    {
        State myState = getState();
        
        assert myState == State.AwaitingFetch
             : String.format( "expecting state '%s',  found '%s'",
                              State.AwaitingFetch,
                              myState                              );
        
        logger.debug( "handling fetch completion, current state: {}", myState );
        
        try
        {
            fetchFuture = null;
            
            compareAndSetState( State.AwaitingFetch, State.ProcessingResults );
    
            processResults();
        }
        finally
        {
            logger.debug( "fetch completion handler finished" );
        }
    }
    

    /**
     * FILLIN
     */
    private void scheduleCurrentWorkTask()
    {
        WorkTask myRunnableWorkTask = currentWorkTask;
        currentWorkTask = null;

        CompletableFuture.runAsync
                          (
                              () ->
                                  {
                                      try
                                      {
                                          runWorkTask( myRunnableWorkTask );
                                          handleTaskCompletion( myRunnableWorkTask, null );
                                      }
                                      catch ( Throwable myThrowable )
                                      {
                                          handleTaskCompletion( null, myThrowable );
                                      }
                                  },
                              executor
                          );
    }
    

    /**
     * FILLIN
     * 
     * @param       aWorkTask
     */
    private void runWorkTask( WorkTask aWorkTask )
    {
        logger.debug( "running work task: {}", aWorkTask );
        
        List<Row> myRows     = aWorkTask.getRows();
        List<T>   myEntities = aWorkTask.getEntities();
        
        try
        {
            for ( int i = myEntities.size(), ci = myRows.size() ; i < ci ; i++ )
            {
                myEntities.add( executionContext.getEntityCreator().get() );
            }
            
            int myIndex = 0;
            
            for ( int i = 0, ci = myRows.size() ; i < ci ; i++ )
            {
                Row myRow    = myRows.get( i );
                T   myEntity = myEntities.get( myIndex );
                
                executionContext.getEntityExtractor().accept( myRow, myEntity );
                if ( executionContext.getEntityFilter().test( myEntity ) )
                {
                    myIndex++;
                }
                
                myRows.set( i, null );
            }
            
            List<T> myRestrictedEntities = myEntities.subList( 0, myIndex );
            
            myRows.clear();
            
            executionContext.getEntityProcessor().accept( myRestrictedEntities );
        }
        finally
        {
            aWorkTask.clean();
            
            logger.debug( "work task complete: {}", aWorkTask );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aWorkTask
     * @param       aCause
     */
    private void handleTaskCompletion( WorkTask    aWorkTask,
                                       Throwable   aCause     )
    {
        logger.debug( "handling task completion for work task {}", aWorkTask );
        
        try
        {
            // Free up any unused resources within the work task
            aWorkTask.clean();
            
            boolean myDebug            = logger.isDebugEnabled();
            boolean myProcessResults   = false;
            boolean myNotifyCompletion = false;
            
            lockWorkTasks();
            
            try
            {
                // Make the work task available for use 
                availableTasks.offer( aWorkTask );
                int myTaskCount = availableTaskCount.incrementAndGet();
                
                State myState = getState();

                if ( myDebug )
                {
                    logger.debug( "making task {} available, total now available: {}, state: {}",
                                  aWorkTask,
                                  myTaskCount,
                                  myState                                                         );
                }
                
                switch ( myState )
                {
                    case AwaitingTask:
                        
                        if ( compareAndSetState( State.AwaitingTask, State.ProcessingResults ) )
                        {
                            myProcessResults = true;
                        }
                        
                        break;
                        
                    case Completing:
                        
                        if ( myTaskCount == executionContext.getConcurrencyLevel() )
                        {
                            if ( completed )
                            {
                                setState( State.Complete );

                                myNotifyCompletion = true;
                            }
                            else
                            {
                                setState( State.Inactive );
                            }
                        }
                        
                        break;
                        
                    default:
                        
                        // Ignore other states
                        break;
                }
            }
            finally
            {
                unlockWorkTasks();
            }
            
            if ( myProcessResults )
            {
                processResults();
            }
            
            if ( myNotifyCompletion )
            {
                performCompletionNotification();
            }
        }
        finally
        {
            logger.debug( "task completion handler finished for work task {}", aWorkTask );
        }
    }
    

    /**
     * FILLIN
     */
    public void complete()
    {
        logger.debug( "marking processing as complete" );
        
        try
        {
            completed = true;
    
            if ( compareAndSetState( State.Inactive, State.Complete ) )
            {
                performCompletionNotification();
            }
        }
        finally
        {
            logger.debug( "finished marking processing as complete" );
        }
    }

    
    /**
     * FILLIN
     */
    private void performCompletionNotification()
    {
        boolean myCompleted = completed;
        
        logger.debug( "performing completion notification - processing has {}been marked complete",
                      myCompleted ? "" : "not "                                                     );
        
        completionNotifier.notify( this, myCompleted );
    }
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    private State getState()
    {
        return state.get();
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aState
     * 
     * @return
     */
    private State setState( State aState )
    {
        State myOldState = state.getAndSet( aState );
        
        logger.debug( "setting state to '{}', old state was '{}'", aState, myOldState );
        
        return myOldState;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aExpect
     * @param       aUpdate
     * 
     * @return
     */
    private boolean compareAndSetState( State   aExpect,
                                        State   aUpdate  )
    {
        boolean myFlag = state.compareAndSet( aExpect, aUpdate );
        
        logger.debug( "attempting transition from '{}' to '{}': {}",
                      aExpect,
                      aUpdate,
                      myFlag ? "succeeded" : "failed"                );
        
        return myFlag;
    }


    /**
     * FILLIN
     * 
     * @param       aExpectedStates
     * @param       aUpdateState
     */
    private void transitionState( Set<State>   aExpectedStates,
                                  State        aUpdateState     )
    {
        state.getAndUpdate
        (
            ( myState ) ->
                {
                    if ( ! aExpectedStates.contains( myState ) )
                    {
                        invalidStateTransition( aExpectedStates );
                    }
                    
                    return State.ProcessingResults;
                }
        );
    }
    

    /**
     * FILLIN
     * 
     * @param       aExpectedStates
     */
    private void invalidStateTransition( Set<State> aExpectedStates )
    {
        StringBuilder myBuilder   = new StringBuilder( "the state needs to be " );
        int           myCount     = 0;
        String        mySeparator = "";
        
        for ( State myExpectedState : aExpectedStates )
        {
            myBuilder.append( mySeparator                )
                     .append( "'"                        )
                     .append( myExpectedState.toString() )
                     .append( "'"                        )
                     ;

            myCount++;
            
            if ( myCount < aExpectedStates.size()-1 )
            {
                mySeparator = ", ";
            }
            else if ( myCount == aExpectedStates.size()-1 )
            {
                mySeparator = ( myCount == 1 ) ? " or " : ", or ";
            }
        }
        
        String myMessage = myBuilder.toString();
        
        logger.error( myMessage );
        
        throw new IllegalStateException( myMessage );
    }
    
    
    /**
     * FILLIN
     */
    private void lockWorkTasks()
    {
        while ( ! workTasksLocked.compareAndSet( false, true ) );
    }


    /**
     * FILLIN
     */
    private void unlockWorkTasks()
    {
        boolean myUnlocked = workTasksLocked.compareAndSet( true, false );
        
        assert myUnlocked : "work tasks were not locked";
    }
}
