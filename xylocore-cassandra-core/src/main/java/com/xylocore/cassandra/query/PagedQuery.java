//
//   Copyright 2013 The Palantir Corporataaion
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class PagedQuery<T>
{
    //
    // Nested classes
    //
    
    
    private class QueryContext
    {
        private PagedQueryExecutionContext<T>   executionContext;
        private Map<String,Object>              parameters;
        private CompletableFuture<Void>         future;
        private int                             queryLevel;
        private ResultSet                       resultSet;
        
        public QueryContext( PagedQueryExecutionContext<T>   aExecutionContext,
                             Map<String,Object>              aParameters        )
        {
            if ( aParameters == null )
            {
                aParameters = Collections.emptyMap();
            }

            executionContext = aExecutionContext;
            parameters       = new HashMap<>( aParameters );
            future           = new CompletableFuture<>();
            queryLevel       = 0;
        }

        public PagedQueryExecutionContext<T> getExecutionContext()
        {
            return executionContext;
        }
        
        public Map<String,Object> getParameters()
        {
            return parameters;
        }
        
        public CompletableFuture<Void> getFuture()
        {
            return future;
        }

        public int getQueryLevel()
        {
            return queryLevel;
        }
        
        public boolean nextQueryLevel()
        {
            queryLevel++;
            
            return ( queryLevel < nextQueries.size() );
        }
        
        public void resetQueryLevel()
        {
            queryLevel = 0;
        }
        
        public ResultSet getResultSet()
        {
            return resultSet;
        }
        
        public void setResultSet( ResultSet aResultSet )
        {
            resultSet = aResultSet;
        }
    }

    
    
    
    //
    // Members
    //
    
    
    private static final Logger     logger      = LoggerFactory.getLogger( PagedQuery.class );

    private Session                 session;
    private Executor                executor;
    private PreparedStatement       firstQuery;
    private List<PreparedStatement> nextQueries;
    private Map<String,String>      keyColumnNames;
    
    
    
    
    //
    // Class implementation
    //
    
    
    /**
     * FILLIN
     * 
     * @param       aSession
     * @param       aExecutor
     * @param       aFirstQuery
     * @param       aNextQueries
     * @param       aKeyColumnNames
     */
    PagedQuery( Session                   aSession,
                Executor                  aExecutor,
                PreparedStatement         aFirstQuery,
                List<PreparedStatement>   aNextQueries,
                Map<String,String>        aKeyColumnNames )
    {
        Validate.notNull       ( aSession        );
        Validate.notNull       ( aFirstQuery     );
        Validate.notEmpty      ( aNextQueries    );
        Validate.noNullElements( aNextQueries    );
        Validate.notEmpty      ( aKeyColumnNames );
        
        if ( aExecutor == null )
        {
            aExecutor = ForkJoinPool.commonPool();
        }
        
        session        = aSession;
        executor       = aExecutor;
        firstQuery     = aFirstQuery;
        nextQueries    = new ArrayList<>( aNextQueries );
        keyColumnNames = Collections.emptyMap();
        
        if ( aKeyColumnNames != null && ! aKeyColumnNames.isEmpty() )
        {
            keyColumnNames = new HashMap<>( aKeyColumnNames );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aExecutionContext
     * 
     * @return
     */
    public CompletableFuture<Void> execute( PagedQueryExecutionContext<T> aExecutionContext )
    {
        return execute( aExecutionContext, null );
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aExecutionContext
     * @param       aParameters
     * 
     * @return
     */
    public CompletableFuture<Void> execute( PagedQueryExecutionContext<T>   aExecutionContext,
                                            Map<String,Object>              aParameters        )
    {
        Validate.notNull( aExecutionContext );
        
        QueryContext myQueryContext =
                new QueryContext( aExecutionContext,
                                  aParameters        );
        
        myQueryContext.getParameters().put( "queryLimit", new Integer( aExecutionContext.getQueryLimit() ) );
        
        performQuery( myQueryContext, this::getStartQueryBinder );
        
        return myQueryContext.getFuture();
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * 
     * @return
     */
    private BoundStatement getStartQueryBinder( QueryContext aQueryContext )
    {
        return bindQuery( aQueryContext, firstQuery );
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * 
     * @return
     */
    private BoundStatement getNextQueryBinder( QueryContext aQueryContext )
    {
        return bindQuery( aQueryContext, nextQueries.get( aQueryContext.getQueryLevel() ) );
    }

    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * @param       aQuery
     * 
     * @return
     */
    private BoundStatement bindQuery( QueryContext        aQueryContext,
                                      PreparedStatement   aQuery         )
    {
        BoundStatement myStatement = aQuery.bind();
        
        for ( String myParameterName : aQueryContext.getParameters().keySet() )
        {
            if ( aQuery.getVariables().contains( myParameterName ) )
            {
                DataType myParameterType  = aQuery.getVariables().getType( myParameterName );
                Object   myParameterValue = aQueryContext.getParameters().get( myParameterName );
                
                bindParameter( myStatement, myParameterName, myParameterType, myParameterValue );
            }
        }
        
        return myStatement;
    }
    

    /**
     * FILLIN
     * 
     * @param       aStatement
     * @param       aParameterName
     * @param       aParameterType
     * @param       aParameterValue
     */
    private void bindParameter( BoundStatement   aStatement,
                                String           aParameterName,
                                DataType         aParameterType,
                                Object           aParameterValue )
    {
        switch ( aParameterType.getName() )
        {
            case ASCII:     aStatement.setString ( aParameterName, (String)      aParameterValue ); break; // (1,  String.class),
            case BIGINT:    aStatement.setLong   ( aParameterName, (Long)        aParameterValue ); break; // (2,  Long.class),
            case BLOB:      aStatement.setBytes  ( aParameterName, (ByteBuffer)  aParameterValue ); break; // (3,  ByteBuffer.class),
            case BOOLEAN:   aStatement.setBool   ( aParameterName, (Boolean)     aParameterValue ); break; // (4,  Boolean.class),
            case COUNTER:   aStatement.setLong   ( aParameterName, (Long)        aParameterValue ); break; // (5,  Long.class),
            case DECIMAL:   aStatement.setDecimal( aParameterName, (BigDecimal)  aParameterValue ); break; // (6,  BigDecimal.class),
            case DOUBLE:    aStatement.setDouble ( aParameterName, (Double)      aParameterValue ); break; // (7,  Double.class),
            case FLOAT:     aStatement.setFloat  ( aParameterName, (Float)       aParameterValue ); break; // (8,  Float.class),
            case INET:      aStatement.setInet   ( aParameterName, (InetAddress) aParameterValue ); break; // (16, InetAddress.class),
            case INT:       aStatement.setInt    ( aParameterName, (Integer)     aParameterValue ); break; // (9,  Integer.class),
            case TEXT:      aStatement.setString ( aParameterName, (String)      aParameterValue ); break; // (10, String.class),
            case TIMESTAMP: aStatement.setDate   ( aParameterName, (Date)        aParameterValue ); break; // (11, Date.class),
            case UUID:      aStatement.setUUID   ( aParameterName, (UUID)        aParameterValue ); break; // (12, UUID.class),
            case VARCHAR:   aStatement.setString ( aParameterName, (String)      aParameterValue ); break; // (13, String.class),
            case VARINT:    aStatement.setVarint ( aParameterName, (BigInteger)  aParameterValue ); break; // (14, BigInteger.class),
            case TIMEUUID:  aStatement.setUUID   ( aParameterName, (UUID)        aParameterValue ); break; // (15, UUID.class),
            
            default:
                
                throw new IllegalStateException( "unhandled parameter data type: " + aParameterType.getName() );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * @param       aQueryBinder
     */
    private void performQuery( QueryContext                            aQueryContext,
                               Function<QueryContext,BoundStatement>   aQueryBinder   )
    {
        BoundStatement myStatement = aQueryBinder.apply( aQueryContext );
        myStatement.setFetchSize( aQueryContext.getExecutionContext().getFetchSize() );
        
        ResultSetFuture myCassandraFuture = session.executeAsync( myStatement );
        myCassandraFuture.addListener
        (
            () ->
                {
                    processQueryCompletion( aQueryContext, myCassandraFuture );
                },
            executor
        );
    }

    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * @param       aCassandraFuture
     */
    private void processQueryCompletion( QueryContext      aQueryContext,
                                         ResultSetFuture   aCassandraFuture )
    {
        logger.debug( "entering processQueryCompletion" );
        
        try
        {
            ResultSet myResultSet = aCassandraFuture.get();

            aQueryContext.setResultSet( myResultSet );
            
            processResultSet
            (
                aQueryContext,
                ( myNoResults ) -> { resultSetComplete( aQueryContext, myNoResults ); }
            );
        }
        catch ( ExecutionException myExecutionException )
        {
            aQueryContext.getFuture().completeExceptionally( myExecutionException.getCause() );
        }
        catch ( InterruptedException myInterruptedException )
        {
            aQueryContext.getFuture().completeExceptionally( myInterruptedException );
        }
        catch ( CancellationException myCancellationException )
        {
            aQueryContext.getFuture().cancel( true );
        }
        catch ( Exception myException )
        {
            aQueryContext.getFuture().completeExceptionally( myException );
        }
        finally
        {
            logger.debug( "exiting processQueryCompletion" );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * @param       aResultSet
     * @param       aCompletionHandler
     */
    private void processResultSet( QueryContext        aQueryContext,
                                   Consumer<Boolean>   aCompletionHandler )
    {
        PagedQueryExecutionContext<T> myExecutionContext = aQueryContext.getExecutionContext();
        ResultSet                     myResultSet        = aQueryContext.getResultSet();
        boolean                       myNoResults        = true;
        
        if ( ! myResultSet.isExhausted() )
        {
            List<T> myEntities = new ArrayList<>();
            T       myEntity   = null;
            Row     myRow;
            
            myNoResults = false;
            
            if ( myExecutionContext.isReuseEntity() )
            {
                logger.debug( "creating reusable entity" );
                
                myEntity = myExecutionContext.getEntityCreator().get();
            }
            
            while ( (myRow = myResultSet.one()) != null )
            {
                if ( ! myExecutionContext.isReuseEntity() )
                {
                    logger.debug( "creating non-reusable entity" );
                    
                    myEntity = myExecutionContext.getEntityCreator().get();
                }
                
                logger.debug( "extracting row data into entity" );
                
                myExecutionContext.getEntityExtractor().accept( myRow, myEntity );
                
                if
                (
                    myExecutionContext.getEntityFilter() == null          ||
                    myExecutionContext.getEntityFilter().test( myEntity )
                )
                {
                    if ( logger.isDebugEnabled() )
                    {
                        logger.debug( "processing partition: {}", myEntity.toString() );
                    }
    
                    logger.debug( "processing entity" );
                    
                    myEntities.clear();
                    myEntities.add( myEntity );
                    
                    myExecutionContext.getEntityProcessor().accept( myEntities );
                }
                else
                {
                    if ( logger.isDebugEnabled() )
                    {
                        logger.debug( "partition filtered: {}", myEntity.toString() );
                    }
                }
                
                storeLastKey( aQueryContext, myRow );
            }
        }
        
        aCompletionHandler.accept( myNoResults );
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * @param       aEntityCount
     */
    private void resultSetComplete( QueryContext   aQueryContext,
                                    boolean        aNoResults     )
    {
        logger.debug( "resultset processing complete" );
        
        boolean myComplete = false;
        
        if ( aNoResults )
        {
            logger.debug( "checking for additional query levels" );
            
            if ( ! aQueryContext.nextQueryLevel() )
            {
                logger.debug( "no additional query levels - query complete" );
                
                aQueryContext.getFuture().complete( null );
                myComplete = true;
            }
        }
        else
        {
            logger.debug( "resetting query level" );
            
            aQueryContext.resetQueryLevel();
        }
        
        if ( ! myComplete )
        {
            logger.debug( "query not complete - performing next query using query level {}", aQueryContext.getQueryLevel() );

            performQuery( aQueryContext, this::getNextQueryBinder );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aQueryContext
     * @param       aRow
     */
    private void storeLastKey( QueryContext   aQueryContext,
                               Row            aRow           )
    {
        logger.debug( "storing key information from last retrieved row" );
        
        for ( String myColumnName : keyColumnNames.keySet() )
        {
            logger.debug( "working on column {}", myColumnName );

            int myColumnIndex = aRow.getColumnDefinitions().getIndexOf( myColumnName );
            if ( myColumnIndex != -1 )
            {
                String myParameterName = keyColumnNames.get( myColumnName );
                Object myColumnValue   = extractRowValue( aRow, myColumnIndex );
    
                if ( logger.isDebugEnabled() )
                {
                    DataType myColumnType = aRow.getColumnDefinitions().getType( myColumnIndex );
                    
                    logger.debug( "storing column in parameters: column: '{}', parameter: '{}', type: {}, value: {}",
                                  myColumnName,
                                  myParameterName,
                                  myColumnType.getName().toString(),
                                  myColumnValue.toString()                                                        );
                }
    
                aQueryContext.getParameters().put( myParameterName, myColumnValue );
            }
        }
    }
    
    
    static Object extractRowValue( Row   aRow,
                                   int   aColumnIndex )
    {
        DataType myColumnType    = aRow.getColumnDefinitions().getType( aColumnIndex );
        Object   myColumnValue;
        
        switch ( myColumnType.getName() )
        {
            case ASCII:     myColumnValue = aRow.getString ( aColumnIndex ); break; // (1,  String.class),
            case BIGINT:    myColumnValue = aRow.getLong   ( aColumnIndex ); break; // (2,  Long.class),
            case BLOB:      myColumnValue = aRow.getBytes  ( aColumnIndex ); break; // (3,  ByteBuffer.class),
            case BOOLEAN:   myColumnValue = aRow.getBool   ( aColumnIndex ); break; // (4,  Boolean.class),
            case COUNTER:   myColumnValue = aRow.getLong   ( aColumnIndex ); break; // (5,  Long.class),
            case DECIMAL:   myColumnValue = aRow.getDecimal( aColumnIndex ); break; // (6,  BigDecimal.class),
            case DOUBLE:    myColumnValue = aRow.getDouble ( aColumnIndex ); break; // (7,  Double.class),
            case FLOAT:     myColumnValue = aRow.getFloat  ( aColumnIndex ); break; // (8,  Float.class),
            case INET:      myColumnValue = aRow.getInet   ( aColumnIndex ); break; // (16, InetAddress.class),
            case INT:       myColumnValue = aRow.getInt    ( aColumnIndex ); break; // (9,  Integer.class),
            case TEXT:      myColumnValue = aRow.getString ( aColumnIndex ); break; // (10, String.class),
            case TIMESTAMP: myColumnValue = aRow.getDate   ( aColumnIndex ); break; // (11, Date.class),
            case UUID:      myColumnValue = aRow.getUUID   ( aColumnIndex ); break; // (12, UUID.class),
            case VARCHAR:   myColumnValue = aRow.getString ( aColumnIndex ); break; // (13, String.class),
            case VARINT:    myColumnValue = aRow.getVarint ( aColumnIndex ); break; // (14, BigInteger.class),
            case TIMEUUID:  myColumnValue = aRow.getUUID   ( aColumnIndex ); break; // (15, UUID.class),
            
            default:
                
                throw new IllegalStateException( "unhandled column data type: " + myColumnType.getName() );
        }
        
        return myColumnValue;
    }
}
