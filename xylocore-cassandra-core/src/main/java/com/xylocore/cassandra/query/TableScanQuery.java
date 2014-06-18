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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableMetadata.Order;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class TableScanQuery<T>
{
    //
    // Nested classes
    //
    
    
    private static class ClusterQueryState
        implements
            PartitionKeyInfo
    {
        //
        // Members
        //
        
        private Map<String,Object>  partitionKeys   = new LinkedHashMap<>();
        private long                token;
        
        
        //
        // Class implementation
        //
        
        public void setToken( long aToken )
        {
            token = aToken;
        }
        
        public <T> void setPartitionKey( String   aKeyName,
                                         T        aKeyValue )
        {
            partitionKeys.put( aKeyName, aKeyValue );
        }
        
        public Map<String,Object> getPartitionKeys()
        {
            return partitionKeys;
        }
        

        //
        // PartitionRowInfo interface implementation
        //
        
        @Override
        public long getToken()
        {
            return token;
        }
        
        @Override
        public <T> T getPartitionKey( String aKeyName )
        {
            @SuppressWarnings( "unchecked" )
            T myValue = (T) partitionKeys.get( aKeyName );
            
            return myValue;
        }
    }
    
    
    
    
    //
    // Members
    //
    

    private static final Logger             logger                          = LoggerFactory.getLogger( TableScanQuery.class );
    private static final String             queryLimitParameterName         = "queryLimit";
    private static final String             lastTokenParameterName          = "lastToken";
    private static final String[]           keyBindNames                    = { "key0" , "key1" , "key2" , "key3" , "key4" ,
                                                                                "key5" , "key6" , "key7" , "key8" , "key9" ,
                                                                                "key10", "key11", "key12", "key13", "key14",
                                                                                "key15", "key16", "key17", "key18", "key19"  };
    
    private Session                         session;
    private Executor                        executor;
    private String                          keyspace;
    private String                          tableName;
    private TableMetadata                   tableMetadata;
    private Map<String,String>              columnsOfInterest;
    private Map<String,String>              keyParameterMappings;
    private boolean                         clusteringQueryNeeded;
    private PreparedStatement               partitionFirstPreparedStatement;
    private List<PreparedStatement>         partitionNextPreparedStatement;
    private PreparedStatement               clusterFirstPreparedStatement;
    private List<PreparedStatement>         clusterNextPreparedStatements;
    private PagedQuery<T>                   standalonePagedQuery;
    private PagedQuery<PartitionKeyInfo>    partitionPagedQuery;
    private PagedQuery<T>                   clusterPagedQuery;
    
    
    

    //
    // Class implementation
    //
    
    
//    static class Entity
//    {
//        private int id;
//        private int v;
//        
//        public int getId() { return id; }
//        public void setId( int aId ) { id = aId; }
//        public int getV() { return v; }
//        public void setV( int aV ) { v = aV; }
//        
//        @Override
//        public String toString()
//        {
//            return super.toString() + "[id: " + id + ", v: " + v + "]";
//        }
//    }

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
    
    
    public static void main( String[] args )
    {
        Cluster myCluster = null;
        
        try
        {
            myCluster =
                    Cluster.builder()
                           .addContactPoint( "127.0.0.1" )
                           .withPort( 9042 )
                           .build();
            
            Session mySession = myCluster.connect( "cirdan" );


            List<Entity> myAllEntities = new ArrayList<Entity>();
            
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
                            myEntities.forEach( e -> { logger.debug( "entity: {}", e ); } );
                            
                            myAllEntities.addAll( myEntities );
                        };
            
            TableScanQueryExecutionContext<Entity> myExecutionContext =
                    TableScanQueryExecutionContextBuilder.builder( Entity.class )
                                                         .entityCreator  ( () -> { return new Entity(); } )
                                                         .reuseEntity    ( false                          )
                                                         .entityExtractor( myEntityExtractor              )
                                                         .entityProcessor( myEntityProcessor              )
                                                         .build();
            
            TableScanQuery<Entity> myQuery =
                    TableScanQueryBuilder.<Entity>builder()
                                         .session  ( mySession )
                                         .keyspace ( "cirdan"  )
                                         .tableName( "b"       )
                                         .columns( "a", "b", "c", "d" )
//                                         .column   ( "a"       )
//                                         .column   ( "b"       )
//                                         .column   ( "c"       )
//                                         .column   ( "d"       )
                                         .build();
            
            CompletableFuture<Void> myFuture = myQuery.execute( myExecutionContext );
            myFuture.get();
        
            myAllEntities.forEach( ( e ) -> System.err.println( e.toString() ) );
        }
        catch ( Exception myException )
        {
            myException.printStackTrace();
        }
        finally
        {
            if ( myCluster != null )
            {
                myCluster.close();
            }
        }
    }

    
    
    
    /**
     * FILLIN
     * 
     * @param       aSession
     * @param       aExecutor
     * @param       aKeyspace
     * @param       aTableName
     * @param       aColumnsOfInterest
     */
    TableScanQuery( Session              aSession,
                    Executor             aExecutor,
                    String               aKeyspace,
                    String               aTableName,
                    Map<String,String>   aColumnsOfInterest )
    {
        session              = aSession;
        executor             = aExecutor;
        keyspace             = aKeyspace;
        tableName            = aTableName;
        columnsOfInterest    = aColumnsOfInterest;
        keyParameterMappings = new HashMap<>();
        
        loadTableMetadata();
        buildQueries();
        buildPagedQueries();
    }
    
    
    /**
     * FILLIN
     */
    private void loadTableMetadata()
    {
        KeyspaceMetadata myKeyspaceMetadata = session.getCluster().getMetadata().getKeyspace( keyspace );
        if ( myKeyspaceMetadata == null )
        {
            // TODO: implement exception
            throw new RuntimeException( "keyspace " + keyspace + " does not exist" );
        }
        
        tableMetadata = myKeyspaceMetadata.getTable( tableName );
        if ( tableMetadata == null )
        {
            // TODO: implement exception
            throw new RuntimeException( "table " + tableName + " does not exist in keyspace " + keyspace );
        }
    }
    
    
    /**
     * FILLIN
     */
    private void buildQueries()
    {
        clusteringQueryNeeded = ( ! tableMetadata.getClusteringColumns().isEmpty() );
        
        buildPartitionQueries();
        
        if ( clusteringQueryNeeded )
        {
            buildClusterQueries();
        }
    }

    
    /**
     * FILLIN
     */
    private void buildPartitionQueries()
    {
        Set<String> myColumns = new LinkedHashSet<>();
        
        String myTokenColumn = buildTokenColumn();
        
        myColumns.add( myTokenColumn );
        
        keyParameterMappings.put( myTokenColumn, lastTokenParameterName );
        
        if ( clusteringQueryNeeded )
        {
            for ( ColumnMetadata myColumnMetadata : tableMetadata.getPartitionKey() )
            {
                myColumns.add( myColumnMetadata.getName() );
            }
        }
        else
        {
            myColumns.addAll( columnsOfInterest.keySet() );
        }
        
        String mySelect        = buildSelect( myColumns, clusteringQueryNeeded );
        String myLimitFragment = buildLimitFragment();
        
        String myFirstQuery = mySelect + myLimitFragment;
        String myNextQuery  = mySelect + " WHERE " + myTokenColumn + " > :" + lastTokenParameterName + myLimitFragment;
        
        logger.debug( "partition first query: {}", myFirstQuery );
        logger.debug( "partition next query: {}", myNextQuery );
        
        partitionFirstPreparedStatement = session.prepare( myFirstQuery );
        partitionNextPreparedStatement  = Arrays.asList( session.prepare( myNextQuery ) );
    }
    

    /**
     * FILLIN
     */
    private void buildClusterQueries()
    {
        Set<String> myColumns = new LinkedHashSet<>();
        
        for ( ColumnMetadata myColumnMetadata : tableMetadata.getPartitionKey() )
        {
            myColumns.add( myColumnMetadata.getName() );
        }

        for ( ColumnMetadata myColumnMetadata : tableMetadata.getClusteringColumns() )
        {
            myColumns.add( myColumnMetadata.getName() );
        }

        myColumns.addAll( columnsOfInterest.keySet() );
        
        String mySelect        = buildSelect( myColumns, false );
        String myLimitFragment = buildLimitFragment();
        
        StringBuilder myBuilder   = new StringBuilder();
        String        mySeparator = "";
        int           myKeyIndex  = 0;
        
        myBuilder.append( " WHERE " );
        
        for ( ColumnMetadata myColumnMetadata : tableMetadata.getPartitionKey() )
        {
            String myColumnName       = myColumnMetadata.getName();
            String myKeyParameterName = getKeyBindName( myKeyIndex );
            
            myBuilder.append( mySeparator        )
                     .append( myColumnName       )
                     .append( " = :"             )
                     .append( myKeyParameterName )
                     ;

            keyParameterMappings.put( myColumnName, myKeyParameterName );
            
            mySeparator = " AND ";
            myKeyIndex++;
        }
        
        String myWherePartitionKeys = myBuilder.toString();
        
        String myFirstQuery = mySelect + myWherePartitionKeys + myLimitFragment;
        
        logger.debug( "cluster first query: {}", myFirstQuery );
        
        clusterFirstPreparedStatement = session.prepare( myFirstQuery );
        
        List<ColumnMetadata> myClusteringColumns   = tableMetadata.getClusteringColumns();
        List<Order>          myColumnOrders        = tableMetadata.getClusteringOrder();
        String[]             myClusterWhereClauses = new String[myClusteringColumns.size()];
        String               myWherePrefix         = myWherePartitionKeys;
        
        clusterNextPreparedStatements = new ArrayList<>( myClusteringColumns.size() );
        
        for ( int i = 0, ci = myClusteringColumns.size() ; i < ci ; i++ )
        {
            ColumnMetadata myColumnMetadata   = myClusteringColumns.get( i );
            Order          myColumnOrder      = myColumnOrders.get( i );
            String         myColumnName       = myColumnMetadata.getName();
            String         myKeyParameterName = getKeyBindName( myKeyIndex );

            myClusterWhereClauses[i] =
                    myWherePrefix                                  +
                    " AND "                                        +
                    myColumnName                                   +
                    " "                                            +
                    ( ( myColumnOrder == Order.ASC ) ? ">" : "<" ) +
                    " :"                                           +
                    myKeyParameterName;
            
            String myNextQuery = mySelect + myClusterWhereClauses[i] + myLimitFragment;

            if ( logger.isDebugEnabled() )
            {
                logger.debug( "cluster next query {}: {}", (ci-i-1), myNextQuery );
            }
            
            clusterNextPreparedStatements.add( session.prepare( myNextQuery ) );

            myWherePrefix +=
                    " AND "             +
                    myColumnName        +
                    " = :"              +
                    myKeyParameterName;
            
            keyParameterMappings.put( myColumnName, myKeyParameterName );
            
            myKeyIndex++;
        }
        
        Collections.reverse( clusterNextPreparedStatements );
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aColumns
     * @param       aDistinctNeeded
     * 
     * @return
     */
    private String buildSelect( Set<String>   aColumns,
                                boolean       aDistinctNeeded )
    {
        StringBuilder myBuilder   = new StringBuilder();
        String        mySeparator = "";
        
        myBuilder.append( "SELECT " );

        if ( aDistinctNeeded )
        {
            myBuilder.append( "DISTINCT " );
        }
        
        for ( String myColumnName : aColumns )
        {
            myBuilder.append( mySeparator  )
                     .append( myColumnName )
                     ;
            
            String myAlias = columnsOfInterest.get( myColumnName );
            if ( myAlias != null && ! myColumnName.equals( myAlias ) )
            {
                myBuilder.append( " AS "  )
                         .append( myAlias )
                         ;
            }
            
            mySeparator = ", ";
        }
        
        myBuilder.append( " FROM "  )
                 .append( tableName )
                 ;
        
        return myBuilder.toString();
    }
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    private String buildLimitFragment()
    {
        return " LIMIT :" + queryLimitParameterName;
    }

    
    /**
     * FILLIN
     * 
     * @return
     */
    private String buildTokenColumn()
    {
        StringBuilder myBuilder   = new StringBuilder();
        String        mySeparator = "";
        
        myBuilder.append( "TOKEN(" );
        
        for ( ColumnMetadata myColumnMetadata : tableMetadata.getPartitionKey() )
        {
            myBuilder.append( mySeparator                )
                     .append( myColumnMetadata.getName() )
                     ;
            
            mySeparator = ",";
        }
        
        myBuilder.append( ")" );
        
        return myBuilder.toString();
    }
    
    
    /**
     * FILLIN
     */
    private void buildPagedQueries()
    {
        if ( clusteringQueryNeeded )
        {
            partitionPagedQuery =
                    new PagedQuery<PartitionKeyInfo>( session,
                                                      executor,
                                                      partitionFirstPreparedStatement,
                                                      partitionNextPreparedStatement,
                                                      keyParameterMappings             );
            clusterPagedQuery =
                    new PagedQuery<T>( session,
                                       executor,
                                       clusterFirstPreparedStatement,
                                       clusterNextPreparedStatements,
                                       keyParameterMappings           );
        }
        else
        {
            standalonePagedQuery =
                    new PagedQuery<T>( session,
                                       executor,
                                       partitionFirstPreparedStatement,
                                       partitionNextPreparedStatement,
                                       keyParameterMappings             );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aExecutionContext
     * 
     * @return
     */
    public CompletableFuture<Void> execute( TableScanQueryExecutionContext<T> aExecutionContext )
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
    public CompletableFuture<Void> execute( TableScanQueryExecutionContext<T>   aExecutionContext,
                                            Map<String,Object>                  aParameters        )
    {
        CompletableFuture<Void> myPartitionFuture = null;
        
        if ( clusteringQueryNeeded )
        {
            Consumer<List<PartitionKeyInfo>> myPartitionKeyProcessor =
                    ( myPartitionKeyInfos ) ->
                        {
                            ClusterQueryState myState = (ClusterQueryState) myPartitionKeyInfos.get( 0 );
                            
                            CompletableFuture<Void> myClusterFuture =
                                    clusterPagedQuery.execute( aExecutionContext,
                                                               myState.getPartitionKeys() );
                            
                            try
                            {
                                myClusterFuture.get();
                            }
                            catch ( Exception myException )
                            {
                                throw new CompletionException( myException );
                            }
                        };
                    
            PagedQueryExecutionContext<PartitionKeyInfo> myPartitionExecutionContext =
                    PagedQueryExecutionContextBuilder.builder        ( PartitionKeyInfo.class                    )
                                                     .entityCreator  ( () -> { return new ClusterQueryState(); } )
                                                     .reuseEntity    ( true                                      )
                                                     .entityExtractor( this::partitionKeyExtractor               )
                                                     .entityFilter   ( aExecutionContext.getPartitionKeyFilter() )
                                                     .entityProcessor( myPartitionKeyProcessor                   )
                                                     .build();
            
            myPartitionFuture = partitionPagedQuery.execute( myPartitionExecutionContext );
        }
        else
        {
            myPartitionFuture = standalonePagedQuery.execute( aExecutionContext );
        }
        
        return myPartitionFuture;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aRow
     * @param       aPartitionKeyInfo
     */
    private void partitionKeyExtractor( Row                aRow,
                                        PartitionKeyInfo   aPartitionKeyInfo )
    {
        ClusterQueryState    myState         = (ClusterQueryState) aPartitionKeyInfo;
        List<ColumnMetadata> myPartitionKeys = tableMetadata.getPartitionKey();
        
        myState.setToken( aRow.getLong( 0 ) );
        
        for ( int i = 0, ci = myPartitionKeys.size() ; i < ci ; i++ )
        {
            Object myKeyValue = PagedQuery.extractRowValue( aRow, i+1 );
            
            myState.setPartitionKey( myPartitionKeys.get( i ).getName(), myKeyValue );
            myState.setPartitionKey( getKeyBindName( i ), myKeyValue );
        }
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aIndex
     * 
     * @return
     */
    private String getKeyBindName( int aIndex )
    {
        Validate.isTrue( aIndex >= 0 );
        
        String myName;
        
        if ( aIndex < keyBindNames.length )
        {
            myName = keyBindNames[aIndex];
        }
        else
        {
            myName = "key"+aIndex;
        }
        
        return myName;
    }
}
