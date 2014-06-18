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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.datastax.driver.core.Session;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class TableScanQueryBuilder<T>
{
    //
    // Members
    //
    
    
    private Session             session;
    private Executor            executor;
    private String              keyspace;
    private String              tableName;
    private Map<String,String>  columnsOfInterest;
    
    
    
    
    //
    // Class implementation
    //
    
    
    private TableScanQueryBuilder()
    {
        session           = null;
        keyspace          = null;
        tableName         = null;
        columnsOfInterest = new LinkedHashMap<>();
        
        executor( null );
    }
    
    
    
    public static <T> TableScanQueryBuilder<T> builder()
    {
        return new TableScanQueryBuilder<>();
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aSession
     * 
     * @return
     */
    public TableScanQueryBuilder<T> session( Session aSession )
    {
        Validate.notNull( aSession, "a session must be specified" );
        
        session = aSession;
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aKeyspace
     * 
     * @return
     */
    public TableScanQueryBuilder<T> keyspace( String aKeyspace )
    {
        aKeyspace = StringUtils.trimToNull( aKeyspace );
        
        keyspace = aKeyspace;
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aTableName
     * 
     * @return
     */
    public TableScanQueryBuilder<T> tableName( String aTableName )
    {
        Validate.notBlank( aTableName );
        
        tableName = aTableName;
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aExecutor
     * 
     * @return
     */
    public TableScanQueryBuilder<T> executor( Executor aExecutor )
    {
        if ( aExecutor == null )
        {
            aExecutor = ForkJoinPool.commonPool();
        }
        
        executor = aExecutor;
        
        return this;
    }
    

    /**
     * FILLIN
     * 
     * @param       aColumnNames
     * 
     * @return
     */
    public TableScanQueryBuilder<T> columns( String ... aColumnNames )
    {
        for ( int i = 0, ci = aColumnNames.length ; i < ci ; i++ )
        {
            column( aColumnNames[i] );
        }
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aColumnName
     * 
     * @return
     */
    public TableScanQueryBuilder<T> column( String aColumnName )
    {
        return column( aColumnName, null );
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aColumnName
     * @param       aAlias
     * 
     * @return
     */
    public TableScanQueryBuilder<T> column( String   aColumnName,
                                            String   aAlias       )
    {
        Validate.notBlank( aColumnName );
        
        aAlias = StringUtils.trimToNull( aAlias );
        
        if ( aAlias == null )
        {
            aAlias = aColumnName;
        }
        
        columnsOfInterest.put( aColumnName, aAlias );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public TableScanQuery<T> build()
    {
        if ( session == null )
        {
            throw new IllegalStateException( "a session has not been specified for the builder" );
        }
        
        if ( tableName == null )
        {
            throw new IllegalStateException( "a table name has not been specified for the builder" );
        }
        
        if ( columnsOfInterest.isEmpty() )
        {
            throw new IllegalStateException( "at least one column of interest must be specified" );
        }
        
        if ( keyspace == null )
        {
            keyspace = session.getLoggedKeyspace();
        }

        return new TableScanQuery<>( session,
                                     executor,
                                     keyspace,
                                     tableName,
                                     columnsOfInterest );
    }
}
