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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class PagedQueryBuilder<T>
{
    //
    // Members
    //
    
    
    private Session                 session;
    private Executor                executor;
    private PreparedStatement       firstQuery;
    private List<PreparedStatement> nextQueries;
    private Map<String,String>      keyColumnNames;
    
    
    
    
    //
    // Class implementation
    //
    
    
    private PagedQueryBuilder()
    {
        session        = null;
        firstQuery     = null;
        nextQueries    = new ArrayList<>();
        keyColumnNames = new LinkedHashMap<>();
        
        executor( null );
    }
    
    
    
    public static <T> PagedQueryBuilder<T> builder()
    {
        return new PagedQueryBuilder<>();
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aSession
     * 
     * @return
     */
    public PagedQueryBuilder<T> session( Session aSession )
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
    public PagedQueryBuilder<T> firstQuery( PreparedStatement aFirstQuery )
    {
        Validate.notNull( aFirstQuery, "the query must be specified" );
        
        firstQuery = aFirstQuery;
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aExecutor
     * 
     * @return
     */
    public PagedQueryBuilder<T> executor( Executor aExecutor )
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
    public PagedQueryBuilder<T> keyColumns( String ... aColumnNames )
    {
        for ( int i = 0, ci = aColumnNames.length ; i < ci ; i++ )
        {
            keyColumn( aColumnNames[i] );
        }
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aColumnNames
     * 
     * @return
     */
    public PagedQueryBuilder<T> keyColumns( Collection<String> aColumnNames )
    {
        for ( String myColumnName : aColumnNames )
        {
            keyColumn( myColumnName );
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
    public PagedQueryBuilder<T> keyColumn( String aColumnName )
    {
        return keyColumn( aColumnName, null );
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aColumnName
     * @param       aParameterName
     * 
     * @return
     */
    public PagedQueryBuilder<T> keyColumn( String   aColumnName,
                                           String   aParameterName )
    {
        Validate.notBlank( aColumnName );
        
        aParameterName = StringUtils.trimToNull( aParameterName );
        
        if ( aParameterName == null )
        {
            aParameterName = aColumnName;
        }
        
        keyColumnNames.put( aColumnName, aParameterName );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public PagedQuery<T> build()
    {
        Validate.notNull ( session       , "a session has not been specified for the builder"                        );
        Validate.notNull ( firstQuery    , "the first query has not been specified for the builder"                  );
        Validate.notEmpty( nextQueries   , "there must be at least one next query specified for the builder"         );
        Validate.notEmpty( keyColumnNames, "there must be at least one key column mapping specified for the builder" );
        
        return new PagedQuery<>( session,
                                 executor,
                                 firstQuery,
                                 nextQueries,
                                 keyColumnNames );
    }
}
