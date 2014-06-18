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

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.datastax.driver.core.Row;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class TableScanQueryExecutionContextBuilder<T>
{
    //
    // Nested classes
    //
    
    
    static class TableScanQueryExecutionContextImpl<T>
        extends
            PagedQueryExecutionContextBuilder.PagedQueryExecutionContextImpl<T>
        implements
            TableScanQueryExecutionContext<T>
    {
        //
        // Members
        //
        
        private Predicate<PartitionKeyInfo> partitionKeyFilter;
        
        
        //
        // Class implementation
        //

        /**
         * FILLIN
         */
        TableScanQueryExecutionContextImpl()
        {
            setPartitionKeyFilter( null );
        }
        
        @Override
        void validate()
        {
            super.validate();
        }
        
        void setPartitionKeyFilter( Predicate<PartitionKeyInfo> aPartitionKeyFilter )
        {
            if ( aPartitionKeyFilter == null )
            {
                aPartitionKeyFilter = ( myInfo ) -> { return true; };
            }
            
            partitionKeyFilter = aPartitionKeyFilter;
        }
        
        
        //
        // TableScanQueryExecutionContext interface implementation
        //
        
        @Override
        public Predicate<PartitionKeyInfo> getPartitionKeyFilter()
        {
            return partitionKeyFilter;
        }
    }    
    
    
    
    
    //
    // Members
    //
    

    private TableScanQueryExecutionContextImpl<T> executionContext;

    
    
    
    //
    // Class implementation
    //
    

    /**
     * FILLIN
     */
    private TableScanQueryExecutionContextBuilder()
    {
    }
    

    /**
     * FILLIN
     * 
     * @return
     */
    private TableScanQueryExecutionContextImpl<T> getExecutionContext()
    {
        if ( executionContext == null )
        {
            executionContext = new TableScanQueryExecutionContextImpl<>();
        }
        
        return executionContext;
    }
    

    /**
     * FILLIN
     * 
     * @param       aEntityClass
     * 
     * @return
     */
    public static <T> TableScanQueryExecutionContextBuilder<T> builder( Class<T> aEntityClass )
    {
        return new TableScanQueryExecutionContextBuilder<T>();
    }
    

    /**
     * FILLIN
     * 
     * @param       aEntityCreator
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> entityCreator( Supplier<T> aEntityCreator )
    {
        getExecutionContext().setEntityCreator( aEntityCreator );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aReuseEntity
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> reuseEntity( boolean aReuseEntity )
    {
        getExecutionContext().setReuseEntity( aReuseEntity );
        
        return this;
    }

    
    /**
     * FILLIN
     * 
     * @param       aEntityExtractor
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> entityExtractor( BiConsumer<Row,T> aEntityExtractor )
    {
        getExecutionContext().setEntityExtractor( aEntityExtractor );
        
        return this;
    }

    
    /**
     * FILLIN
     * 
     * @param       aEntityFilter
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> entityFilter( Predicate<T> aEntityFilter )
    {
        getExecutionContext().setEntityFilter( aEntityFilter );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aEntityProcessor
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> entityProcessor( Consumer<List<T>> aEntityProcessor )
    {
        getExecutionContext().setEntityProcessor( aEntityProcessor );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aQueryLimit
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> queryLimit( int aQueryLimit )
    {
        getExecutionContext().setQueryLimit( aQueryLimit );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aFetchSize
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> fetchSize( int aFetchSize )
    {
        getExecutionContext().setFetchSize( aFetchSize );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @param       aFetchThreshold
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> fetchThreshold( int aFetchThreshold )
    {
        getExecutionContext().setFetchThreshold( aFetchThreshold );
        
        return this;
    }

    
    /**
     * FILLIN
     * 
     * @param       aSliceSize
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> sliceSize( int aSliceSize )
    {
        getExecutionContext().setSliceSize( aSliceSize );
        
        return this;
    }

    
    /**
     * FILLIN
     * 
     * @param       aForceFullSlice
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> forceFullSlice( boolean aForceFullSlice )
    {
        getExecutionContext().setForceFullSlice( aForceFullSlice );
        
        return this;
    }

    
    /**
     * FILLIN
     * 
     * @param       aConcurrencyLevel
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> concurrencyLevel( int aConcurrencyLevel )
    {
        getExecutionContext().setConcurrencyLevel( aConcurrencyLevel );
        
        return this;
    }

    
    /**
     * FILLIN
     * 
     * @param       aPartitionKeyFilter
     * 
     * @return
     */
    public TableScanQueryExecutionContextBuilder<T> partitionKeyFilter( Predicate<PartitionKeyInfo> aPartitionKeyFilter )
    {
        getExecutionContext().setPartitionKeyFilter( aPartitionKeyFilter );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public TableScanQueryExecutionContext<T> build()
    {
        TableScanQueryExecutionContextImpl<T> myExecutionContext = getExecutionContext();
        myExecutionContext.validate();
        
        executionContext = null;
        
        return myExecutionContext;
    }
}
