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

import org.apache.commons.lang3.Validate;

import com.datastax.driver.core.Row;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class PagedQueryExecutionContextBuilder<T>
{
    //
    // Nested classes
    //
    
    
    static class PagedQueryExecutionContextImpl<T>
        implements
            PagedQueryExecutionContext<T>
    {
        //
        // Members
        //
        
        private Supplier<T>         entityCreator;
        private boolean             reuseEntity;
        private BiConsumer<Row,T>   entityExtractor;
        private Predicate<T>        entityFilter;
        private Consumer<List<T>>   entityProcessor;
        private int                 queryLimit;
        private int                 fetchSize;
        private int                 fetchThreshold;
        private int                 sliceSize;
        private boolean             forceFullSlice;
        private int                 concurrencyLevel;
        
        
        //
        // Class implementation
        //

        /**
         * FILLIN
         */
        PagedQueryExecutionContextImpl()
        {
            setReuseEntity     ( false  );
            setEntityFilter    ( null   );
            setQueryLimit      ( 10_000 );
            setFetchSize       ( 0      );
            setFetchThreshold  ( 0      );
            setSliceSize       ( 1      );
            setForceFullSlice  ( false  );
            setConcurrencyLevel( 1      );
        }
        
        void validate()
        {
            Validate.notNull( entityCreator  , "an entity creator needs to be set for the execution context"   );
            Validate.notNull( entityExtractor, "an entity extractor needs to be set for the execution context" );
            Validate.notNull( entityProcessor, "an entity processor needs to be set for the execution context" );
        }
        
        void setEntityCreator( Supplier<T> aEntityCreator )
        {
            entityCreator = aEntityCreator;
        }

        void setReuseEntity( boolean aReuseEntity )
        {
            reuseEntity = aReuseEntity;
        }

        void setEntityExtractor( BiConsumer<Row,T> aEntityExtractor )
        {
            entityExtractor = aEntityExtractor;
        }

        void setEntityFilter( Predicate<T> aEntityFilter )
        {
            if ( aEntityFilter == null )
            {
                aEntityFilter = ( myEntity ) -> { return true; };
            }
            
            entityFilter = aEntityFilter;
        }

        void setEntityProcessor( Consumer<List<T>> aEntityProcessor )
        {
            entityProcessor = aEntityProcessor;
        }

        void setQueryLimit( int aQueryLimit )
        {
            Validate.isTrue( aQueryLimit > 0, "query limit must be greater than zero" );
            
            queryLimit = aQueryLimit;
        }

        void setFetchSize( int aFetchSize )
        {
            Validate.isTrue( aFetchSize >= 0, "fetch size must be greater than or equal to zero" );
            
            fetchSize = aFetchSize;
        }
        
        void setFetchThreshold( int aFetchThreshold )
        {
            Validate.isTrue( aFetchThreshold >= 0, "fetch threshold must be greater than or equal to zero" );
            
            fetchThreshold = aFetchThreshold;
        }

        void setSliceSize( int aSliceSize )
        {
            Validate.isTrue( aSliceSize > 0, "slice size must be greater than zero" );
            
            sliceSize = aSliceSize;
        }
        
        void setForceFullSlice( boolean aForceFullSlice )
        {
            forceFullSlice = aForceFullSlice;
        }

        void setConcurrencyLevel( int aConcurrencyLevel )
        {
            Validate.isTrue( aConcurrencyLevel > 0, "concurrency level must be greater than zero" );
            
            concurrencyLevel = aConcurrencyLevel;
        }
        
        
        //
        // PagedQueryExecutionContext interface implementation
        //
        
        @Override
        public Supplier<T> getEntityCreator()
        {
            return entityCreator;
        }
        
        @Override
        public boolean isReuseEntity()
        {
            return reuseEntity;
        }
        
        @Override
        public BiConsumer<Row,T> getEntityExtractor()
        {
            return entityExtractor;
        }
        
        @Override
        public Predicate<T> getEntityFilter()
        {
            return entityFilter;
        }
    
        @Override
        public Consumer<List<T>> getEntityProcessor()
        {
            return entityProcessor;
        }
        
        @Override
        public int getQueryLimit()
        {
            return queryLimit;
        }
        
        @Override
        public int getFetchSize()
        {
            return fetchSize;
        }
        
        @Override
        public int getFetchThreshold()
        {
            return fetchThreshold;
        }
        
        @Override
        public int getSliceSize()
        {
            return sliceSize;
        }
        
        @Override
        public boolean isForceFullSlice()
        {
            return forceFullSlice;
        }
        
        @Override
        public int getConcurrencyLevel()
        {
            return concurrencyLevel;
        }
    }    
    
    
    
    
    //
    // Members
    //
    

    private PagedQueryExecutionContextImpl<T> executionContext;

    
    
    
    //
    // Class implementation
    //
    

    /**
     * FILLIN
     */
    private PagedQueryExecutionContextBuilder()
    {
    }
    

    /**
     * FILLIN
     * 
     * @return
     */
    private PagedQueryExecutionContextImpl<T> getExecutionContext()
    {
        if ( executionContext == null )
        {
            executionContext = new PagedQueryExecutionContextImpl<>();
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
    public static <T> PagedQueryExecutionContextBuilder<T> builder( Class<T> aEntityClass )
    {
        return new PagedQueryExecutionContextBuilder<T>();
    }
    

    /**
     * FILLIN
     * 
     * @param       aEntityCreator
     * 
     * @return
     */
    public PagedQueryExecutionContextBuilder<T> entityCreator( Supplier<T> aEntityCreator )
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
    public PagedQueryExecutionContextBuilder<T> reuseEntity( boolean aReuseEntity )
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
    public PagedQueryExecutionContextBuilder<T> entityExtractor( BiConsumer<Row,T> aEntityExtractor )
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
    public PagedQueryExecutionContextBuilder<T> entityFilter( Predicate<T> aEntityFilter )
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
    public PagedQueryExecutionContextBuilder<T> entityProcessor( Consumer<List<T>> aEntityProcessor )
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
    public PagedQueryExecutionContextBuilder<T> queryLimit( int aQueryLimit )
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
    public PagedQueryExecutionContextBuilder<T> fetchSize( int aFetchSize )
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
    public PagedQueryExecutionContextBuilder<T> fetchThreshold( int aFetchThreshold )
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
    public PagedQueryExecutionContextBuilder<T> sliceSize( int aSliceSize )
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
    public PagedQueryExecutionContextBuilder<T> forceFullSlice( boolean aForceFullSlice )
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
    public PagedQueryExecutionContextBuilder<T> concurrencyLevel( int aConcurrencyLevel )
    {
        getExecutionContext().setConcurrencyLevel( aConcurrencyLevel );
        
        return this;
    }
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public PagedQueryExecutionContext<T> build()
    {
        PagedQueryExecutionContextImpl<T> myExecutionContext = getExecutionContext();
        myExecutionContext.validate();
        
        executionContext = null;
        
        return myExecutionContext;
    }
}
