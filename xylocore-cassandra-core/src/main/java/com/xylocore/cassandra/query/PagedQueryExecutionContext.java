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

public interface PagedQueryExecutionContext<T>
{
    /**
     * FILLIN
     * 
     * @return
     */
    public Supplier<T> getEntityCreator();
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public boolean isReuseEntity();

    
    /**
     * FILLIN
     * 
     * @return
     */
    public BiConsumer<Row,T> getEntityExtractor();

    
    /**
     * FILLIN
     * 
     * @return
     */
    public Predicate<T> getEntityFilter();
    

    /**
     * FILLIN
     * 
     * @return
     */
    public Consumer<List<T>> getEntityProcessor();
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public int getQueryLimit();
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public int getFetchSize();
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public int getFetchThreshold();

    
    /**
     * FILLIN
     * 
     * @return
     */
    public int getSliceSize();
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public boolean isForceFullSlice();
    
    
    /**
     * FILLIN
     * 
     * @return
     */
    public int getConcurrencyLevel();
}
