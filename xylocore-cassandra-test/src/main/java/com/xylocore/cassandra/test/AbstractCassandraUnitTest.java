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

import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

@RunWith( SpringJUnit4ClassRunner.class )
@TestExecutionListeners( { CassandraUnitTestExecutionListener.class } )
@EmbeddedCassandra
public abstract class AbstractCassandraUnitTest
{
    //
    // Members
    //
    
    
    protected final Logger      logger      = LoggerFactory.getLogger( getClass() );
    
    protected Cluster           cluster;
    protected Session           session;
    
    
    
    
    //
    // Class implementation
    //
    
    
    @Before
    public void setup()
    {
        cluster =
                Cluster.builder()
                       .addContactPoints( "127.0.0.1" )
                       .withPort( 9142 )
                       .build();
        
        session = cluster.connect( "cassandra_unit_keyspace" );
    }

    
    @After
    public void tearDown()
    {
        cluster.close();
    }
}
