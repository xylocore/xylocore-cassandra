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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;


/**
 * FILLIN
 * 
 * @author      Eric R. Medley
 */

public class TestRow
    implements
        Row
{
    int id;
    
    
    public TestRow( int aId )
    {
        id = aId;
    }
    
    
    public int getId()
    {
        return id;
    }
    
    
    @Override
    public String toString()
    {
        return "TestRow[" + id + "]";
    }
    
    
    @Override
    public ColumnDefinitions getColumnDefinitions()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public boolean isNull( int aI )
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public boolean isNull( String aName )
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public boolean getBool( int aI )
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public boolean getBool( String aName )
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public int getInt( int aI )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public int getInt( String aName )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public long getLong( int aI )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public long getLong( String aName )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public Date getDate( int aI )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Date getDate( String aName )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public float getFloat( int aI )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public float getFloat( String aName )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public double getDouble( int aI )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public double getDouble( String aName )
    {
        // TODO Auto-generated method stub
        return id;
    }


    @Override
    public ByteBuffer getBytesUnsafe( int aI )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ByteBuffer getBytesUnsafe( String aName )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ByteBuffer getBytes( int aI )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ByteBuffer getBytes( String aName )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public String getString( int aI )
    {
        // TODO Auto-generated method stub
        return Integer.toString( id );
    }


    @Override
    public String getString( String aName )
    {
        // TODO Auto-generated method stub
        return Integer.toString( id );
    }


    @Override
    public BigInteger getVarint( int aI )
    {
        // TODO Auto-generated method stub
        return BigInteger.valueOf( id );
    }


    @Override
    public BigInteger getVarint( String aName )
    {
        // TODO Auto-generated method stub
        return BigInteger.valueOf( id );
    }


    @Override
    public BigDecimal getDecimal( int aI )
    {
        // TODO Auto-generated method stub
        return BigDecimal.valueOf( id );
    }


    @Override
    public BigDecimal getDecimal( String aName )
    {
        // TODO Auto-generated method stub
        return BigDecimal.valueOf( id );
    }


    @Override
    public UUID getUUID( int aI )
    {
        // TODO Auto-generated method stub
        return new UUID( 0L, id );
    }


    @Override
    public UUID getUUID( String aName )
    {
        // TODO Auto-generated method stub
        return new UUID( 0L, id );
    }


    @Override
    public InetAddress getInet( int aI )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public InetAddress getInet( String aName )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public <T> List<T> getList( int aI,
                                Class<T> aElementsClass )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public <T> List<T> getList( String aName,
                                Class<T> aElementsClass )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public <T> Set<T> getSet( int aI,
                              Class<T> aElementsClass )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public <T> Set<T> getSet( String aName,
                              Class<T> aElementsClass )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public <K,V> Map<K,V> getMap( int aI,
                                  Class<K> aKeysClass,
                                  Class<V> aValuesClass )
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public <K,V> Map<K,V> getMap( String aName,
                                  Class<K> aKeysClass,
                                  Class<V> aValuesClass )
    {
        // TODO Auto-generated method stub
        return null;
    }
}
