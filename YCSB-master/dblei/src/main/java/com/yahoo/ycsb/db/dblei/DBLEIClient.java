/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.dblei;

import Operations.ScanIterator;
import com.yahoo.ycsb.*;
import nodes.Stub;
import org.json.JSONObject;

import java.io.*;
    import java.nio.ByteBuffer;
    import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * See {@code rocksdb/README.md} for details.
 */



public class DBLEIClient extends com.yahoo.ycsb.DB {
  private Stub stub;
  private JSONObject jo = new JSONObject();
  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }


  public void init() throws DBException {
    jo.put("cenas", "ola");
    Random gerador = new Random();
    int a = gerador.nextInt(10000) + 20000;
    stub = new Stub();
//    System.out.println("-------Init------");
  }


  public void cleanup() throws DBException {
    try {
      stub.ms.stop().get();
      super.cleanup();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }


  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result){
    /*System.out.println(key);
    System.out.println(fields);
    System.out.println(result);
    System.out.println("FIM OPERACAO READ");
    */
    Long chave;
    try{
      chave = Long.parseLong(key.substring(4, 7)) - 100;
    } catch(Exception e){
      chave = Long.parseLong(key);
    }
    //    Long chave = Long.parseLong(key.split("user")[1].substring(0, 3)) - 100;
    try {
      JSONObject jos = stub.get(chave);
      return Status.OK;
    }catch(Exception e){
      e.printStackTrace();
    }
    return Status.ERROR;
  }


  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                              Vector<HashMap<String, ByteIterator>> result) {
    try {
      ScanIterator si = stub.scan();

      while (si.hasNext()) {
        si.next();
      }
      return Status.OK;
    }catch(Exception e){
//      System.out.println(e);
    }
    return Status.ERROR;
  }


  public Status update(String table, String key, Map<String, ByteIterator> values){
    Long chave;
    try{
      chave = Long.parseLong(key.substring(4, 7)) - 100;
    }catch(Exception e){
      chave = Long.parseLong(key);
    }
    JSONObject job = new JSONObject();
    for(Map.Entry<String, ByteIterator> v: values.entrySet()){
      job.put(v.getKey(), new String(v.getValue().toArray()));
    }
    try {
      stub.put(chave, job);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }


  public Status insert(String table, String key, Map<String, ByteIterator> values){
    Long chave;
    try{
      chave = Long.parseLong(key.substring(4, 7)) - 100;
    }catch(Exception e){
      chave = Long.parseLong(key);
    }
    JSONObject job = new JSONObject();
    for(Map.Entry<String, ByteIterator> v: values.entrySet()){
      job.put(v.getKey(), new String(v.getValue().toArray()));
    }
    try {
//      stub.put(chave, job);
      stub.put(chave, job);
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }


  public Status delete(String table, String key) {
    return Status.OK;
  }


}