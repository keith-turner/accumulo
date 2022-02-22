/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.log;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Syncable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferedWalOutputStream extends OutputStream implements Syncable {

  private static class Buffer {
    byte[] data;
    int position;

    Buffer(int size) {
      this.data = new byte[size];
      this.position = 0;
    }

    int write(int b){
      if(position < data.length) {
        data[position++]=(byte)b;
        return 1;
      } else {
        return 0;
      }
    }

    int write(byte b[], int off, int len) {
      int left = data.length - position;
      int lenToWrite = Math.min(left, len);
      System.arraycopy(b, off, data, position,lenToWrite);
      position+=lenToWrite;
      return lenToWrite;
    }

    public void writeTo(OutputStream wrapped) throws IOException {
      wrapped.write(data, 0, position);
    }

    public boolean isEmpty(){
      return position == 0;
    }
  }

  private static final int BUFFER_SIZE = 10*1024*1024;

  private OutputStream wrapped;
  private BlockingQueue<Buffer> bufferQueue = new ArrayBlockingQueue<>(11);
  private Buffer currentBuffer;
  private Lock bufferLock = new ReentrantLock();
  private Lock writeLock = new ReentrantLock();
  private boolean closed = false;

  public BufferedWalOutputStream(OutputStream wrapped) {
    this.wrapped = wrapped;
    this.currentBuffer = new Buffer(BUFFER_SIZE);
  }

  private void writeBuffers() throws IOException {
    writeLock.lock();
    try{
      List<Buffer> buffers = new ArrayList<>();
      bufferQueue.drainTo(buffers);

      for (Buffer buffer : buffers) {
        buffer.writeTo(wrapped);
      }

    }finally {
      writeLock.unlock();
    }
  }

  private void newBuffer(){
    bufferLock.lock();
    try{
      if(!currentBuffer.isEmpty()) {
        bufferQueue.add(currentBuffer);
        currentBuffer = new Buffer(BUFFER_SIZE);
      }
    }finally {
      bufferLock.unlock();
    }
  }

  @Override public void write(int b) throws IOException {
    boolean createdNewBuffer = false;

    bufferLock.lock();
    try {
      Preconditions.checkState(!closed);
      int written = currentBuffer.write(b);
      while (written < 1) {
        newBuffer();
        written = currentBuffer.write(b);
        createdNewBuffer = true;
      }
    } finally {
      bufferLock.unlock();
    }

    if (createdNewBuffer) {
      if (bufferQueue.size() > 10) {
        writeBuffers();
      }
    }
  }

  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }

  public void write(byte b[], int off, int len) throws IOException {
    Objects.checkFromIndexSize(off, len, b.length);

    boolean createdNewBuffer = false;

    bufferLock.lock();
    try{
      Preconditions.checkState(!closed);
      int written = currentBuffer.write(b, off, len);
      while(written < len) {
        newBuffer();
        off+=written;
        len-=written;
        written = currentBuffer.write(b, off, len);
        createdNewBuffer = true;
      }
    }finally {
      bufferLock.unlock();
    }

    if(createdNewBuffer) {
      if(bufferQueue.size() > 10){
        writeBuffers();
      }
    }

  }

  @Override public void hflush() throws IOException {
    newBuffer();
    writeLock.lock();
    try {
      Preconditions.checkState(!closed);
      writeBuffers();
      ((Syncable)wrapped).hflush();
    }finally {
      writeLock.unlock();
    }
  }

  @Override public void hsync() throws IOException {
    newBuffer();
    writeLock.lock();
    try {
      Preconditions.checkState(!closed);
      writeBuffers();
      ((Syncable)wrapped).hsync();
    }finally {
      writeLock.unlock();
    }
  }


  public void flush() throws IOException {
    // intentional noop
  }

  public void close() throws IOException {
    bufferLock.lock();
    try{
      writeLock.lock();
      try{
          if(closed)
            return;
          newBuffer();
          writeBuffers();
          wrapped.close();
          closed = true;
      }finally {
        writeLock.unlock();
      }
    }finally {
      bufferLock.unlock();
    }
  }
}
