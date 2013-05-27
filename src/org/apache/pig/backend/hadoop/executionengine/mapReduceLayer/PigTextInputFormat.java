/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

// ALL INTERNETARCHIVE CHANGES INCLUDE A COMMENT STARTING "// IA "

// IA Override end-of-line marker to be just '\n'
//
// Pig's built-in text input system uses any of 
//    \r
//    \r\n
//    \n
// as the end-of-line marker.  Often, we have text files with '\r'
// characters in the middle of a line and don't want it to be used as
// a line terminator.
//
// Setting the property:
//
//   textinputformat.record.delimiter.force.newline = true
//
// will over-ride Pig's normal defaults and will *only* use '\n' as
// the line terminator.  Setting the property to 'false' will restore
// Pig's built-in behavior.
// 
// The default value is 'true', so we are changing the default
// behavior of Pig.
//
// NOTE: We cannot simply set the 'textinputformat.record.delimiter'
// property in a Pig script because there's no way to embed a '\n'
// character in a Pig 'set' statement.


import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;

// IA Imports needed by our customized createRecordReader method
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PigTextInputFormat extends TextInputFormat {

  // IA Begin
  /**
   * Set the 'textinputformat.record.delimiter' to a custom value if
   * the IA-specific override property is true.
   */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader( InputSplit split, TaskAttemptContext context )
  {
    // Force parent reader to *only* use \n for eol, not \r
    if ( context.getConfiguration().getBoolean( "textinputformat.record.delimiter.force.newline", true ) )
      {
        context.getConfiguration().set( "textinputformat.record.delimiter", "\n" );
      }
    
    return super.createRecordReader( split, context );
  }
  // IA End

   /*
     * This is to support multi-level/recursive directory listing until 
     * MAPREDUCE-1577 is fixed.
     */
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {        
        return MapRedUtil.getAllFileRecursively(super.listStatus(job), 
                job.getConfiguration());             
    }
    
}
