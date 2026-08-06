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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestFixedByteBufferRoundtrip {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "fixed_value", Types.FixedType.ofLength(16)));

  @Test
  public void testFixedByteBufferWriteAndRead() throws IOException {
    // Create test records with fixed byte values
    GenericRecord record = GenericRecord.create(SCHEMA);
    List<Record> records = ImmutableList.of(
        record.copy(ImmutableMap.of(
            "id", 1L,
            "fixed_value", ByteBuffer.wrap(new byte[] {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
            }))),
        record.copy(ImmutableMap.of(
            "id", 2L,
            "fixed_value", ByteBuffer.wrap(new byte[] {
                16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1
            }))),
        record.copy(ImmutableMap.of(
            "id", 3L,
            "fixed_value", ByteBuffer.wrap(new byte[] {
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            }))));

    // Write data using InternalWriter
    OutputFile outputFile = new InMemoryOutputFile();
    try (DataWriter<Record> dataWriter =
        Avro.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(InternalWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record rec : records) {
        dataWriter.write(rec);
      }
    }

    // Read data back using InternalReader
    List<Record> readRecords;
    try (AvroIterable<Record> reader =
        Avro.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createResolvingReader(InternalReader::create)
            .build()) {
      readRecords = Lists.newArrayList(reader);
    }

    // Verify data integrity
    assertThat(readRecords).hasSize(records.size());
    for (int i = 0; i < records.size(); i++) {
      Record expected = records.get(i);
      Record actual = readRecords.get(i);

      assertThat(actual.getField("id")).as("ID mismatch at index %d", i)
          .isEqualTo(expected.getField("id"));

      ByteBuffer expectedBuffer = (ByteBuffer) expected.getField("fixed_value");
      ByteBuffer actualBuffer = (ByteBuffer) actual.getField("fixed_value");

      assertThat(actualBuffer).as("Fixed value mismatch at index %d", i)
          .isEqualTo(expectedBuffer);
    }
  }
}
