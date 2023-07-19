/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.coders.instances.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.spotify.scio.avro.{ScioGenericRecordDatumFactory, ScioSpecificRecordDatumFactory}
import com.twitter.chill.KSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder

import scala.collection.mutable.{Map => MMap}

private[coders] class GenericAvroSerializer extends KSerializer[GenericRecord] {
  private lazy val cache: MMap[String, AvroCoder[GenericRecord]] = MMap()

  private def getCoder(schemaStr: String): AvroCoder[GenericRecord] =
    cache.getOrElseUpdate(
      schemaStr,
      AvroCoder.of(ScioGenericRecordDatumFactory, new Schema.Parser().parse(schemaStr))
    )
  private def getCoder(schemaStr: String, schema: Schema): AvroCoder[GenericRecord] =
    cache.getOrElseUpdate(schemaStr, AvroCoder.of(ScioGenericRecordDatumFactory, schema))

  override def write(kryo: Kryo, out: Output, obj: GenericRecord): Unit = {
    val schemaStr = obj.getSchema.toString
    val coder = this.getCoder(schemaStr, obj.getSchema)
    // write schema before every record in case it's not in reader serializer's cache
    out.writeString(schemaStr)
    coder.encode(obj, out)
  }

  override def read(kryo: Kryo, in: Input, cls: Class[GenericRecord]): GenericRecord = {
    val coder = this.getCoder(in.readString())
    coder.decode(in)
  }
}

private[coders] class SpecificAvroSerializer[T <: SpecificRecord] extends KSerializer[T] {
  private lazy val cache: MMap[Class[T], AvroCoder[T]] = MMap()

  private def getCoder(cls: Class[T]): AvroCoder[T] = {
    cache.getOrElseUpdate(
      cls, {
        val schema = SpecificData.get().getSchema(cls)
        val factory = new ScioSpecificRecordDatumFactory(cls)
        AvroCoder.of(factory, schema)
      }
    )
  }

  override def write(kser: Kryo, out: Output, obj: T): Unit =
    this.getCoder(obj.getClass.asInstanceOf[Class[T]]).encode(obj, out)

  override def read(kser: Kryo, in: Input, cls: Class[T]): T =
    this.getCoder(cls).decode(in)
}
