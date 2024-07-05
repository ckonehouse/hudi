/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/***
 * Represents a feature Vector
 */
public class HoodieVector implements Serializable {
  private final float[] vector;
  // Bytes used to store each element in the vector
  public static final int BYTES = Float.BYTES;

  /**
   * Construct a vector from the contents of the given field in the given record
   * @param rec The record
   * @param field The field which has the vector
   */
  public HoodieVector(GenericRecord rec, String field) {
    this((ArrayList<Float>)rec.get(field));
  }

  public HoodieVector(List<Float> floatList) {
    this.vector = new float[floatList.size()];
    int i = 0;
    for (Float f : floatList) {
      this.vector[i++] = f;
    }
  }

  public float[] toFloatArray() {
    return vector;
  }

  public List<Float> toList() {
    List<Float> result = new ArrayList<>(vector.length);
    for (float f : vector) {
      result.add(f);
    }
    return result;
  }
}
