/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.util

import org.apache.spark.SparkAssertionException
import org.apache.spark.sql.types._

object SchemaComparer {
  def assertSchemaEqual(
                         s1: StructType,
                         s2: StructType,
                         ignoreNullable: Boolean = false): Unit = {
    if (!structTypeEquals(s1, s2, ignoreNullable)) {
      // TODO: prettify error message
      throw new SparkAssertionException(
        "DIFFERENT_SCHEMA",
        Map(
          "expectedCount" -> s1.prettyJson,
          "actualCount" -> s2.prettyJson)
      )
    }
  }
  def structTypeEquals(
      s1: StructType,
      s2: StructType,
      ignoreNullable: Boolean = false): Boolean = {
    if (s1.length != s2.length) {
      false
    } else {
      s1.sortBy(_.name).zip(s2.sortBy(_.name)).forall { t =>
        ((t._1.nullable == t._2.nullable) || ignoreNullable) &&
        (t._1.name == t._2.name) &&
        dataTypeEquals(t._1.dataType, t._2.dataType, ignoreNullable)
      }
    }
  }

  def dataTypeEquals(dt1: DataType, dt2: DataType, ignoreNullable: Boolean): Boolean = {
    (ignoreNullable, dt1, dt2) match {
      case (true, st1: StructType, st2: StructType) => structTypeEquals(st1, st2, ignoreNullable)
      case (true, ArrayType(vdt1, _), ArrayType(vdt2, _)) =>
        dataTypeEquals(vdt1, vdt2, ignoreNullable)
      case (true, MapType(kdt1, vdt1, _), MapType(kdt2, vdt2, _)) =>
        dataTypeEquals(kdt1, kdt2, ignoreNullable) && dataTypeEquals(vdt1, vdt2, ignoreNullable)
      case _ => dt1 == dt2
    }
  }
}
