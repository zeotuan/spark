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

import org.apache.spark.{SparkAssertionException, SparkFunSuite}
import org.apache.spark.sql.types._

class SchemaComparerSuite extends SparkFunSuite {

  test("do not throw error if the schemas are equal") {
      val s1 = StructType(
        Seq(
          StructField("something", StringType),
          StructField("mood", StringType)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType),
          StructField("mood", StringType)
        )
      )
      SchemaComparer.assertSchemaEqual(s1, s2)
    }

    test("throw error if the schemas aren't equal") {
      val s1 = StructType(
        Seq(
          StructField("something", StringType)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType),
          StructField("mood", StringType)
        )
      )
      val e = intercept[SparkAssertionException] {
        SchemaComparer.assertSchemaEqual(s1, s2)
      }
      assert(e.getMessage.contains("DIFFERENT_SCHEMA"))
    }

    test("can ignore the nullable when determining equality") {
      val s1 = StructType(
        Seq(
          StructField("something", StringType),
          StructField("mood", StringType)
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, nullable = false),
          StructField("mood", StringType)
        )
      )
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = true)
    }

    test("ignore the nullable when determining equality on complex data types") {
      val s1 = StructType(
        Seq(
          StructField("something", StringType),
          StructField("array", ArrayType(StringType, containsNull = true)),
          StructField("map", MapType(StringType, StringType, valueContainsNull = false)),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, nullable = false),
                  StructField("mood", ArrayType(StringType, containsNull = false))
                )
              ))
          )
        )
      )
      val s2 = StructType(
        Seq(
          StructField("something", StringType, nullable = false),
          StructField("array", ArrayType(StringType, containsNull = false)),
          StructField("map", MapType(StringType, StringType, valueContainsNull = true)),
          StructField(
            "struct",
            StructType(
              StructType(
                Seq(
                  StructField("something", StringType, nullable = false),
                  StructField("mood", ArrayType(StringType, containsNull = true))
                )
              )),
            nullable = false
          )
        )
      )
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = true)
    }

  test("don't ignore when determining equality on complex data types") {
    val s1 = StructType(
      Seq(
        StructField("something", StringType),
        StructField("array", ArrayType(StringType, containsNull = true)),
        StructField("map", MapType(StringType, StringType, valueContainsNull = false)),
        StructField(
          "struct",
          StructType(
            StructType(
              Seq(
                StructField("something", StringType, nullable = false),
                StructField("mood", ArrayType(StringType, containsNull = false))
              )
            ))
        )
      )
    )
    val s2 = StructType(
      Seq(
        StructField("something", StringType, nullable = false),
        StructField("array", ArrayType(StringType, containsNull = false)),
        StructField("map", MapType(StringType, StringType, valueContainsNull = true)),
        StructField(
          "struct",
          StructType(
            StructType(
              Seq(
                StructField("something", StringType, nullable = false),
                StructField("mood", ArrayType(StringType, containsNull = true))
              )
            )),
          nullable = false
        )
      )
    )
    val e = intercept[SparkAssertionException] {
        SchemaComparer.assertSchemaEqual(s1, s2)
    }
    assert(e.getMessage.contains("DIFFERENT_SCHEMA"))
  }

  test("throw error when determining equality on different complex data types") {
    val s1 = StructType(
      Seq(
        StructField("something", StringType),
        StructField("array", ArrayType(StringType, containsNull = true)),
        StructField("map", MapType(StringType, StringType, valueContainsNull = false)),
        StructField(
          "struct",
          StructType(
            StructType(
              Seq(
                StructField("something", StringType, nullable = false),
                StructField("mood", ArrayType(StringType, containsNull = false))
              )
            ))
        )
      )
    )
    val s2 = StructType(
      Seq(
        StructField("something", StringType, nullable = false),
        StructField("array", ArrayType(
          StructType(
            Seq(
              StructField("nested", StringType)
            )
          ), containsNull = false)
        ),
        StructField("map", MapType(StringType, StringType, valueContainsNull = true)),
        StructField(
          "struct",
          StructType(
            StructType(
              Seq(
                StructField("something", StringType, nullable = false),
                StructField("mood", ArrayType(StringType, containsNull = true))
              )
            )),
          nullable = false
        )
      )
    )
    val e = intercept[SparkAssertionException] {
      SchemaComparer.assertSchemaEqual(s1, s2, ignoreNullable = true)
    }

    assert(e.getMessage.contains("DIFFERENT_SCHEMA"))
  }
}

