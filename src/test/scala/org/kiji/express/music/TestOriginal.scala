/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wibidata.testing.express

import scala.collection.mutable.Buffer

import com.twitter.scalding.{JobTest, TextLine, Tsv}
import org.scalatest.FunSuite

class TestOriginal extends FunSuite with com.twitter.scalding.TupleConversions {
  val testInput = (0, "0 1 2 3") :: Nil
  def validateTest(output: Buffer[(String, String)]) {
    println(output)
  }
  test("Working version") {
      val input = "input-file"
      val output = "output-file"
      JobTest(new WorkingJob(_))
      .arg("input", input)
      .arg("output", output)
      .source(TextLine(input), testInput)
      .sink(Tsv(output)){validateTest}
      .runHadoop
      .finish
    }

    test("Broken version") {
      val input = "input-file"
      val output = "output-file"
      JobTest(new BrokenJob(_))
      .arg("input", input)
      .arg("output", output)
      .source(TextLine(input), testInput)
      .sink(Tsv(output)){validateTest}
      .runHadoop
      .finish
    }
}
