package com.socrata.geoexport.encoders

import org.scalatest.{FunSuite, MustMatchers}

class ShapefileEncoderTest extends FunSuite with MustMatchers {
  test("Truncation is a noop if all names are short") {
    ShapefileEncoder.truncateAndDedup(Seq("a", "b", "c")) must be (Seq("a", "b", "c"))
  }

  test("A single long name is just truncated") {
    ShapefileEncoder.truncateAndDedup(Seq("a", "b is a very long name", "c")) must be (Seq("a", "b is a ver", "c"))
  }

  test("A long name truncated and deduped if it already exists") {
    ShapefileEncoder.truncateAndDedup(Seq("a", "b is a very long name", "b is a ver")) must be (Seq("a", "b is a v_2", "b is a ver"))
  }

  test("Multiple long names are deduped in order") {
    ShapefileEncoder.truncateAndDedup(Seq("a", "b is a very long name", "b is a very long name again")) must be (Seq("a", "b is a ver", "b is a v_2"))
  }

  test("If two identical short names exist, they are deduped") {
    ShapefileEncoder.truncateAndDedup(Seq("a", "a")) must be (Seq("a", "a_2"))
  }
}
