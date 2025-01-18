package com.example.readers

trait RddReaderUtil {

  val getRecord = (values: Iterator[String],
                   sep: Char,
                   headerPos: Byte,
                   headerField: String) => values
    .map(_.split(sep))
    .filter(_(headerPos) != headerField)
}
