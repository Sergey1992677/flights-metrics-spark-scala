package com.example.jobs

trait ReadJob[T] {

  def read():  T
}