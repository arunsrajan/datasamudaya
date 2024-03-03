package com.github.datasamudaya.stream.executors.actors;

import java.io.Serializable;

public record OutputObject(Object value, boolean left, boolean right) implements Serializable {}
