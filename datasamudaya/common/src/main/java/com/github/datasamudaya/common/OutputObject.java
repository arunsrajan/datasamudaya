package com.github.datasamudaya.common;

import java.io.Serializable;

public record OutputObject(Object value, boolean left, boolean right) implements Serializable {}
