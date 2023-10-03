package com.github.datasamudaya.common;

import java.util.Map;
import java.util.HashMap;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CompressedVectorSchemaRoot {	
	Map<String, String> columnvectorschemarootkeymap = new HashMap<>();
	Map<String, String> vectorschemarootkeybytesmap = new HashMap<>();
	Integer recordcount;
}
