package com.github.datasamudaya.stream.utils;

import static java.util.Objects.nonNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

import org.apache.calcite.sql.type.SqlTypeName;

import com.github.datasamudaya.common.DataSamudayaConstants;

import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.reader.YosegiSchemaReader;

public class YosegiRecordSpliterator extends Spliterators.AbstractSpliterator<Map<String, Object>> {
    private final YosegiSchemaReader reader;
    List<String> reqcols;
    Map<String,SqlTypeName> sqltypename;
    public YosegiRecordSpliterator(YosegiSchemaReader reader, List<String> reqcols,
    		Map<String,SqlTypeName> sqltypename) {
        super(Long.MAX_VALUE, Spliterator.ORDERED);
        this.reader = reader;
        this.reqcols = reqcols;
        this.sqltypename = sqltypename;
    }
    public void getValueFromIParser(Map<String, Object> map, String col, IParser cv) {
    	try {
    		PrimitiveObject po = cv.get(col);
    		if(po instanceof IntegerObj iobj) {
    			map.put(col, iobj.getInt());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, posqlcount.getInt());
    		} else if(po instanceof LongObj lobj) {
    			map.put(col, lobj.getLong());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, posqlcount.getInt());
    		} else if(po instanceof FloatObj fobj) {
    			map.put(col, fobj.getFloat());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, posqlcount.getInt());
    		} else if(po instanceof DoubleObj dobj) {
    			map.put(col, dobj.getDouble());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, posqlcount.getInt());
    		} else if(po instanceof StringObj sobj) {
    			map.put(col, sobj.getString());
    		} else if(po instanceof ByteObj bobj) {
    			map.put(col, Long.valueOf(bobj.getByte()));
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			if(nonNull(posqlcount)) {
    				map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, posqlcount.getInt());
    			} else {
    				map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, 0);
    			}
    		} else if(po instanceof ShortObj shobj) {
    			map.put(col, Long.valueOf(shobj.getShort()));
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			if(nonNull(posqlcount)) {
    				map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, posqlcount.getInt());
    			} else {
    				map.put(col+DataSamudayaConstants.SQLCOUNTFORAVG, 0);
    			}
    		}
    	} catch(Exception ex) {
    		ex.printStackTrace();
    	}
	}
    @Override
    public boolean tryAdvance(Consumer<? super Map<String, Object>> action) {    
        try {
        	if(!reader.hasNext()) {
        		return false;
        	}
        	while (reader.hasNext()) {
    			IParser parser = reader.next();
    			Map<String, Object> map = new LinkedHashMap<>();
    			reqcols.stream().forEach(col -> {
    				getValueFromIParser(map, col, parser);
    			});
    			action.accept(map);
    		}
    		reader.close();         
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
