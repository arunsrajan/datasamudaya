package com.github.datasamudaya.stream.utils;

import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

import org.apache.calcite.sql.type.SqlTypeName;

import com.github.datasamudaya.common.DataSamudayaConstants;

import jp.co.yahoo.yosegi.message.objects.BooleanObj;
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

public class YosegiRecordSpliterator extends Spliterators.AbstractSpliterator<Object[]> {
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
    public void getValueFromIParser(List<Object> valueobjects, List<Boolean> toconsider, String col, IParser cv) {
    	try {
    		PrimitiveObject po = cv.get(col);
    		if(po instanceof IntegerObj iobj) {
    			valueobjects.add(iobj.getInt());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider.add(posqlcount.getBoolean());
    		} else if(po instanceof LongObj lobj) {
    			valueobjects.add(lobj.getLong());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider.add(posqlcount.getBoolean());
    		} else if(po instanceof FloatObj fobj) {
    			valueobjects.add(fobj.getFloat());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider.add(posqlcount.getBoolean());
    		} else if(po instanceof DoubleObj dobj) {
    			valueobjects.add(dobj.getDouble());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider.add(posqlcount.getBoolean());
    		} else if(po instanceof BooleanObj boolobj) {
    			valueobjects.add(boolobj.getBoolean());
    			toconsider.add(true);
    		} else if(po instanceof StringObj sobj) {
    			valueobjects.add(sobj.getString());
    			toconsider.add(true);
    		} else if(po instanceof ByteObj bobj) {
    			valueobjects.add(Long.valueOf(bobj.getByte()));
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			if(nonNull(posqlcount)) {
    				toconsider.add(posqlcount.getBoolean());
    			} else {
    				toconsider.add(false);
    			}
    		} else if(po instanceof ShortObj shobj) {
    			valueobjects.add(Long.valueOf(shobj.getShort()));
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			if(nonNull(posqlcount)) {
    				toconsider.add(posqlcount.getBoolean());
    			} else {
    				toconsider.add(false);
    			}
    		}
    	} catch(Exception ex) {
    		ex.printStackTrace();
    	}
	}
    @Override
    public boolean tryAdvance(Consumer<? super Object[]> action) {    
        try {
        	if(!reader.hasNext()) {
        		return false;
        	}
        	while (reader.hasNext()) {
    			IParser parser = reader.next();
    			List<Object> values = new ArrayList<>();
    			List<Boolean> toconsider = new ArrayList<>();
    			reqcols.stream().forEach(col -> {
    				getValueFromIParser(values, toconsider, col, parser);
    			});
    			Object[] valuewithconsideration = new Object[2];
    			valuewithconsideration[0] = values.toArray(new Object[0]);
    			valuewithconsideration[1] = toconsider.toArray(new Object[0]);
    			action.accept(valuewithconsideration);
    		}
    		reader.close();         
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
