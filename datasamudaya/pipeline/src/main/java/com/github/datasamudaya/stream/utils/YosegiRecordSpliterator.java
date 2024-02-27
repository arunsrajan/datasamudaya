package com.github.datasamudaya.stream.utils;

import static java.util.Objects.nonNull;

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
    List<Integer> reqcols;
    Map<String,SqlTypeName> sqltypename;
    List<String> allcols;
    public YosegiRecordSpliterator(YosegiSchemaReader reader, List<Integer> reqcols,
    		List<String> allcols, Map<String,SqlTypeName> sqltypename) {
        super(Long.MAX_VALUE, Spliterator.ORDERED);
        this.reader = reader;
        this.reqcols = reqcols;
        this.allcols = allcols;
        this.sqltypename = sqltypename;
    }
    public void getValueFromIParser(Object[] valueobjects, Object[] toconsider, String col, IParser cv, int index) {
    	try {
    		PrimitiveObject po = cv.get(col);
    		if(po instanceof IntegerObj iobj) {
    			valueobjects[index]=iobj.getInt();
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider[index]=posqlcount.getBoolean();
    		} else if(po instanceof LongObj lobj) {
    			valueobjects[index]=lobj.getLong();
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider[index]=posqlcount.getBoolean();
    		} else if(po instanceof FloatObj fobj) {
    			valueobjects[index]=fobj.getFloat();
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider[index]=posqlcount.getBoolean();
    		} else if(po instanceof DoubleObj dobj) {
    			valueobjects[index]=dobj.getDouble();
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			toconsider[index]=posqlcount.getBoolean();
    		} else if(po instanceof BooleanObj boolobj) {
    			valueobjects[index]=boolobj.getBoolean();
    			toconsider[index]=true;
    		} else if(po instanceof StringObj sobj) {
    			valueobjects[index]=sobj.getString();
    			toconsider[index]=true;
    		} else if(po instanceof ByteObj bobj) {
    			valueobjects[index]=Long.valueOf(bobj.getByte());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			if(nonNull(posqlcount)) {
    				toconsider[index]=posqlcount.getBoolean();
    			} else {
    				toconsider[index]=false;
    			}
    		} else if(po instanceof ShortObj shobj) {
    			valueobjects[index]=Long.valueOf(shobj.getShort());
    			PrimitiveObject posqlcount = cv.get(col+DataSamudayaConstants.SQLCOUNTFORAVG);
    			if(nonNull(posqlcount)) {
    				toconsider[index]=posqlcount.getBoolean();
    			} else {
    				toconsider[index]=false;
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
    			Object[] values = new Object[allcols.size()];
    			Object[] toconsider = new Object[allcols.size()];
    			reqcols.stream().forEach(index -> {
    				getValueFromIParser(values, toconsider, allcols.get(index), parser, index);
    			});
    			Object[] valuewithconsideration = new Object[2];
    			valuewithconsideration[0] = values;
    			valuewithconsideration[1] = toconsider;
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
