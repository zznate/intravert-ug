package org.usergrid.vx.experimental.scan;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.usergrid.vx.experimental.IntraOp;
import org.usergrid.vx.experimental.IntraService;
import org.usergrid.vx.experimental.IntraState;
import org.vertx.java.core.Vertx;

public class PeopleFromNY implements ScanFilter {

	public PeopleFromNY() {
	}

	@Override
	public boolean filter(Map currentRow, ScanContext c) {
		String val = null;
		try {
			val = ByteBufferUtil.string((ByteBufferUtil
					.clone((ByteBuffer) currentRow.get("value"))));
			((ByteBuffer) currentRow.get("value")).rewind();
			((ByteBuffer) currentRow.get("name")).rewind();
		} catch (CharacterCodingException e) {
			System.err.println(e);
		}
		System.out.println("Got value " + val);
		if (val.equals("NY")) {
			System.out.println(val + " was ny collecting");
			c.collect(currentRow);
			return true;
		} else {
			System.out.println("returning true not collecting");
			return true;
		}
	}

}