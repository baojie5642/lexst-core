/**
 *
 */
package com.lexst.db.column;

import java.io.*;

import com.lexst.db.*;
import com.lexst.db.field.*;
import com.lexst.util.*;

public class WChar extends Word {
	private static final long serialVersionUID = 1L;

	private byte[] value;

	/**
	 *
	 */
	public WChar() {
		super(Type.WCHAR);
	}

	/**
	 * @param type
	 * @param id
	 */
	public WChar(short id) {
		super(Type.WCHAR, id);
	}

	/**
	 * @param id
	 * @param value
	 */
	public WChar(short id, byte[] value) {
		this(id);
		this.setValue(value);
	}

	/**
	 * @param b
	 */
	public void setValue(byte[] b) {
		if (b != null && b.length > 0) {
			value = new byte[b.length];
			System.arraycopy(b, 0, value, 0, b.length);
			this.setNull(false);
		} else {
			value = null;
			this.setNull(true);
		}
		setHash(value);
	}

	/**
	 * value
	 * @return
	 */
	public byte[] getValue() {
		return this.value;
	}

	@Override
	public byte[] bytes() {
		return value;
	}

	/* (non-Javadoc)
	 * @see com.lexst.db.column.Column#build(java.io.ByteArrayOutputStream, java.io.ByteArrayOutputStream)
	 */
	@Override
	public int build(ByteArrayOutputStream head, ByteArrayOutputStream body) {
		byte tag = (byte) (isNull() ? 1 : 0);
		tag <<= 6;
		tag |= (getType() & 0x3f);
		head.write(tag);

		int size = 1;
		if (!isNull()) {
			int count = (value == null ? 0 : value.length);
			byte[] b = com.lexst.util.Numeric.toBytes(count);
			head.write(b, 0, b.length);
			size += b.length;
			if (count > 0) {
				body.write(value, 0, value.length);
				size += count;
			}
		}
		return size;
	}

	/* (non-Javadoc)
	 * @see com.lexst.db.column.Column#parse(byte[], int, int)
	 */
	@Override
	public int[] resolve(byte[] b, int headoff, int bodyoff) {
		int headCount = 0, bodyCount = 0;
		// parse head
		byte tag = b[headoff++];
		headCount += 1;

		byte less = (byte) ((tag >>> 6) & 0x3);
		setNull(less == 1);
		byte type = (byte) (tag & 0x3f);
		if (!match_type(type)) {
			return null;
		}
		// if not null
		if (!isNull()) {
			int len = Numeric.toInteger(b, headoff, 4);
			headCount += 4; headoff += 4;
			if (len > 0) {
				if (bodyoff + len > b.length) {
					return null;
				}
				value = new byte[len];
				System.arraycopy(b, bodyoff, value, 0, len);
				bodyCount = len;
			}
		}
		return new int[] { headCount, bodyCount };
	}

	public boolean equals(Object arg) {
		if (arg == null || arg.getClass() != WChar.class) {
			return false;
		}
		if (field == null || field.getClass() != WCharField.class) {
			return false;
		}
		
		WCharField fid = (WCharField)field;
		WChar object = (WChar)arg;
		return match(fid.isSentient(), value, object.value);
	}
}
