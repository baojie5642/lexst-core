/**
 *
 */
package com.lexst.sql;

import java.util.*;
import java.util.regex.*;

import com.lexst.db.*;
import com.lexst.db.account.*;
import com.lexst.db.charset.*;
import com.lexst.db.column.Column;
import com.lexst.db.column.Char;
import com.lexst.db.column.NChar;
import com.lexst.db.column.WChar;
import com.lexst.db.column.RWChar;
import com.lexst.db.column.Real;
import com.lexst.db.column.Raw;
import com.lexst.db.column.TimeStamp;
import com.lexst.db.column.RNChar;
import com.lexst.db.column.RChar;
import com.lexst.db.column.Time;
import com.lexst.db.field.*;
import com.lexst.db.index.*;
import com.lexst.db.row.*;
import com.lexst.db.schema.*;
import com.lexst.db.statement.*;
import com.lexst.site.*;
import com.lexst.util.*;
import com.lexst.util.datetime.*;


public class SQLParser {

	/**
	 *
	 */
	public SQLParser() {
		super();
	}
	
	// ipv4 address
	private final static String IPv4 = "^\\s*([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\s*$";

	// show charset
	private final static String SHOW_CHARSET2 = "^\\s*(?i)SHOW\\s+(?i)CHARSET\\s*$";
	private final static String SHOW_CHARSET = "^\\s*(?i)SHOW\\s+(?i)CHARSET\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";

	// eg: create database|schema db_name char=utf8 nachar=utf16 wchar=utf32 maxsize=256m
	private final static String CREATE_SCHEMA = "^\\s*(?i)CREATE\\s+(?:(?i)SCHEMA|DATABASE)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*(.*)$";
	private final static String CREATE_SCHEMA_CHARSET = "^\\s*(?i)(CHAR|NCHAR|WCHAR)\\s*[=]\\s*([a-zA-Z0-9]{1,}[_-a-zA-Z0-9]*)\\s*(.*)$";
	private final static String MAXSIZE = "^\\s*(?i)MAXSIZE\\s*[=]\\s*([0-9]{1,})(?i)(M|G|T|P)\\s*(.*)$";

	private final static String DROP_SCHEMA = "^\\s*(?i)DROP\\s+(?:(?i)SCHEMA|DATABASE)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String SHOW_SCHEMA = "^\\s*(?i)SHOW\\s+(?:(?i)SCHEMA|DATABASE)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";

	// eg: create user XXX password 'XXX' or create user XXX identified by 'XXX' or create user XXX password=[XXX]
	private final static String CREATE_USER1  = "^\\s*(?i)CREATE\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s+(?i)IDENTIFIED\\s+(?i)BY\\s+\\'(\\p{Graph}{1,})\\'\\s*(.*)$";
	private final static String CREATE_USER2 = "^\\s*(?i)CREATE\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s+(?i)PASSWORD\\s+\\'(\\p{Graph}{1,})\\'\\s*(.*)$";
	private final static String CREATE_USER3 = "^\\s*(?i)CREATE\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s+(?i)PASSWORD\\s*[=]\\s*(\\p{Graph}{1,})\\s+(.*)$";
	private final static String CREATE_USER4 = "^\\s*(?i)CREATE\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s+(?i)PASSWORD\\s*[=]\\s*(\\p{Graph}{1,})\\s*$";
	
	// eg: drop user pentium
	private final static String DROP_USER = "^\\s*(?i)DROP\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s*$";
	// eg: drop sha1 user 2339000
	private final static String DROP_SHA1USER = "^\\s*(?i)DROP\\s+(?)SHA1\\s+(?i)USER\\s+([0-9a-fA-F]{40})\\s*$";
	
	// eg: alter user XXX identified by XXX or alter user XXX password 'XXX' or alter user XXX password=XXX
	private final static String ALTER_USER1  = "^\\s*(?i)ALTER\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s+(?i)IDENTIFIED\\s+(?i)BY\\s+\\'(\\p{Graph}{1,})\\'\\s*$";
	private final static String ALTER_USER2 = "^\\s*(?i)ALTER\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s+(?i)PASSWORD\\s+\\'(\\p{Graph}{1,})\\'\\s*$";
	private final static String ALTER_USER3 = "^\\s*(?i)ALTER\\s+(?i)USER\\s+(\\p{Graph}{1,})\\s+(?i)PASSWORD\\s*[=]\\s*(\\p{Graph}{1,})\\s*$";
	
	// eg: grant select, delete, update, dba, on db.table to user
	private final static String GRANT_SPACE = "^\\s*(?i)GRANT\\s+(\\p{Print}{1,})\\s+(?i)ON\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)\\s*$";
	private final static String GRANT_SCHEMA = "^\\s*(?i)GRANT\\s+(\\p{Print}{1,})\\s+(?i)ON\\s+(?:(?i)SCHEMA|DATABASE)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)\\s*$";
	private final static String GRANT_USER = "^\\s*(?i)GRANT\\s+(\\p{Print}{1,})\\s+(?i)TO\\s+(.+)\\s*$";

	// eg: revoke all from user_name or revoke
	private final static String REVOKE_SPACE = "^\\s*(?i)REVOKE\\s+(\\p{Print}{1,})\\s+(?i)ON\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)FORM\\s+(.+)\\s*$";
	private final static String REVOKE_SCHEMA = "^\\s*(?i)REVOKE\\s+(\\p{Print}{1,})\\s+(?i)ON\\s+(?:(?i)SCHEMA|DATABASE)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)FROM\\s+(.+)\\s*$";
	private final static String REVOKE_USER = "^\\s*(?i)REVOKE\\s+(\\p{Print}{1,})\\s+(?i)FROM\\s+(.+)\\s*$";

	// eg: create table database.table (id int, word char not null like not case default 'pentium', tag long)
	private final static String CREATE_TABLE = "^\\s*(?i)CREATE\\s+(?i)TABLE([\\p{Print}]*)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*\\((.+)\\)\\s*$";
	// eg: create index database.table (id primary, word(128) )
	private final static String CREATE_INDEX = "^\\s*(?i)CREATE\\s+(?i)INDEX\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*\\((.+)\\)\\s*$";
	// eg. create logout database.table (colname1 asc, colname2 desc, colname2 asc)
	private final static String CREATE_LAYOUT = "^\\s*(?i)CREATE\\s+(?i)LAYOUT\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*\\((.+)\\)\\s*$";
	// eg: drop table video.item
	private final static String DROP_TABLE = "^\\s*(?i)DROP\\s+(?i)TABLE\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	// eg: show table video.item
	private final static String SHOW_TABLE = "^\\s*(?i)SHOW\\s+(?i)TABLE\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";

	/* RAW|BINARY|CHAR|NCHAR|WCHAR|SHORT|SMALLINT|INT|INTEGER|LONG|BIGINT|REAL|DOUBLE|TIMESTAMP|DATE|TIME */
	private final static String TABLE_COLUMN = "^\\s*(?i)([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+([a-zA-Z0-9]{1,})(.*)$";

	// table column's element
	private final static String TABLE_COLUMN_NULL = "^\\s*(?i)(NULL)(.*)$";
	private final static String TABLE_COLNUM_LIKE = "^\\s*(?i)(LIKE)(.*)$";
	private final static String TABLE_COLUMN_CASE = "^\\s*(?i)(CASE)(.*)$"; //sentient
	// "NOT" column
	private final static String TABLE_COLUMN_NOTNULL = "^\\s*(?i)(NOT)\\s+(?i)(NULL)(.*)$";
	private final static String TABLE_COLUMN_NOTLIKE = "^\\s*(?i)(NOT)\\s+(?i)(LIKE)(.*)$";
	private final static String TABLE_COLUMN_NOTCASE = "^\\s*(?i)(NOT)\\s+(?i)(CASE)(.*)$";
	// compress type
	private final static String TABLE_COLUMN_PACKING = "^\\s*(?i)(PACKING)\\s*([a-zA-Z0-9]{1,}[-_a-zA-Z0-9]*)(.*)$";
	// default text syntax
	// 字符串类型(包括字符和日期格式,日期函数) reg_defaultchar reg_defaultdigit
	private final static String TABLE_COLUMN_DEFCHAR = "^\\s*(?i)(DEFAULT)\\s+\\'(.+)\\'(.*)$";
	// default digiti syntax
	private final static String TABLE_COLUMN_DEFDIGIT = "^\\s*(?i)(DEFAULT)\\s+([-+]?\\d*\\.?\\d*)(.*)$";
	private final static String[] TABLE_ALLCOLUMN = { TABLE_COLUMN_NULL,
			TABLE_COLNUM_LIKE, TABLE_COLUMN_CASE, TABLE_COLUMN_NOTNULL,
			TABLE_COLUMN_NOTLIKE, TABLE_COLUMN_NOTCASE, TABLE_COLUMN_PACKING,
			TABLE_COLUMN_DEFCHAR, TABLE_COLUMN_DEFDIGIT };

	// layout column syntax
	private final static String LAYOUT_COLUMN1 = "^\\s*(?i)([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)(ASC|DESC)\\s*$";
	private final static String LAYOUT_COLUMN2 = "^\\s*(?i)([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";

	// table prefix syntax
	private final static String TABLE_PREFIX_HOSTMODE1 = "^\\s*(?i)HOSTMODE\\s*=\\s*(?i)(SHARE|EXCLUSIVE)\\s+(.*)$";
	private final static String TABLE_PREFIX_HOSTMODE2 = "^\\s*(?i)HOSTMODE\\s*=\\s*(?i)(SHARE|EXCLUSIVE)\\s*$";
	private final static String TABLE_PREFIX_HOSTCACHE1 = "^\\s*(?i)HOSTCACHE\\s*=\\s*(?i)(YES|NO)\\s+(.*)$";
	private final static String TABLE_PREFIX_HOSTCACHE2 = "^\\s*(?i)HOSTCACHE\\s*=\\s*(?i)(YES|NO)\\s*$";
	private final static String TABLE_PREFIX_CHUNKSIZE1 = "^\\s*(?i)CHUNKSIZE\\s*=\\s*([0-9]{1,})(?i)M\\s+(.*)$";
	private final static String TABLE_PREFIX_CHUNKSIZE2 = "^\\s*(?i)CHUNKSIZE\\s*=\\s*([0-9]{1,})(?i)M\\s*$";
	private final static String TABLE_PREFIX_CHUNKCOPY1 = "^\\s*(?i)CHUNKCOPY\\s*=\\s*([0-9]{1,})\\s+(.*)$";
	private final static String TABLE_PREFIX_CHUNKCOPY2 = "^\\s*(?i)CHUNKCOPY\\s*=\\s*([0-9]{1,})\\s*$";
	private final static String TABLE_PREFIX_PRIMEHOST1 = "^\\s*(?i)PRIMEHOST\\s*=\\s*([0-9]{1,})\\s+(.*)$";
	private final static String TABLE_PREFIX_PRIMEHOST2 = "^\\s*(?i)PRIMEHOST\\s*=\\s*([0-9]{1,})\\s*$";
	private final static String TABLE_PREFIX_CLUSTERS_NUM1 = "^\\s*(?i)CLUSTERS\\s*=\\s*([0-9]{1,})\\s+(.*)$";
	private final static String TABLE_PREFIX_CLUSTERS_NUM2 = "^\\s*(?i)CLUSTERS\\s*=\\s*([0-9]{1,})\\s*$";
	private final static String TABLE_PREFIX_CLUSTERS_IP = "^\\s*(?i)CLUSTERS\\s*=\\s*\\\"([0-9\\.\\,\\s]*)\\\"\\s*(.*)$";

	// index item syntax
	private final static String INDEX_COLUMN_NAME = "^\\s*(?i)([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String INDEX_COLUMN_PRIMARY = "^\\s*(?i)([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)(PRIMARY)\\s*$";
	private final static String INDEX_COLUMN_PRESIZE = "^\\s*(?i)([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*\\(([0-9]{1,})\\)\\s*$";
	private final static String INDEX_COLUMN_TOTAL = "^\\s*(?i)([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*\\(([0-9]{1,})\\)\\s+(?i)(PRIMARY)\\s*$";
	private final static String[] INDEX_ALLCOLUMN = { INDEX_COLUMN_NAME, INDEX_COLUMN_PRESIZE, INDEX_COLUMN_PRIMARY, INDEX_COLUMN_TOTAL };

	private final static String SQL_SELECT_ALL    = "^\\s*(?i)SELECT\\s+(.+)\\s+(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)WHERE\\s+(.+)\\s+(?i)ORDER\\s+(?i)BY\\s+(.+)\\s*$";
	private final static String SQL_SELECT_SINGLE = "^\\s*(?i)SELECT\\s+(.+)\\s+(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String SQL_SELECT_WHERE  = "^\\s*(?i)SELECT\\s+(.+)\\s+(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)WHERE\\s+(.+)\\s*$";

	private final static String SQL_DELETE = "^\\s*(?i)DELETE\\s+(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)WHERE\\s+(.+)\\s*$";
	
	private final static String SQL_UPDATE = "^\\s*(?i)UPDATE\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)SET\\s+(.+)\\s+(?i)WHERE\\s+(.+)\\s*$";
	private final static String SQL_UPDATE_SET1 = "^\\s*([-_a-zA-Z0-9]{1,})\\s*=\\s*(.+)\\s*$";
	private final static String SQL_UPDATE_SET2 = "^\\s*([-_a-zA-Z0-9]{1,})\\s*=\\s*\\'(.+)\\'\\s*$";
	private final static String SQL_UPDATE_SET3 = "^\\s*([-_a-zA-Z0-9]{1,})\\s*=\\s*(.+)\\s*\\,\\s*(.+)$";
	private final static String SQL_UPDATE_SET4 = "^\\s*([-_a-zA-Z0-9]{1,})\\s*=\\s*\\'(.+)\\'\\s*\\,\\s*(.+)$";

	private final static String SQL_SELECT_PREFIX_TOP = "^\\s*(?i)TOP\\s+(\\d+)(.*)$";
	private final static String SQL_SELECT_PREFIX_RANGE = "^\\s*(?i)RANGE\\s*\\(\\s*(\\d+)\\s*\\,\\s*(\\d+)\\s*\\)(.*)$";

	private final static String SQL_WHERE_SUFFIX = "^\\s*(.*)\\s+(?i)(AND|OR)\\s*$";
	private final static String SQL_WHERE_ELEMENT = "^\\s*(.*)\\s+(?i)(AND|OR)\\s+(.*)\\s*$";

	private final static String SQL_WHERE_ELEMENT_PREFIX = "^\\s*(?i)(AND|OR)\\s+(.+)\\s*$";
	private final static String SQL_WHERE_CHAR1 = "^\\s*(?i)(AND|OR)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*(?i)(=|!=|<>|>|>=|<|<=|LIKE)\\s*\\'(.+)\\'\\s*$"; //字符
	private final static String SQL_WHERE_CHAR2 = "^\\s*([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*(?i)(=|!=|<>|>|>=|<|<=|LIKE)\\s*\\'(.+)\\'\\s*$"; //字符
	private final static String SQL_WHERE_NUM1  = "^\\s*(?i)(AND|OR)\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*(?i)(=|!=|<>|>|>=|<|<=|LIKE)\\s*(.+)\\s*$"; //数字
	private final static String SQL_WHERE_NUM2  = "^\\s*([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*(?i)(=|!=|<>|>|>=|<|<=|LIKE)\\s*(.+)\\s*$"; //数字

	private final static String SQL_ORDERBY = "^\\s*([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)(ASC|DESC)\\s*$";

	private final static String SQL_INSERT = "^\\s*(?i)INSERT\\s+(?i)INTO\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+\\((.+)\\)\\s+(?i)VALUES\\s*\\((.+)\\)\\s*$";
	private final static String SQL_INJECT = "^\\s*(?i)INJECT\\s+(?i)INTO\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+\\((.+)\\)\\s+(?i)VALUES\\s*(.+)\\s*$";

	private final static String SQL_DATE_STYLE = "^\\s*([0-9]{4})[.-]([0-9]{1,2})[.-]([0-9]{1,2})\\s*$";
	private final static String SQL_TIME_STYLE = "^\\s*([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2}).([0-9]{1,3})\\s*$";
	private final static String SQL_TIME_STYLE2 = "^\\s*([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*$";
	private final static String SQL_TIMESTAMP_STYLE = "^\\s*([0-9]{4})[.-]([0-9]{1,2})[.-]([0-9]{1,2})\\s*([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*";

	private final static String SET_CHUNKSIZE = 
		"^\\s*(?i)SET\\s+(?i)CHUNKSIZE\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+([0-9]{1,})(?i)M\\s*$";
	
	private final static String SET_OPTITIME =
		"^\\s*(?i)SET\\s+(?i)OPTIMIZE\\s+(?i)TIME\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)(HOURLY|DAILY|WEEKLY|MONTHLY)\\s+\\'(.+)\\'\\s*$";
	
	private final static String SQL_HOURLY = "^\\s*([0-9]{1,2}):([0-9]{1,2})\\s*$";
	private final static String SQL_DAILY = "^\\s*([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*$";
	private final static String SQL_WEEKLY = "^\\s*([0-9]{1})\\s+([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*$";
	private final static String SQL_MONTHLY = "^\\s*([0-9]{1,2})\\s+([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})\\s*$";
	
	// load and stop index
	private final static String LOAD_INDEX1  = "^\\s*(?i)LOAD\\s+(?i)INDEX\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String LOAD_INDEX2 = "^\\s*(?i)LOAD\\s+(?i)INDEX\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)$";
	private final static String STOP_INDEX1 =  "^\\s*(?i)STOP\\s+(?i)INDEX\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String STOP_INDEX2 = "^\\s*(?i)STOP\\s+(?i)INDEX\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)$";;

	// load and stop chunk
	private final static String LOAD_CHUNK1 =  "^\\s*(?i)LOAD\\s+(?i)CHUNK\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String LOAD_CHUNK2 = "^\\s*(?i)LOAD\\s+(?i)CHUNK\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)$";
	private final static String STOP_CHUNK1 =  "^\\s*(?i)STOP\\s+(?i)CHUNK\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String STOP_CHUNK2 = "^\\s*(?i)STOP\\s+(?i)CHUNK\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)$";;

	// optimize
	private final static String OPTIMIZE1 = "^\\s*(?i)OPTIMIZE\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String OPTIMIZE2 = "^\\s*(?i)OPTIMIZE\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)$";

	// build naming task
	private final static String BUILD_TASK =  "^\\s*(?i)BUILD\\s+(?i)TASK\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";
	private final static String BUILD_TASK2 = "^\\s*(?i)BUILD\\s+(?i)TASK\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)TO\\s+(.+)$";;

	// dc style
	private final static String DC1 = "^\\s*(?i)DC\\s+(?i)FROM\\s+(.+)\\s+(?i)TO\\s+(.+)\\s+(?i)COLLECT\\s+(.+)\\s*";
	private final static String DC2 = "^\\s*(?i)DC\\s+(?i)FROM\\s+(.+)\\s+(?i)TO\\s+(.+)\\s*$";
	// adc style
	private final static String ADC1 = "^\\s*(?i)ADC\\s+(?i)FROM\\s+(.+)\\s+(?i)TO\\s+(.+)\\s+(?i)COLLECT\\s+(.+)\\s*"; 
	private final static String ADC2 = "^\\s*(?i)ADC\\s+(?i)FROM\\s+(.+)\\s+(?i)TO\\s+(.+)\\s*$";

	private final static String DC_NAMING = "^\\s*(?i)NAMING\\s*:\\s*([a-zA-Z0-9]{1,}[_-a-zA-Z0-9]*)\\s*(.*)$";
	private final static String DC_SITES = "^\\s*(?i)SITES\\s*:\\s*([0-9]{1,})\\s*(.*)$";
	private final static String DC_QUERY = "^\\s*(?i)QUERY\\s*:\\s*\\\"(.+?)\\\"\\s*(.*)$";
	private final static String DC_VALUES = "^\\s*(?i)VALUES\\s*:\\s*\\\"(.+?)\\\"\\s*(.*)$";
	private final static String DC_BLOCKS = "^\\s*(?i)BLOCKS\\s*:\\s*([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)%([0-9]{1,})\\s*(.*)$";
	private final static String DC_SPACE = "^\\s*(?i)SHOW\\s*:\\s*([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*(.*)$";
	private final static String DC_WRITETO = "^\\s*(?i)WRITETO\\s*:\\s*([a-zA-Z/]{1,}[/\\a-zA-Z0-9]*)\\s*(.*)$";

	private final static String DC_VALUE_BOOLEAN = "^\\s*([a-zA-Z]{1}[\\w-]*)\\s*=\\s*(?i)(FALSE|TRUE)\\s*(.*)$";
	private final static String DC_VALUE_DOUBLE = "^\\s*([a-zA-Z]{1}[\\w-]*)\\s*=\\s*([-]{0,1}[0-9]{1,16}[.][0-9]{1,12})\\s*(.*)$";
	private final static String DC_VALUE_LONG = "^\\s*([a-zA-Z]{1}[\\w-]*)\\s*=\\s*([-]{0,1}[0-9]{1,16})\\s*(.*)$";
	private final static String DC_VALUE_RAW = "^\\s*([a-zA-Z]{1}[\\w-]*)\\s*=\\s*(?i)0X([0-9a-fA-F]+)\\s*(.*)$";
	private final static String DC_VALUE_CHARS = "^\\s*([a-zA-Z]{1}[\\w-]*)\\s*=\\s*\\'(.+?)\\'\\s*(.*)$";
	private final static String DC_VALUE_TIMESTAMP = "^\\s*([0-9]{2}|[0-9]{4})-([0-9}]{1,2})-([0-9]{1,2})\\s+([0-9]{1,2}):([0-9}]{1,2}):([0-9]{1,2})\\s*$";
	private final static String DC_VALUE_DATE = "^\\s*([0-9]{2}|[0-9]{4})-([0-9}]{1,2})-([0-9]{1,2})\\s*$";
	private final static String DC_VALUE_TIME = "^\\s*([0-9]{1,2}):([0-9}]{1,2}):([0-9]{1,2})\\s*$";
	private final static String DC_VALUE_FILTE = "^\\s*(?:,)\\s*(.*)$";
	
	/*
	 * 在限定符之后添加问号(?)，则使限定符成为“勉强模式”。
	 * 勉强模式的限定符，总是尽可能少的匹配。
	 * 如果之后的表达式匹配失败，勉强模式也可以尽可能少的再匹配一些，以使整个表达式匹配成功。
	 */
	
	// show site address
	private final static String SHOW_SITE1 = "^\\s*(?i)SHOW\\s+(?i)(ALL|HOME|LOG|DATA|WORK|BUILD|CALL)\\s+(?i)SITE\\s*$";
	private final static String SHOW_SITE2 = "^\\s*(?i)SHOW\\s+(?i)(ALL|HOME|LOG|DATA|WORK|BUILD|CALL)\\s+(?i)SITE\\s+(?i)FROM\\s+(.+)\\s*$";
	
	// get collect path
	private final static String SET_COLLECT_PATH = "^\\s*(?i)SET\\s+(?i)COLLECT\\s+(?i)PATH\\s+(.+)\\s*$";
	// get collect task name
	private final static String TEST_COLLECT_TASK = "^\\s*(?i)TEST\\s+(?i)COLLECT\\s+(?i)TASK\\s+(.+)\\s*$";
	
	/**
	 * throw a error
	 * @param sql
	 * @param index
	 */
	private void throwable(String sql, int index) throws SQLSyntaxException {
		StringBuilder b = new StringBuilder();
		if (index > 0) {
			char[] c = new char[index - 1];
			for (int i = 0; i < c.length; i++) {
				c[i] = 0x20;
			}
			b.append(c);
		}
		b.append('^');
		String s = String.format("%s\n%s\nsql syntax error", sql, b.toString());
		throw new SQLSyntaxException(s);
	}

	private void throwable(String sql, int index, String tip) throws SQLSyntaxException {
		StringBuilder b = new StringBuilder();
		if (index > 0) {
			char[] c = new char[index - 1];
			for (int i = 0; i < c.length; i++) {
				c[i] = 0x20;
			}
			b.append(c);
		}
		b.append('^');
		String s = String.format("%s\n%s\n%s", sql, b.toString(), tip);
		throw new SQLSyntaxException(s);
	}

	private boolean isIPv4(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.IPv4);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return false;
		}
		for (int i = 1; i <= 4; i++) {
			String s = matcher.group(i);
			int value = Integer.parseInt(s);
			if (value > 255) {
				return false;
			} else if (i == 1 && value < 1) {
				return false;
			}
		}
		return true;
	}
	
	private boolean isCreateSchemaOption(String sql) {
		String regex = "^\\s*(?i)CREATE\\s+(?:(?i)SCHEMA|DATABASE)\\s*$";		
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	private boolean isCreateTableOption(String sql) {
		String regex = "^\\s*(?i)CREATE\\s+(?i)TABLE\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	private boolean isCreateUserOption(String sql) {
		String regex = "^\\s*(?i)CREATE\\s+(?i)USER\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	private boolean isDropSchemaOption(String sql) {
		String regex = "^\\s*(?i)DROP\\s+(?:(?i)SCHEMA|DATABASE)\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	private boolean isDropTableOption(String sql) {
		String regex = "^\\s*(?i)DROP\\s+(?i)TABLE\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	private boolean isDropUserOption(String sql) {
		String regex = "^\\s*(?i)DROP\\s+(?i)USER\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	private boolean isAlterUserOption(String sql) {
		String regex = "^\\s*(?i)ALTER\\s+(?i)USER\\s*$";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	private int[] splitSQLControl(String[] options) {
		ArrayList<Integer> a = new ArrayList<Integer>();
		for (String opt : options) {
			opt = opt.trim();
			if ("select".equalsIgnoreCase(opt)) {
				a.add(Control.SELECT);
			} else if ("insert".equalsIgnoreCase(opt)) {
				a.add(Control.INSERT);
			} else if ("delete".equalsIgnoreCase(opt)) {
				a.add(Control.DELETE);
			} else if ("update".equalsIgnoreCase(opt)) {
				a.add(Control.UPDATE);
			} else if ("all".equalsIgnoreCase(opt)) {
				a.add(Control.ALL);
			} else if ("dba".equalsIgnoreCase(opt)) {
				a.add(Control.DBA);
			} else if ("GRANT".equalsIgnoreCase(opt)) {
				a.add(Control.GRANT);
			} else if ("REVOKE".equalsIgnoreCase(opt)) {
				a.add(Control.REVOKE);
			} else if (isCreateUserOption(opt)) {
				a.add(Control.CREATE_USER);
			} else if (isDropUserOption(opt)) {
				a.add(Control.DROP_USER);
			} else if(isAlterUserOption(opt)) {
				a.add(Control.ALTER_USER);
			} else if (isCreateSchemaOption(opt)) {
				a.add(Control.CREATE_SCHEMA);
			} else if (isDropSchemaOption(opt)) {
				a.add(Control.DROP_SCHEMA);
			} else if (isCreateTableOption(opt)) {
				a.add(Control.CREATE_TABLE);
			} else if (isDropTableOption(opt)) {
				a.add(Control.DROP_TABLE);
			} else {
				// error
				return null;
			}
		}
		
		int[] all = new int[a.size()];
		for (int i = 0; i < all.length; i++) {
			all[i] = a.get(i).intValue();
		}
		return all;
	}

	/**
	 * split "revoke" syntax
	 * @param sql
	 * @return
	 */
	public Permit splitRevoke(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.REVOKE_SPACE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitRevokeSchema(sql);
		}
		int count = matcher.groupCount();
		if (count != 4) {
			throwable(sql, 0);
		}

		String option = matcher.group(1);
		String schema = matcher.group(2);
		String table = matcher.group(3);
		String user = matcher.group(4);

		String[] options = option.split(",");
		String[] users = user.split(",");

		TablePermit permit = new TablePermit();
		permit.setUsers(users);
		int[] all = splitSQLControl(options);
		if(all == null || all.length != options.length) {
			throwable(sql, matcher.start(1));
		}

		Control ctrl = new Control();
		boolean success = ctrl.set(Permit.TABLE_PERMIT, all);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		permit.add(new Space(schema, table), ctrl);
		return permit;
	}

	/**
	 * @param sql
	 * @return
	 */
	private Permit splitRevokeSchema(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.REVOKE_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitRevokeUser(sql);
		}

		int count = matcher.groupCount();
		if (count != 3) {
			throwable(sql, 0);
		}
		String opt = matcher.group(1);
		String schema = matcher.group(2);
		String user = matcher.group(3);

		String[] opts = opt.split(",");
		String[] schemas = schema.split(",");
		String[] users = user.split(",");

		SchemaPermit permit = new SchemaPermit();
		permit.setUsers(users);
		int[] all = splitSQLControl(opts);
		if (all == null || all.length != opts.length) {
			throwable(sql, matcher.start(1));
		}

		Control ctrl = new Control();
		boolean success = ctrl.set(Permit.SCHEMA_PERMIT, all);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		for (String s : schemas) {
			permit.add(s, ctrl);
		}
		return permit;
	}

	private Permit splitRevokeUser(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.REVOKE_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal grant syntax!");
		}
		int count = matcher.groupCount();
		if(count != 2) {
			throwable(sql, 0);
		}

		String opt = matcher.group(1);
		String user = matcher.group(2);

		String[] opts = opt.split(",");
		String[] users = user.split(",");

		// default account(ip address)
		UserPermit permit = new UserPermit();
		permit.setUsers(users);
		int[] all = splitSQLControl(opts);
		if (all == null || all.length != opts.length) {
			throwable(sql, matcher.start(1));
		}
		Control ctrl = new Control();
		boolean success = ctrl.set(Permit.USER_PERMIT, all);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		for (String s : users) {
			permit.add(s, ctrl);
		}
		return permit;
	}

	/**
	 * split "grant" syntax
	 * @param sql
	 */
	public Permit splitGrant(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.GRANT_SPACE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitGrantSchema(sql);
		}
		int count = matcher.groupCount();
		if (count != 4) {
			throwable(sql, 0);
		}
		String opt = matcher.group(1);
		// space
		String db = matcher.group(2);
		String table = matcher.group(3);
		// user
		String user = matcher.group(4);
		// operation options
		String[] opts = opt.split(",");
		String[] users = user.split(",");

		TablePermit permit = new TablePermit();
		permit.setUsers(users);
		int[] all = splitSQLControl(opts);
		if(all == null || all.length != opts.length) {
			throwable(sql, matcher.start(1));
		}
		Control ctrl = new Control();
		boolean success = ctrl.set(Permit.TABLE_PERMIT, all);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		permit.add(new Space(db, table), ctrl);
		return permit;
	}

	private Permit splitGrantSchema(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.GRANT_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitGrantUser(sql);
		}

		int count = matcher.groupCount();
		if(count != 3) {
			throwable(sql, 0);
		}
		String option = matcher.group(1);
		String schema = matcher.group(2);
		String user = matcher.group(3);

		String[] options = option.split(",");
		String[] schemas = schema.split(",");
		String[] users = user.split(",");

		SchemaPermit permit = new SchemaPermit();
		permit.setUsers(users);
		int[] all = splitSQLControl(options);
		if(all == null || all.length != options.length) {
			throwable(sql, matcher.start(1));
		}

		Control ctrl = new Control();
		boolean success = ctrl.set(Permit.SCHEMA_PERMIT, all);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		for(String s : schemas) {
			permit.add(s, ctrl);
		}
		return permit;
	}

	private Permit splitGrantUser(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.GRANT_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal grant syntax!");
		}
		int count = matcher.groupCount();
		if(count != 2) {
			throwable(sql, 0);
		}

		String opt = matcher.group(1);
		String user = matcher.group(2);

		String[] opts = opt.split(",");
		String[] users = user.split(",");

		// default account(ip address)
		UserPermit permit = new UserPermit();
		permit.setUsers(users);
		int[] all = splitSQLControl(opts);
		if (all == null || all.length != opts.length) {
			throwable(sql, matcher.start(1));
		}
		// boolean success = permit.setOption(Permit.USER_PERMIT, all);
		Control ctrl = new Control();
		boolean success = ctrl.set(Permit.USER_PERMIT, all);
		if (!success) {
			throwable(sql, matcher.start(1));
		}
		for (String s : users) {
			permit.add(s, ctrl);
		}
		return permit;
	}
	
	/**
	 * split "create user" syntax
	 * @param sql
	 * @return
	 */
	public User splitCreateUser(String sql) {
		User user = null;
		String suffix = "";
		
		Pattern pattern = Pattern.compile(SQLParser.CREATE_USER1);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			user = new User(matcher.group(1), matcher.group(2));
			suffix = matcher.group(3);
		} else {
			pattern = Pattern.compile(SQLParser.CREATE_USER2);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				user = new User(matcher.group(1), matcher.group(2));
				suffix = matcher.group(3);
			} else {
				pattern = Pattern.compile(SQLParser.CREATE_USER3);
				matcher = pattern.matcher(sql);
				if (matcher.matches()) {
					user = new User(matcher.group(1), matcher.group(2));
					suffix = matcher.group(3);
				} else {
					pattern = Pattern.compile(SQLParser.CREATE_USER4);
					matcher = pattern.matcher(sql);
					if (!matcher.matches()) {
						throwable(sql, 0);
					}
					user = new User(matcher.group(1), matcher.group(2));
				}
			}
		}
		
		if (suffix.trim().length() > 0) {
			pattern = Pattern.compile(SQLParser.MAXSIZE);
			matcher = pattern.matcher(suffix);
			if (!matcher.matches()) {
				throwable(sql, 0);
			}
			String digit = matcher.group(1);
			String unit = matcher.group(2);
			long value = Long.parseLong(digit);
			if ("M".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * 1024 * 1024);
			} else if ("G".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * 1024 * 1024 * 1024);
			} else if ("T".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * 1024 * 1024 * 1024 * 1024);
			} else if ("P".equalsIgnoreCase(unit)) {
				user.setMaxSize(value * 1024 * 1024 * 1024 * 1024 * 1024);
			}
		}
		
		return user;
	}

	public User splitDropUser(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.DROP_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable(sql, 0);
		}
		// database name
		int count = matcher.groupCount();
		if(count != 1) {
			throwable(sql, 0);
		}
		String username = matcher.group(1);
		return new User(username);
	}
	
	public User splitDropSHA1User(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.DROP_SHA1USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throwable(sql, 0);
		}

		String hex = matcher.group(1);
		User user = new User();
		user.setHexUsername(hex);
		return user;
	}

	public User splitAlterUser(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.ALTER_USER1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			// return splitAlterUser2(sql);
			pattern = Pattern.compile(SQLParser.ALTER_USER2);
			matcher = pattern.matcher(sql);
			if (!matcher.matches()) {
				pattern = Pattern.compile(SQLParser.ALTER_USER3);
				matcher = pattern.matcher(sql);
				if (!matcher.matches()) {
					throwable(sql, 0);
				}
			}
		}
		int count = matcher.groupCount();
		if(count != 2) {
			throwable(sql, 0);
		}
		String username = matcher.group(1);
		String password = matcher.group(2);
		return new User(username, password);
	}

	/**
	 * resolve database
	 * @param sql
	 * @return
	 */
	private Schema splitSchema(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.CREATE_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String db_name = matcher.group(1);
		// suffix string
		String suffix = matcher.group(2);
		// save
		Schema schema = new Schema(db_name);

		// resolve suffix value
		while (suffix.trim().length() > 0) {
			pattern = Pattern.compile(SQLParser.CREATE_SCHEMA_CHARSET);
			matcher = pattern.matcher(suffix);
			if (matcher.matches()) {
				String type = matcher.group(1);
				String name = matcher.group(2);
				suffix = matcher.group(3);
				if ("CHAR".equalsIgnoreCase(type)) {
					schema.setCharset(name);
				} else if ("NCHAR".equalsIgnoreCase(type)) {
					schema.setNCharset(name);
				} else if ("WCHAR".equalsIgnoreCase(type)) {
					schema.setWCharset(name);
				}
				continue;
			}
			
			pattern = Pattern.compile(SQLParser.MAXSIZE);
			matcher = pattern.matcher(suffix);
			if (matcher.matches()) {
				String digit = matcher.group(1);
				String unit = matcher.group(2);
				suffix = matcher.group(3);

				long value = Long.parseLong(digit);
				if ("M".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * 1024 * 1024);
				} else if ("G".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * 1024 * 1024 * 1024);
				} else if ("T".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * 1024 * 1024 * 1024 * 1024);
				} else if ("P".equalsIgnoreCase(unit)) {
					schema.setMaxSize(value * 1024 * 1024 * 1024 * 1024 * 1024);
				}
				continue;
			}	
			throw new SQLSyntaxException("invalid syntax:" + suffix);
		}
		return schema;
	}

	/**
	 * @param sql
	 * @param grant
	 * @return
	 */
	public Schema splitCreateSchema(SQLCharmap map, String sql) {
		Schema schema = splitSchema(sql);
		if (schema == null) {
			this.throwable(sql, 0);
		}
		// check charset
		String charset = schema.getCharset();
		if (charset == null) charset = "UTF-8";
		SQLCharType mode = map.find(charset);
		if (mode == null) {
			throw new SQLSyntaxException("unknown charset '%s'", charset);
		}
		schema.setCharset(charset);
		// nchar set
		charset = schema.getNCharset();
		if (charset == null) charset = "UTF-16";
		mode = map.find(charset);
		if (mode == null) {
			throw new SQLSyntaxException("unknown narrow charset '%s'", charset);
		}
		schema.setNCharset(charset);
		// wchar set
		charset = schema.getWCharset();
		if (charset == null) charset = "UTF-32";
		mode = map.find(charset);
		if (mode == null) {
			throw new SQLSyntaxException("unknown wide charset '%s'", charset);
		}
		schema.setWCharset(charset);
		return schema;
	}

	/**
	 * @param sql
	 * @return
	 */
	public Schema splitDropSchema(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.DROP_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid syntax: " + sql);
		}
				
		String db_name = matcher.group(1);
		// save
		Schema schema = new Schema(db_name);
		return schema;
	}

	/**
	 * resolve sql to Table object
	 * @param sqlTable
	 * @param sqlIndex
	 * @param sqlLayout
	 * @return
	 */
	public Table splitCreateTable(String sqlTable, String sqlIndex, String sqlLayout) {
		Table table = this.splitTable(sqlTable, sqlIndex);
		if (sqlLayout != null && sqlLayout.trim().length() > 0) {
			this.splitLayout(table, sqlLayout);
		}
		
		// set clusters number
		Clusters clusters = table.getClusters();
		if (clusters.getNumber() == 0 && clusters.isEmpty()) {
			clusters.setNumber(1);
		}
		return table;
	}

	/**
	 * @param table
	 * @param sqlLayout
	 */
	private void splitLayout(Table table, String sqlLayout) {
		Pattern pattern = Pattern.compile(SQLParser.CREATE_LAYOUT);
		Matcher matcher = pattern.matcher(sqlLayout);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal sql's layout syntax!");
		}
		// must 3
		int count = matcher.groupCount();
		if (count != 3) {
			this.throwable(sqlLayout, 0);
		}

		// database name and table name
		String dbName = matcher.group(1);
		String tableName = matcher.group(2);
		// table column set
		String suffix = matcher.group(3);
		if (!Space.inSchemaSize(dbName.getBytes().length)
				|| !Space.inTableSize(tableName.getBytes().length)) {
			throw new SQLSyntaxException("database or table name size <= 64 word");
		}

		Space space = new Space(dbName, tableName);
		if(!table.getSpace().equals(space)) {
			throw new SQLSyntaxException("not match sql space");
		}

		Field pid = table.pid();
		if (pid == null) {
			throw new SQLSyntaxException("cannot find primary key");
		}
		//
		String[] items = suffix.split(",");
		if (items == null || items.length == 0) {
			throw new SQLSyntaxException("layout column missing");
		}
		Pattern pattern1 = Pattern.compile(SQLParser.LAYOUT_COLUMN1);
		Pattern pattern2 = Pattern.compile(SQLParser.LAYOUT_COLUMN2);
		Layout layout = null;
		for (String item : items) {
			String name, tag;
			Matcher matcher1 = pattern1.matcher(item);
			Matcher matcher2 = pattern2.matcher(item);
			if (matcher1.matches()) {
				name = matcher1.group(1);
				tag = matcher1.group(2);
			} else if (matcher2.matches()) {
				name = matcher2.group(1);
				tag = "ASC";
			} else {
				throw new SQLSyntaxException("invalid layout column '%s'", item);
			}
			
			Field field = table.find(name);
			if (field == null) {
				throw new SQLSyntaxException("cannot find '%s' column by layout", name);
			}
			// resolve column identity
			short colid = field.getColumnId();
			if(colid == pid.getColumnId()) {
				throw new SQLSyntaxException("'%s' is primary key, layout refuse!", name);
			}
			// resolve column sequence type
			byte type = Layout.parse(tag);
			if (!Layout.isASC(type) && !Layout.isDESC(type)) {
				throw new SQLSyntaxException("invalid layout tag '%s'", tag);
			}
			
			if (layout == null) {
				layout = new Layout(colid, type);
			} else {
				Layout sub = new Layout(colid, type);
				layout.setLast(sub);
			}
		}
		
		table.setLayout(layout);
	}
	
	/**
	 * resolve table prefix syntax
	 * @param prefix
	 * @param table
	 */
	private void splitTablePrefix(String prefix, Table table) {
		while(prefix.trim().length() > 0) {
			// resolve "hostmode=[share|exclusive]";
			Pattern pattern = Pattern.compile(SQLParser.TABLE_PREFIX_HOSTMODE1);
			Matcher matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = matcher.group(2);
				if ("share".equalsIgnoreCase(s)) {
					table.setMode(Table.SHARE);
				} else if ("exclusive".equalsIgnoreCase(s)) {
					table.setMode(Table.EXCLUSIVE);
				}
				continue;
			}
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_HOSTMODE2);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = "";
				if ("share".equalsIgnoreCase(s)) {
					table.setMode(Table.SHARE);
				} else if ("exclusive".equalsIgnoreCase(s)) {
					table.setMode(Table.EXCLUSIVE);
				}
				continue;
			}
			// resolve "cache=[yes|no]"
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_HOSTCACHE1);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = matcher.group(2);
				if("yes".equalsIgnoreCase(s)) {
					table.setCaching(true);
				} else if("no".equalsIgnoreCase(s)) {
					table.setCaching(false);
				}
				continue;
			}
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_HOSTCACHE2);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = "";
				if("yes".equalsIgnoreCase(s)) {
					table.setCaching(true);
				} else if("no".equalsIgnoreCase(s)) {
					table.setCaching(false);
				}
				continue;
			}
			// resolve "chunksize=[digit]M"
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_CHUNKSIZE1);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = matcher.group(2);
				int value = Integer.parseInt(s);
				table.setChunkSize(value * 1024 * 1024);
				continue;
			}
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_CHUNKSIZE2);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = "";
				int value = Integer.parseInt(s);
				table.setChunkSize(value * 1024 * 1024);
				continue;
			}
			// resolve "chunkcopy=[digit]"
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_CHUNKCOPY1);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = matcher.group(2);
				table.setCopy(Integer.parseInt(s));
				continue;
			}
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_CHUNKCOPY2);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = "";
				table.setCopy(Integer.parseInt(s));
				continue;
			}
			// resolve "primehost=[digit]"
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_PRIMEHOST1);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = matcher.group(2);
				table.setPrimes(Integer.parseInt(s));
				continue;
			}
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_PRIMEHOST2);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = "";
				table.setPrimes(Integer.parseInt(s));
				continue;
			}
			// resolve "clusters=[digit]"
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_CLUSTERS_NUM1);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				Clusters clusters = table.getClusters();
				clusters.setNumber(Integer.parseInt(s));
				prefix = matcher.group(2);
				continue;
			}
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_CLUSTERS_NUM2);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				Clusters clusters = table.getClusters();
				clusters.setNumber(Integer.parseInt(s));
				prefix = "";
				continue;
			}
			// resolve "clusters='[ip]' "
			pattern = Pattern.compile(SQLParser.TABLE_PREFIX_CLUSTERS_IP);
			matcher = pattern.matcher(prefix);
			if(matcher.matches()) {
				String s = matcher.group(1);
				prefix = matcher.group(2);
				Clusters clusters = table.getClusters();
				// split ip address
				String[] elems = s.split(",");
				for(String ip : elems) {
//					pattern = Pattern.compile(SQLParser.IPv4);
//					matcher = pattern.matcher(ip);
//					if (!matcher.matches()) {
//						throw new SQLSyntaxException("syntax error:%s", ip);
//					}
//					for (int i = 1; i < 5; i++) {
//						s = matcher.group(i);
//						if (Integer.parseInt(s) > 255) {
//							throw new SQLSyntaxException("ip address error:%s", ip);
//						}
//					}
					
					if (!isIPv4(ip)) {
						throw new SQLSyntaxException("invalid ip address:%s", ip);
					}
					clusters.add(ip);
				}
				continue;
			}
			throw new SQLSyntaxException("syntax error:%s", prefix);
		}
	}

	/**
	 * split "create table"
	 * @param sqlTable
	 * @param sqlIndex
	 * @return
	 */
	private Table splitTable(String sqlTable, String sqlIndex) {
		// split table
		Pattern pattern = Pattern.compile(SQLParser.CREATE_TABLE);
		Matcher matcher = pattern.matcher(sqlTable);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal sql's table syntax!");
		}
		// must 4
		int count = matcher.groupCount();
		if (count != 4) {
			this.throwable(sqlTable, 0);
		}
		// control information
		String prefix = matcher.group(1);
		// database name and table name
		String dbName = matcher.group(2);
		String tableName = matcher.group(3);
		// table column set
		String suffix = matcher.group(4);
		
		if(prefix.length() > 0) {
			char c = prefix.charAt(0);
			if(c != 0x20) {
				throw new SQLSyntaxException("syntax error:%s!", prefix);	
			}
		}
		if (dbName.length() > 64 || tableName.length() > 64) {
			throw new SQLSyntaxException("database and table name max is 64!");
		}
		Space space = new Space(dbName, tableName);
		Table table = new Table(space);
		// resolve prefix
		splitTablePrefix(prefix, table);

		int offset = matcher.start(4);
		String[] items = suffix.split(",");
		if (items == null || items.length == 0) {
			throw new SQLSyntaxException("invalid sql!");
		}

		short columnId = 1;
		for(String item : items) {
			Field field = splitTableColumn(item);
			if (field == null) {
				this.throwable(sqlIndex, offset + 2, "illegal table column!");
			}
			if (table.find(field.getName()) != null) {
				this.throwable(sqlIndex, offset + 2, "duplicate table column!");
			}
			field.setColumnId(columnId++);
			table.add(field);
			offset += item.length();
		}

		// split index
		pattern = Pattern.compile(SQLParser.CREATE_INDEX);
		matcher = pattern.matcher(sqlIndex);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal sql's index syntax!");
		}
		count = matcher.groupCount();
		if (count != 3) {
			throw new SQLSyntaxException("illegal sql index syntax!");
		}
		String db1 = matcher.group(1);		// database name
		String table1 = matcher.group(2);	// table name
		suffix = matcher.group(3);			// index text
		if (!dbName.equalsIgnoreCase(db1) || !tableName.equalsIgnoreCase(table1)) {
			throw new SQLSyntaxException("space name does not match!");
		}
		offset = matcher.start(3);
		// split index item
		items = suffix.split(",");
		if (items == null || items.length == 0) {
			this.throwable(sqlIndex, offset);
		}
		// parse index item
		for(String item : items) {
			boolean success = splitIndexColumn(table, item);
			if(!success) {
				this.throwable(sqlIndex, offset + 2, "illegal index column!");
			}
			offset += item.length();
		}

		// check parimary index and slave index
		int countPrimary = 0, countIndex = 0;
		for (Field field : table.values()) {
			if (field.isPrimeIndex()) countPrimary++;
			else if (field.isIndex()) countIndex++;
		}
		if (countPrimary != 1) {
			throw new SQLSyntaxException("illegal primary key");
		}
		if (countPrimary == 0 && countIndex == 0) {
			throw new SQLSyntaxException("undefine index key");
		}
		return table;
	}

	public Space splitDropTable(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.DROP_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal sql syntax!");
		}
		// database name
		int count = matcher.groupCount();
		if (count != 2) {
			throw new SQLSyntaxException("illegal sql syntax!");
		}
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new Space(db, table);
	}

	private byte splitPacking(String name) {
		if("GZIP".equalsIgnoreCase(name)) {
			return 1;
		}
		return -1;
	}

	/**
	 * split column
	 * @param text
	 * @param offset
	 * @return
	 */
	private Field splitTableColumn(String text) {
		Pattern pattern = Pattern.compile(SQLParser.TABLE_COLUMN);
		Matcher matcher = pattern.matcher(text);
		if (!matcher.matches()) return null;
		int count = matcher.groupCount();
		if (count != 3) return null;

		Field field = null;
		String name = matcher.group(1);
		String type = matcher.group(2);
		String suffix = matcher.group(3);
		// check data type
		if ("RAW".equalsIgnoreCase(type) || "BINARY".equalsIgnoreCase(type)) {
			field = new RawField();
		} else if("CHAR".equalsIgnoreCase(type)) {
			field = new CharField();
		} else if("NCHAR".equalsIgnoreCase(type)) {
			field = new NCharField();
		} else if("WCHAR".equalsIgnoreCase(type)) {
			field = new WCharField();
		} else if("SHORT".equalsIgnoreCase(type) || "SMALLINT".equalsIgnoreCase(type)) {
			field = new ShortField();
		} else if("INTEGER".equalsIgnoreCase(type) || "INT".equalsIgnoreCase(type)) {
			field = new IntegerField();
		} else if("LONG".equalsIgnoreCase(type) || "BIGINT".equalsIgnoreCase(type)) {
			field = new LongField();
		} else if("REAL".equalsIgnoreCase(type)) {
			field = new RealField();
		} else if("DOUBLE".equalsIgnoreCase(type)) {
			field = new DoubleField();
		} else if("DATE".equalsIgnoreCase(type)) {
			field = new DateField();
		} else if("TIME".equalsIgnoreCase(type)) {
			field = new TimeField();
		} else if ("TIMESTAMP".equalsIgnoreCase(type) || "DATETIME".equalsIgnoreCase(type)) {
			field = new TimeStampField();
		} else {
			return null;
		}
		field.setName(name);

		// check other parameter
		while (suffix != null && suffix.trim().length() > 0) {
			boolean match = false;
			for (int i = 0; i < TABLE_ALLCOLUMN.length; i++) {
				pattern = Pattern.compile(TABLE_ALLCOLUMN[i]);
				matcher = pattern.matcher(suffix);
				match = matcher.matches();
				if (!match) continue;
				count = matcher.groupCount();
				if (!(2 <= count && count <= 3)) return null; //error

				String item1 = matcher.group(1);
				String item2 = matcher.group(2);

				if(count == 2) {
					suffix = item2;
				} else 	if (count == 3) {
					suffix = matcher.group(3);
				}

				if ("NULL".equalsIgnoreCase(item1)) {
					field.setAllowNull(true);
				} else if ("LIKE".equalsIgnoreCase(item1)) {
					if (!Type.isWord(field.getType())) return null;
					((WordField) field).setLike(true);
				} else if ("CASE".equalsIgnoreCase(item1)) {
					if (!Type.isWord(field.getType())) return null;
					((WordField) field).setSentient(true);
				} else if ("NOT".equalsIgnoreCase(item1) && "NULL".equalsIgnoreCase(item2)) {
					field.setAllowNull(false);
				} else if ("NOT".equalsIgnoreCase(item1) && "LIKE".equalsIgnoreCase(item2)) {
					if (!Type.isWord(field.getType())) return null;
					((WordField) field).setLike(false);
				} else if ("NOT".equalsIgnoreCase(item1) && "CASE".equalsIgnoreCase(item2)) {
					if (!Type.isWord(field.getType())) return null;
					((WordField) field).setSentient(false);
				} else if ("PACKING".equalsIgnoreCase(item1)) {
					byte ptype = this.splitPacking(item2);
					if (ptype == -1 || !Type.isVariable(field.getType())) return null;
					((VariableField) field).setPacking(ptype);
				} else if ("DEFAULT".equalsIgnoreCase(item1)) {
					switch (field.getType()) {
					case Type.RAW:
						if(field.getClass() != RawField.class) {
							return null;
						}
						((RawField) field).setValue(item2.getBytes());
						break;
					case Type.CHAR:
						if (field.getClass() != CharField.class) {
							return null;
						}
						((CharField)field).setValue( item2.getBytes() );
						break;
					case Type.NCHAR:
						if (field.getClass() != NCharField.class) {
							return null;
						}
						// 使用指定的编码字节类型
						((NCharField) field).setValue(item2.getBytes());
						break;
					case Type.WCHAR:
						if (field.getClass() != WCharField.class) {
							return null;
						}
						((WCharField) field).setValue(item2.getBytes());
						break;
					case Type.SHORT:
						if (field.getClass() != ShortField.class) return null;
						((ShortField) field).setValue(Short.parseShort(item2));
						break;
					case Type.INTEGER:
						if(field.getClass() != IntegerField.class) return null;
						((IntegerField)field).setValue(Integer.parseInt(item2));
						break;
					case Type.LONG:
						if (field.getClass() != LongField.class) return null;
						((LongField)field).setValue(Long.parseLong(item2));
						break;
					case Type.REAL:
						break;
					case Type.DOUBLE:
						break;
					case Type.DATE:
						break;
					case Type.TIME:
						break;
					case Type.TIMESTAMP:
						break;
					default:
						return null;
					}
				}
				break;
			}
			if (!match) return null;
		}
		return field;
	}

	private boolean splitIndexColumn(Table table, final String sqlItem) {
		boolean match = false;
		for (int i = 0; i < INDEX_ALLCOLUMN.length; i++) {
			Pattern pattern = Pattern.compile(INDEX_ALLCOLUMN[i]);
			Matcher matcher = pattern.matcher(sqlItem);
			match = matcher.matches();
			if (!match) continue;

			int count = matcher.groupCount();
			String name = matcher.group(1);
			Field field = table.find(name);
			if (field == null) return false;

			if (count == 3) {
				String num = matcher.group(2);
				if (!Type.isVariable(field.getType())) {
					return false;
				}
				((VariableField) field).setIndexSize(Integer.parseInt(num));

				String key = matcher.group(3);
				if ("PRIMARY".equalsIgnoreCase(key)) {
					field.setIndexType(Type.PRIME_INDEX);
				} else {
					return false;
				}
			} else if (count == 2) {
				String value = matcher.group(2);
				if ("PRIMARY".equalsIgnoreCase(value)) {
					field.setIndexType(Type.PRIME_INDEX);
				} else {
					if (!Type.isVariable(field.getType())) {
						return false;
					}
					((VariableField) field).setIndexSize(Integer.parseInt(value));
				}
			} else {
				field.setIndexType(Type.SLAVE_INDEX);
			}
			break;
		}
		return match;
	}


	public Select splitSelect(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_SELECT_ALL);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitSelectWhere(set, table, sql);
		}
		// all select element
		String prefix = matcher.group(1);
		String dbName = matcher.group(2);
		String tableName = matcher.group(3);
		String where = matcher.group(4);
		String orderby = matcher.group(5);

		Select select = new Select(new Space(dbName, tableName));
		// split "select * from"
		this.splitSelectPrefix(table, select, prefix);
		// split "where " syntax
		this.splitWhere(set, table, select, where);
		// split "order by" syntax
		splitOrderBy(table , select, orderby);
		// return result
		return select;
	}

	private Select splitSelectWhere(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_SELECT_WHERE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			return splitSelectSingle(table, sql);
		}
		// all select element
		String prefix = matcher.group(1);
		String dbName = matcher.group(2);
		String tableName = matcher.group(3);
		String where = matcher.group(4);
		// set space name
		Select select = new Select(new Space(dbName, tableName) );
		// split "select * from"
		this.splitSelectPrefix(table, select, prefix);
		// split "where"
		this.splitWhere(set, table, select, where);
		// return result object
		return select;
	}

	private Select splitSelectSingle(Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_SELECT_SINGLE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("illegal select syntax");
		}
		// all select element
		String prefix = matcher.group(1);
		String dbName = matcher.group(2);
		String tableName = matcher.group(3);

		Select select = new Select(new Space(dbName, tableName));
		// split "select * from" scope
		this.splitSelectPrefix(table, select, prefix);
		// return select object
		return select;
	}

	public Delete splitDelete(SQLCharset charset, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_DELETE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("illegal delete syntax");
		}
		// all delete element
		String db_name = matcher.group(1);
		String table_name = matcher.group(2);
		String where_str = matcher.group(3);

		Delete delete = new Delete(new Space(db_name, table_name));
		// split where
		splitWhere(charset, table, delete, where_str);
		// return delete object
		return delete;
	}
	
	public Update splitUpdate(SQLCharset charset, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_UPDATE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			throw new SQLSyntaxException("illegal update syntax");
		}
		
		String db_name = matcher.group(1);
		String table_name = matcher.group(2);
		String set_str = matcher.group(3);
		String where_str = matcher.group(4);
		
		Update update = new Update(new Space(db_name, table_name));
		
		// split update set
		splitUpdateSet(charset, table, set_str, update);
		// split where
		splitWhere(charset, table, update, where_str);
		
		return update;
	}
	
	private void splitUpdateSet(SQLCharset charset, Table table, String sql, Update update) {
		while(sql.trim().length()>0) {
			Pattern pattern = Pattern.compile(SQLParser.SQL_UPDATE_SET1);
			Matcher matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String key = matcher.group(1);
				String value = matcher.group(2);
				sql = "";
				
				Column column = buildValue(charset, table, key, value);
				if(column == null) {
					throw new SQLSyntaxException("illegal syntax '%s'", sql);
				}
				update.add(column);
				continue;
			}
			
			pattern = Pattern.compile(SQLParser.SQL_UPDATE_SET2);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String key = matcher.group(1);
				String value = matcher.group(2);
				sql = "";
				Column column = buildValue(charset, table, key, value);
				if(column == null) {
					throw new SQLSyntaxException("illegal syntax '%s'", sql);
				}
				update.add(column);
				continue;
			}
			
			pattern = Pattern.compile(SQLParser.SQL_UPDATE_SET3);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String key = matcher.group(1);
				String value = matcher.group(2);
				sql = matcher.group(3);
				Column column = buildValue(charset, table, key, value);
				if(column == null) {
					throw new SQLSyntaxException("illegal syntax '%s'", sql);
				}
				update.add(column);
				continue;
			}
			
			pattern = Pattern.compile(SQLParser.SQL_UPDATE_SET4);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String key = matcher.group(1);
				String value = matcher.group(2);
				sql = matcher.group(3);
				Column column = buildValue(charset, table, key, value);
				if(column == null) {
					throw new SQLSyntaxException("illegal syntax '%s'", sql);
				}
				update.add(column);
				continue;
			}
			
			throw new SQLSyntaxException("illegal syntax '%s'", sql);
		}
	}

	public ADC splitADC(String sql, SQLCharset diffuse_set, Table diffuse_table) {
		String from = "";
		String to = "";
		String collect = "";

		Pattern pattern = Pattern.compile(SQLParser.ADC1);
		Matcher matcher = pattern.matcher(sql);
		if(matcher.matches()) {
			from = matcher.group(1);
			to = matcher.group(2);
			collect = matcher.group(3);
		} else {
			pattern = Pattern.compile(SQLParser.ADC2);
			matcher = pattern.matcher(sql);
			if(!matcher.matches()) {
				throw new SQLSyntaxException("invalid adc: " + sql);
			}
			from = matcher.group(1);
			to = matcher.group(2);
		}
		
		ADC adc = new ADC();
		this.splitDiffuse(from, adc, diffuse_set, diffuse_table);
		this.splitAggregate(to, adc);
		if (collect.length() > 0) {
			this.splitCollect(collect, adc);
		}

		return adc;
	}
	
	/**
	 * parse dc command
	 * @param sql
	 * @param diffuse_set
	 * @param diffuse_table
	 * @return
	 */
	public DC splitDC(String sql, SQLCharset diffuse_set, Table diffuse_table) {
		String from = "";
		String to = "";
		String collect = "";

		Pattern pattern = Pattern.compile(SQLParser.DC1);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			from = matcher.group(1);
			to = matcher.group(2);
			collect = matcher.group(3);
		} else {
			pattern = Pattern.compile(SQLParser.DC2);
			matcher = pattern.matcher(sql);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("invalid dc: " + sql);
			}
			from = matcher.group(1);
			to = matcher.group(2);
		}

		DC dc = new DC();
		this.splitDiffuse(from, dc, diffuse_set, diffuse_table);
		this.splitAggregate(to, dc);
		if (collect.length() > 0) {
			this.splitCollect(collect, dc);
		}
		
		return dc;
	}
	
	private void splitDiffuse(String sql, BasicComputing basic, SQLCharset set, Table table) {
		while (sql.trim().length() > 0) {
			// diffuse naming
			Pattern pattern = Pattern.compile(SQLParser.DC_NAMING);
			Matcher matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String s = matcher.group(1);
				sql = matcher.group(2);
				basic.setFromNaming(s);
				continue;
			}
			// diffuse site number
			pattern = Pattern.compile(SQLParser.DC_SITES);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String num = matcher.group(1);
				sql = matcher.group(2);
				basic.setFromSites(Integer.parseInt(num));
				continue;
			}
			// diffuse blocks
			pattern = Pattern.compile(SQLParser.DC_BLOCKS);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String column = matcher.group(1);
				String size = matcher.group(2);
				sql = matcher.group(3);
				basic.setFromBlocks(column, Integer.parseInt(size));
				continue;
			}
			// "select " command
			pattern = Pattern.compile(SQLParser.DC_QUERY);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String selstr = matcher.group(1);
				sql = matcher.group(2);
				Select select = splitSelect(set, table, selstr);
				basic.setFromSelect(select);
				continue;
			}
			// user parameters
			pattern = Pattern.compile(SQLParser.DC_VALUES);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String values = matcher.group(1);
				sql = matcher.group(2);
				List<DCValue> params = splitDCValues(values);
				basic.addFromValues(params);
				continue;
			}
			throw new SQLSyntaxException("illegal syntax '%s'", sql);
		}
	}
	
	private void splitAggregate(String sql, BasicComputing basic) {
		while (sql.trim().length() > 0) {
			Pattern pattern = Pattern.compile(SQLParser.DC_SITES);
			Matcher matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String num = matcher.group(1);
				sql = matcher.group(2);
				basic.setToSites(Integer.parseInt(num));
				continue;
			}
			pattern = Pattern.compile(SQLParser.DC_NAMING);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String s = matcher.group(1);
				sql = matcher.group(2);
				basic.setToNaming(s);
				continue;
			}
			pattern = Pattern.compile(SQLParser.DC_VALUES);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String values = matcher.group(1);
				sql = matcher.group(2);
				List<DCValue> params = this.splitDCValues(values);
				basic.addToValues(params);
				continue;
			}
			throw new SQLSyntaxException("illegal syntax '%s'", sql);
		}
	}
	
	private void splitCollect(String sql, BasicComputing basic) {
		while (sql.trim().length() > 0) {
			// space
			Pattern pattern = Pattern.compile(SQLParser.DC_SPACE);
			Matcher matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String db = matcher.group(1);
				String table = matcher.group(2);
				sql = matcher.group(3);
				basic.setCollectSpace(new Space(db, table));
				continue;
			}
			// naming
			pattern = Pattern.compile(SQLParser.DC_NAMING);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String s = matcher.group(1);
				sql = matcher.group(2);
				basic.setCollectNaming(s);
				continue;
			}
			// writeto
			pattern = Pattern.compile(SQLParser.DC_WRITETO);
			matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String s = matcher.group(1);
				sql = matcher.group(2);
				basic.setCollectWriteto(s);
				continue;
			}
			throw new SQLSyntaxException("illegal syntax '%s'", sql);
		}
	}
	
	private List<DCValue> splitDCValues(String sql) {
		ArrayList<DCValue> a = new ArrayList<DCValue>();
		while (sql.trim().length() > 0) {
			// remove "," symbol
			Pattern pattern = Pattern.compile(SQLParser.DC_VALUE_FILTE);
			Matcher matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				sql = matcher.group(1);
			}
			// split boolean
			pattern = Pattern.compile(SQLParser.DC_VALUE_BOOLEAN);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String name = matcher.group(1);
				String value = matcher.group(2);
				sql = matcher.group(3);
				DCValue param = new DCValue(name, "TRUE".equalsIgnoreCase(value));
				a.add(param);
				continue;
			}
			// split binary
			pattern = Pattern.compile(SQLParser.DC_VALUE_RAW);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String name = matcher.group(1);
				String value = matcher.group(2);
				sql = matcher.group(3);
				if (value.length() % 2 != 0) {
					throw new SQLSyntaxException("illegal binary value:%s", sql);
				}
				byte[] b = new byte[value.length() / 2];
				for (int i = 0, off = 0; i < b.length; i++, off += 2) {
					String w = value.substring(off, off + 2);
					b[i] = (byte)Integer.parseInt(w, 16);
				}
				DCValue param = new DCValue(name, b);
				a.add(param);
				continue;
			}
			// split double
			pattern = Pattern.compile(SQLParser.DC_VALUE_DOUBLE);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String name = matcher.group(1);
				String value = matcher.group(2);
				sql = matcher.group(3);
				DCValue param = new DCValue(name, Double.parseDouble(value));
				a.add(param);
				continue;
			}
			// split long
			pattern = Pattern.compile(SQLParser.DC_VALUE_LONG);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String name = matcher.group(1);
				String value = matcher.group(2);
				sql = matcher.group(3);
				DCValue param = new DCValue(name, DCValue.INT64, Long.parseLong(value));
				a.add(param);
				continue;
			}
			// split string
			pattern = Pattern.compile(SQLParser.DC_VALUE_CHARS);
			matcher = pattern.matcher(sql);
			if(matcher.matches()) {
				String name = matcher.group(1);
				String value = matcher.group(2);
				sql = matcher.group(3);
				// split timestamp
				pattern = Pattern.compile(SQLParser.DC_VALUE_TIMESTAMP);
				matcher = pattern.matcher(sql);
				if(matcher.matches()) {
					int year = Integer.parseInt(matcher.group(1));
					int month = Integer.parseInt(matcher.group(2));
					int day = Integer.parseInt(matcher.group(3));
					int hour = Integer.parseInt(matcher.group(4));
					int minute = Integer.parseInt(matcher.group(5));
					int second = Integer.parseInt(matcher.group(6));
					long timestamp = SimpleTimeStamp.format(year, month, day, hour, minute, second, 0);
					DCValue param = new DCValue(name, DCValue.TIMESTAMP, timestamp);
					a.add(param);
					continue;
				}
				// split date
				pattern = Pattern.compile(SQLParser.DC_VALUE_DATE);
				matcher = pattern.matcher(sql);
				if(matcher.matches()) {
					int year = Integer.parseInt(matcher.group(1));
					int month = Integer.parseInt(matcher.group(2));
					int day = Integer.parseInt(matcher.group(3));
					int date = SimpleDate.format(year, month, day);
					DCValue param = new DCValue(name, DCValue.DATE, date);
					a.add(param);
					continue;
				}
				// split time
				pattern = Pattern.compile(SQLParser.DC_VALUE_TIME);
				matcher = pattern.matcher(sql);
				if(matcher.matches()) {
					int hour = Integer.parseInt(matcher.group(1));
					int minute = Integer.parseInt(matcher.group(2));
					int second = Integer.parseInt(matcher.group(3));
					int time =SimpleTime.format(hour, minute, second, 0);
					DCValue param = new DCValue(name, DCValue.TIME, time);
					a.add(param);
					continue;
				}
				// this is string
				DCValue param = new DCValue(name, value);
				a.add(param);
				continue;
			}
			throw new SQLSyntaxException("invalid dc syntax:%s", sql);
		}
		return a;
	}
	
	private Column buildValue(SQLCharset charset, Table table, String name, String value) {
		Field field = table.find(name);
		if(field == null) return null;
		
		short columnId = field.getColumnId();
		Column column = null;
		switch(field.getType()) {
		case Type.RAW:
			column = new Raw(columnId, value.getBytes());
			break;
		case Type.CHAR: {
			SQLChar sqlchar = charset.getChar();
			byte[] b = sqlchar.encode(value.getBytes());
			column = new Char(columnId, b);
		}
			break;
		case Type.NCHAR: {
			SQLChar sc = charset.getNChar();
			byte[] b = sc.encode(value.getBytes());
			column = new NChar(columnId, b);
		}
			break;
		case Type.WCHAR: {
			SQLChar sc = charset.getWChar();
			byte[] b = sc.encode(value.getBytes());
			column = new WChar(columnId, b);
		}
			break;
		case Type.SHORT:
			column = new com.lexst.db.column.Short(columnId, Short.parseShort(value));
			break;
		case Type.INTEGER:
			column = new com.lexst.db.column.Integer(columnId, Integer.parseInt(value));
			break;
		case Type.LONG:
			column = new com.lexst.db.column.Long(columnId, Long.parseLong(value));
			break;
		case Type.REAL:
			column = new Real(columnId, Float.parseFloat(value));
			break;
		case Type.DOUBLE:
			column = new com.lexst.db.column.Double(columnId, Double.parseDouble(value));
			break;
		case Type.DATE: {
			int num = splitDate(value);
			if(num == -1) {
				throw new SQLSyntaxException("illegal column '%s'", value);
			}
			column = new com.lexst.db.column.Date(columnId, num);
		}
			break;
		case Type.TIME: {
			int num = splitTime(value);
			if(num == -1) {
				throw new SQLSyntaxException("illegal column '%s'", value);
			}
			column = new Time(columnId, num);
		}
			break;
		case Type.TIMESTAMP: {
			long num = splitTimeStamp(value);
			if (num == -1) {
				throw new SQLSyntaxException("illegal column '%s'", value);
			}
			column = new TimeStamp(columnId, num);
		}
			break;
		}

		return column;
	}

	private void splitSelectPrefix(Table table, Select select, String sql) {
		while (sql != null && sql.trim().length() > 0) {
			String suffix = splitSelectPrefixTop(table, select, sql);
			if (!sql.equals(suffix)) {
				sql = suffix; continue;
			}
			suffix = splitSelectPrefixRange(table, select, sql);
			if (!sql.equals(suffix)) {
				sql = suffix; continue;
			}
			suffix = splitSelectPrefixColumn(table, select, sql);
			if (suffix == null) break;
			throw new SQLSyntaxException("invalid select syntax '%s'", suffix);
		}
	}

	private String splitSelectPrefixTop(Table table, Select select, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_SELECT_PREFIX_TOP);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			return sql;
		}

		String top = matcher.group(1);
		String suffix = matcher.group(2);
		select.setRange(1, Integer.parseInt(top));
		return suffix;
	}

	private String splitSelectPrefixRange(Table table, Select select, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_SELECT_PREFIX_RANGE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return sql;
		}

		String begin = matcher.group(1);
		String end = matcher.group(2);
		String suffix = matcher.group(3);

		int num1 = Integer.parseInt(begin);
		int num2 = Integer.parseInt(end);
		if (num1 > num2) {
			throw new SQLSyntaxException("illegal range %s - %s", begin, end);
		}
		select.setRange(num1, num2);
		return suffix;
	}

	private String splitSelectPrefixColumn(Table table, Select select, String sql) {
		// find all column
		if ("*".equals(sql.trim())) {
			Set<Short> set = table.idSet();
			short[] all = new short[set.size()];
			int index = 0;
			for (short columnId : set) {
				all[index++] = columnId;
			}
			select.setSelectId(all);
			return null;
		}

		String[] cols = sql.split(",");
		ArrayList<Short> a = new ArrayList<Short>();
		for (String colname : cols) {
			Field field = table.find(colname.trim());
			if (field == null) {
				throw new SQLSyntaxException("unknown \'%s\' column", colname);
			}
			short columnId = field.getColumnId();
			a.add(columnId);
		}
		short[] all = new short[a.size()];
		for (int i = 0; i < all.length; i++) {
			all[i] = a.get(i).shortValue();
		}
		select.setSelectId(all);
		return null;
	}

	private void splitOrderBy(Table table, Select select, String sql) {
		String[] elems = sql.split(",");
		for (String sub : elems) {
			Pattern pattern = Pattern.compile(SQLParser.SQL_ORDERBY);
			Matcher matcher = pattern.matcher(sub);
			if (!matcher.matches()) {
				throw new SQLSyntaxException("illegal sql syntax \'%s\'", sub);
			}
			String colname = matcher.group(1);
			String type = matcher.group(2);

			Field field = table.find(colname);
			if (field == null) {
				throw new SQLSyntaxException("unknown column '%s'", colname);
			} else if (field.isNoneIndex()) {
				throw new SQLSyntaxException("\'%s\' not index", colname);
			}
			Order order = new Order(field.getColumnId(), ("ASC".equals(type) ? Layout.ASC : Layout.DESC));
			select.setOrder(order);
		}
	}

	private void splitWhere(SQLCharset set, Table table, Query query, String sqlWhere) {
		ArrayList<String> array = new ArrayList<String>();
		// split element
		this.splitWhereGroups(sqlWhere, array);
		// split group
		for (String sql : array) {
			byte outsideRelate = Condition.NONE;
			Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_ELEMENT_PREFIX);
			Matcher matcher = pattern.matcher(sql);
			if (matcher.matches()) {
				String symbol = matcher.group(1);
				outsideRelate = Condition.getRelated(symbol);
				sql = matcher.group(2);
			}

			ArrayList<Condition> list = new ArrayList<Condition>();
			String[] all = splitWhereElement(sql);
			for (int i = 0; i < all.length; i++) {
				Condition condi = splitWhereElementChar1(set, table, all[i]);
				if (condi == null) {
					condi = splitWhereElementChar2(set, table, all[i]);
				}
				if (condi == null) {
					condi = splitWhereElementNum1(set, table, all[i]);
				}
				if (condi == null) {
					condi = splitWhereElementNum2(set, table, all[i]);
				}
				if (condi == null) {
					throw new SQLSyntaxException("illegal sql syntax \'%s\'", sql);
				}
				list.add(condi);
			}
			if(list.isEmpty()) {
				throw new SQLSyntaxException("illegal sql '%s'", sql);
			}
			Condition element = list.get(0);
			element.setOutsideRelate(outsideRelate);

			for (int i = 1; i < list.size(); i++) {
				Condition next = list.get(i);
				element.addFriend(next);
			}
			query.setCondition(element);
		}
	}

	private Condition splitWhereElementChar1(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_CHAR1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String related = matcher.group(1);
		String colname = matcher.group(2);
		String compare = matcher.group(3);
		String value = matcher.group(4);

		byte relatedType = Condition.getRelated(related);
		byte compareType = Condition.getCompare(compare);

		IndexColumn col = null;
		if (compareType == Condition.LIKE) {
			col = splitWhereLike(set, table, colname, value);
		} else {
			col = buildIndex(set, table, colname, value);
		}
		if (col == null) {
			throw new SQLSyntaxException("illegal sql '%s'", sql);
		}
		// check value type
		Condition condi = new Condition(relatedType, colname, compareType, col);
		return condi;
	}

	private Condition splitWhereElementChar2(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_CHAR2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String colname = matcher.group(1);
		String compare = matcher.group(2);
		String value = matcher.group(3);

		byte compareType = Condition.getCompare(compare);

		IndexColumn col = null;
		if (compareType == Condition.LIKE) {
			col = splitWhereLike(set, table, colname, value);
		} else {
			col = buildIndex(set, table, colname, value);
		}
		if (col == null) {
			throw new SQLSyntaxException("illegal sql '%s'", sql);
		}
		// check value type
		Condition condi = new Condition(colname, compareType, col);
		return condi;
	}

	private Condition splitWhereElementNum1(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_NUM1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String related = matcher.group(1);
		String colname = matcher.group(2);
		String compare = matcher.group(3);
		String value = matcher.group(4);

		byte relatedType = Condition.getRelated(related);
		byte compareType = Condition.getCompare(compare);
		IndexColumn col = buildIndex(set, table, colname, value);

		// check value type
		Condition condi = new Condition(relatedType, colname, compareType, col);
		return condi;
	}

	private Condition splitWhereElementNum2(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_NUM2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String colname = matcher.group(1);
		String compare = matcher.group(2);
		String value = matcher.group(3);

		byte compareType = Condition.getCompare(compare);
		IndexColumn col = buildIndex(set, table, colname, value);
		// check value type
		Condition condi = new Condition(colname, compareType, col);
		return condi;
	}
	
	private int splitDate(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_DATE_STYLE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return -1;
		
		String year = matcher.group(1);
		String month = matcher.group(2);
		String day = matcher.group(3);
	
		return SimpleDate.format(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day));
	}

	private int splitTime(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_TIME_STYLE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			return splitTime2(sql);
		}
		
		String hour = matcher.group(1);
		String minute = matcher.group(2);
		String second = matcher.group(3);
		String millisecond = matcher.group(4);

		return SimpleTime.format(Integer.parseInt(hour),
				Integer.parseInt(minute), Integer.parseInt(second),
				Integer.parseInt(millisecond));
	}

	private int splitTime2(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_TIME_STYLE2);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return -1;
		
		String hour = matcher.group(1);
		String minute = matcher.group(2);
		String second = matcher.group(3);
		
		return SimpleTime.format(Integer.parseInt(hour),
				Integer.parseInt(minute), Integer.parseInt(second), 0);
	}

	private long splitTimeStamp(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_TIMESTAMP_STYLE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			return -1;
		}
		
		String year = matcher.group(1);
		String month = matcher.group(2);
		String day = matcher.group(3);
		String hour = matcher.group(4);
		String minute = matcher.group(5);
		String second = matcher.group(6);
		String millisecond = matcher.group(7);
		
		return SimpleTimeStamp.format(Integer.parseInt(year),
				Integer.parseInt(month), Integer.parseInt(day),
				Integer.parseInt(hour), Integer.parseInt(minute),
				Integer.parseInt(second), Integer.parseInt(millisecond));
	}

	/**
	 * @param sql
	 * @return
	 */
	private String[] splitWhereLikeElement(String sql) {
		char[] s = {'%', '_'};
		int left = 0;
		for (int i = 0; i < s.length; i++) {
			if (sql.charAt(0) != s[i]) continue;
			for (; left < sql.length(); left++) {
				if (sql.charAt(left) != s[i]) break;
			}
			break;
		}

		int right = sql.length();
		for (int i = 0; i < s.length; i++) {
			if (sql.charAt(right - 1) != s[i]) continue;
			for (; right > 0; right--) {
				if (sql.charAt(right - 1) != s[i]) break;
			}
			break;
		}

		String s1 = sql.substring(0, left);
		String value = sql.substring(left, right);
		String s3 = sql.substring(right);
		return new String[] { s1, value, s3 };
	}

	private IndexColumn splitWhereLike(SQLCharset set, Table table, String colname, String value) {
		Field field = table.find(colname);
		if(field == null) {
			throw new SQLSyntaxException("cannot found \'%s\'", colname);
		}
		short columnId = field.getColumnId();

		// split like
		String[] s = splitWhereLikeElement(value);

		short left = 0, right = 0;
		if (s[0].isEmpty()) left = 0;
		else if("%".equals(s[0])) left = -1;
		else if(s[0].startsWith("_")) left = (short)s[0].length();
		else {
			throw new SQLSyntaxException("illegal sql %s", value);
		}

		if (s[2].isEmpty()) right = 0;
		else if("%".equals(s[2])) right = -1;
		else if (s[2].startsWith("_")) right = (short)s[2].length();
		else {
			throw new SQLSyntaxException("illegal sql %s", value);
		}

		String text = s[1];
		IndexColumn index = null;
		if(field.isChar()) {
			SQLChar lang = set.getChar();
			byte[] b = lang.encode(text);
			CharField charField = (CharField) field;
			if (charField.isSentient()) {
				b = lang.encode(text.toLowerCase());
			}
			RChar rchar = new RChar(columnId, left, right, b);
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, rchar);
		} else if(field.isNChar()) {
			SQLChar lang = set.getNChar();
			byte[] b = lang.encode(text);
			NCharField charField = (NCharField)field;
			if(charField.isSentient()) {
				b = lang.encode(text.toLowerCase());
			}
			RNChar rnchar = new RNChar(columnId, left, right, b);
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, rnchar);
		} else if(field.isWChar()) {
			SQLChar lang = set.getWChar();
			byte[] b = lang.encode(text);
			WCharField charField = (WCharField)field;
			if(charField.isSentient()) {
				b = lang.encode(text.toLowerCase());
			}
			RWChar rwchar = new RWChar(columnId, left, right, b);
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, rwchar);
		}
		return index;
	}

	private IndexColumn buildIndex(SQLCharset set, Table table, String colname, String value) {
		Field field = table.find(colname);
		if(field == null) {
			throw new SQLSyntaxException("cannot found \'%s\'", colname);
		}
		short columnId = field.getColumnId();
		IndexColumn index = null;
		switch(field.getType()) {
		case Type.RAW: {
			byte[] b = value.getBytes();
			Raw raw = new Raw(columnId, b);
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, raw);
		}
			break;
		case Type.CHAR: {
			SQLChar lang = set.getChar();
			byte[] b = lang.encode(value);
			Char chars = new Char(columnId, b);
			CharField charField = (CharField) field;
			if (charField.isSentient()) {
				b = lang.encode(value.toLowerCase());
			}
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, chars);
		}
			break;
		case Type.NCHAR: {
			SQLChar lang = set.getNChar();
			byte[] b = lang.encode(value);
			NChar nchar = new NChar(columnId, b);
			NCharField ncharField = (NCharField)field;
			if(ncharField.isSentient()) {
				b = lang.encode(value.toLowerCase());
			}
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, nchar);
		}
			break;
		case Type.WCHAR:{
			SQLChar lang = set.getChar();
			byte[] b = lang.encode(value);
			WChar w = new WChar(columnId, b);
			WCharField wcharField = (WCharField)field;
			if(wcharField.isSentient()) {
				b = lang.encode(value.toLowerCase());
			}
			long hash = Sign.sign(b, 0, b.length);
			index = new LongIndex(hash, w);
		}
			break;
		case Type.SHORT: {
			short num = Short.parseShort(value);
			com.lexst.db.column.Short small = new com.lexst.db.column.Short(columnId, num);
			index = new ShortIndex(num, small);
		}
			break;
		case Type.INTEGER: {
			int num = Integer.parseInt(value);
			com.lexst.db.column.Integer integer = new com.lexst.db.column.Integer(columnId, num);
			index = new IntegerIndex(num, integer);
		}
			break;
		case Type.LONG: {
			long num = Long.parseLong(value);
			com.lexst.db.column.Long big = new com.lexst.db.column.Long(columnId, num);
			index = new LongIndex(num, big);
		}
			break;
		case Type.REAL: {
			float num = Float.parseFloat(value);
			Real real = new Real(columnId, num);
			index = new RealIndex(num, real);
		}
			break;
		case Type.DOUBLE:{
			double num = Double.parseDouble(value);
			com.lexst.db.column.Double du = new com.lexst.db.column.Double(columnId, num);
			index = new DoubleIndex(num, du);
		}
			break;
		case Type.DATE: {
			int num = this.splitDate(value);
			com.lexst.db.column.Date date = new com.lexst.db.column.Date(columnId, num);
			index = new IntegerIndex(num, date);
		}
			break;
		case Type.TIME: {
			int num = this.splitTime(value);
			Time time = new Time(columnId, num);
			index = new IntegerIndex(num, time);
		}
			break;
		case Type.TIMESTAMP:
			long num = this.splitTimeStamp(value);
			TimeStamp stamp = new TimeStamp(columnId, num);
			index = new LongIndex(num, stamp);
			break;
		}
		return index;
	}

	private void splitWhereGroups(String sql, List<String> array) {
		String[] all = splitWhereInclude(sql);
		if (all == null) {
			array.add(sql);
		} else {
			for (int i = 0; i < all.length; i++) {
				String s = all[i];
				if (i + 1 < all.length) {
					array.add(s);
				} else { // last sql where
					splitWhereGroups(s, array);
				}
			}
		}

		ArrayList<String> a = new ArrayList<String>();
		String suffix = null;
		for (String text : array) {
			if (suffix != null) {
				text = String.format("%s %s", suffix, text);
			}
			Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_SUFFIX);
			Matcher matcher = pattern.matcher(text);
			if (!matcher.matches()) {
				a.add(text);
				suffix = null;
			} else {
				String prefix = matcher.group(1);
				a.add(prefix);
				suffix = matcher.group(2);
			}
		}
		if(suffix != null) {
			throw new SQLSyntaxException("invalid sql syntax");
		}
		array.clear();
		array.addAll(a);
	}

	private String[] splitWhereElement(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_WHERE_ELEMENT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return new String[] { sql };
		}
		String prefix = matcher.group(1);
		String compare = matcher.group(2);
		String suffix = matcher.group(3);

		String elem = String.format("%s %s", compare, suffix);
		String[] sqls = splitWhereElement(prefix);
		ArrayList<String> a = new ArrayList<String>();
		for (int i = 0; i < sqls.length; i++) {
			a.add(sqls[i]);
		}
		a.add(elem);
		String[] all = new String[a.size()];
		return a.toArray(all);
	}

	private String[] splitWhereInclude(String sql) {
		int begin = sql.indexOf("(");
		if (begin == -1) {
			return null;
		}
		int end = sql.indexOf(")", begin + 1);
		if (end == -1) {
			throw new SQLSyntaxException("invalid sql syntax");
		}
		String prefix = sql.substring(0, begin);
		String middle = sql.substring(begin + 1, end);
		String suffix = sql.substring(end + 1);

		ArrayList<String> a = new ArrayList<String>();
		if (prefix.trim().length() > 0) a.add(prefix);
		if (middle.trim().length() > 0) a.add(middle);
		if (suffix.trim().length() > 0) a.add(suffix);

		String[] all = new String[a.size()];
		return a.toArray(all);
	}

	public String splitCharSet(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SHOW_CHARSET);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitCharSet2( sql);
		}
		// find database
		String db = matcher.group(1);
		return db;
	}

	private String splitCharSet2( String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SHOW_CHARSET2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("illegal syntax!");
		}
		return null;
	}

	private void splitInsertItem(SQLCharset set, Table table, Insert insert,
			String fields, String values) {

		String[] all_fields = fields.split(",");
		String[] all_values = splitValues(values);
		if(all_fields.length != all_values.length) {
			throw new SQLSyntaxException("not match column!");
		}

		for (int i = 0; i < all_fields.length; i++) {
			String name = all_fields[i].trim();
			String value = all_values[i].trim();
			Column column = splitInsertValue(name, value, set, table);
			// save column
			insert.add(column);
		}
		// 不足的补齐
		Row row = insert.getRow();
		fill(row, table);
	}
	
	private String[] splitValues(String text) {
		ArrayList<String> a = new ArrayList<String>(10);
		while(!text.trim().isEmpty()) {
			String s = text.trim();
			if (s.charAt(0) == '\'') {
				int end = s.indexOf('\'', 1);
				if(end == -1) {
					throw new SQLSyntaxException("invalid sql: " + text);
				}
				String prefix = s.substring(0, end + 1);
				a.add(prefix);

				text = s.substring(end + 1).trim();
				if(text.isEmpty()) break;

				if (text.charAt(0) != ',') {
					throw new SQLSyntaxException("invalid sql: " + text);
				}
				text = text.substring(1);
			} else {
				int end = s.indexOf(',');
				if (end == -1) {
					a.add(s); break;
				} else {
					text = s.substring(end + 1);
					s = s.substring(0, end).trim();
					a.add(s);
				}
			}
		}
		// get string array
		String[] all = new String[a.size()];
		return a.toArray(all);
	}

	public Insert splitInsert(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_INSERT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
		}
		// data space
		String db = matcher.group(1);
		String tab = matcher.group(2);
		Space space = new Space(db, tab);
		
		if(!table.getSpace().equals(space)) {
			throw new SQLSyntaxException("not match table %s | %s", space, table.getSpace());
		}
		String fields = matcher.group(3);
		String values = matcher.group(4);
		Insert insert = new Insert(table);

		this.splitInsertItem(set, table, insert, fields, values);
		return insert;
	}
	
	private Column splitInsertValue(String name, String value, SQLCharset set,Table table) {
		String char_tag = "^\\s*\\'(.+)\\'\\s*$";
		Pattern pattern = Pattern.compile(char_tag);
		Matcher matcher = pattern.matcher(value);
		boolean strtag = matcher.matches(); // string tag
		if(strtag) {
			value = matcher.group(1);
		}

		Field field = table.find(name);
		if(field == null) {
			throw new SQLSyntaxException("illegal column \'%s\'", name);
		}

		short columnId = field.getColumnId();
		Column column = null;
		if (strtag) {
			switch (field.getType()) {
			case Type.RAW:
				column = new Raw(columnId, value.getBytes());
				break;
			case Type.CHAR: {
				SQLChar sqlchar = set.getChar();
				byte[] b = sqlchar.encode(value.getBytes());
				column = new Char(columnId, b);
			}
			break;
			case Type.NCHAR: {
				SQLChar sqlchar = set.getNChar();
				byte[] b = sqlchar.encode(value.getBytes());
				column = new NChar(columnId, b);
			}
			break;
			case Type.WCHAR: {
				SQLChar sqlchar = set.getWChar();
				byte[] b = sqlchar.encode(value.getBytes());
				column = new WChar(columnId, b);
			}
			break;
			case Type.DATE: {					
				int num = splitDate(value);
				if(num == -1) {
					throw new SQLSyntaxException("illegal column '%s'", value);
				}
				column = new com.lexst.db.column.Date(columnId, num);
			}
			break;
			case Type.TIME: {
				int num = splitTime(value);
				if(num == -1) {
					throw new SQLSyntaxException("illegal column '%s'", value);
				}
				column = new Time(columnId, num);
			}
			break;
			case Type.TIMESTAMP: {
				long num = splitTimeStamp(value);
				if (num == -1) {
					throw new SQLSyntaxException("illegal column '%s'", value);
				}
				column = new TimeStamp(columnId, num);
			}
			break;
			}
		} else {
			switch (field.getType()) {
			case Type.SHORT:
				column = new com.lexst.db.column.Short(columnId, Short.parseShort(value));
				break;
			case Type.INTEGER:
				column = new com.lexst.db.column.Integer(columnId, Integer.parseInt(value));
				break;
			case Type.LONG:
				column = new com.lexst.db.column.Long(columnId, Long.parseLong(value));
				break;
			case Type.REAL:
				column = new Real(columnId, Float.parseFloat(value));
				break;
			case Type.DOUBLE:
				column = new com.lexst.db.column.Double(columnId, Double.parseDouble(value));
				break;
			}
		}
		if (column == null) {
			throw new SQLSyntaxException("invalid column \'%s\' and %s ", name, value);
		}

		return column;
	}
	
	/**
	 * fill left column
	 * @param row
	 * @param table
	 */
	private void fill(Row row, Table table) {
		for (Field field : table.values()) {
			short columnId = field.getColumnId();
			Column column = row.get(columnId);
			if (column != null) continue;
			// get default column
			column = field.getDefault(columnId);
			if (column == null) {
				throw new SQLSyntaxException("%s cannot support default", field.getName());
			}
			row.add(column);
		}
	}
	
	private void splitInjectItem(SQLCharset set, Table table, Inject inject,
			String fields, String values) {
		
		String s1 = "^\\s*\\((.+?)\\)\\s*$";
		String s2 = "^\\s*\\((.+?)\\)\\s*(.+)\\s*$";
		
		List<String> array = new ArrayList<String>();
		while (values.trim().length() > 0) {
			Pattern pattern = Pattern.compile(s2);
			Matcher matcher = pattern.matcher(values);
			if (matcher.matches()) {
				String block = matcher.group(1);
				values = matcher.group(2);
				array.add(block);
				continue;
			}

			pattern = Pattern.compile(s1);
			matcher = pattern.matcher(values);
			if (matcher.matches()) {
				String block = matcher.group(1);
				values = "";
				array.add(block);
				continue;
			}

			throw new SQLSyntaxException("invalid syntax: %s", values);
		}
		
		String[] all_fields = fields.split(",");
		
		for(String block : array) {
			Row row = new Row();
			String[] all_values = splitValues(block);
			if(all_fields.length != all_values.length) {
				throw new SQLSyntaxException("not match column: %s", block);
			}

			for (int i = 0; i < all_fields.length; i++) {
				String colname = all_fields[i].trim();
				String value = all_values[i].trim();
				Column column = splitInsertValue(colname, value, set, table);
				// save column
				row.add(column);
			}
						
			fill(row, table);
			// save a row
			inject.add(row);
		}
		
	}

	public Inject splitInject(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SQL_INJECT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return null;
			// throw new SQLSyntaxException("invalid sql %s", sql);
		}
		
		String db = matcher.group(1);
		String tab = matcher.group(2);
		Space space = new Space(db, tab);
		
		if(!table.getSpace().equals(space)) {
			throw new SQLSyntaxException("not match table %s | %s", space, table.getSpace());
		}
		
		String fields = matcher.group(3);
		String values = matcher.group(4);

		Inject inject = new Inject(table);
		splitInjectItem(set, table, inject, fields, values);
		return inject;
	}
	
	public Object[] splitSetChunkSize(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SET_CHUNKSIZE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return null;
		String db = matcher.group(1);
		String table = matcher.group(2);
		String digit = matcher.group(3);
		Space space = new Space(db, table);
		int size = Integer.parseInt(digit) * 1024 * 1024;
		return new Object[] { space, new Integer(size) };
	}
	
	/**
	 * @param sql
	 * @return
	 */
	public Object[] splitSetOptimizeTime(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SET_OPTITIME);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;
		
		String db = matcher.group(1);
		String table = matcher.group(2);
		String option = matcher.group(3);
		String time = matcher.group(4);
		
		Space space = new Space(db, table);
		int type = 0;
		long trig = 0L;
		
		if ("HOURLY".equalsIgnoreCase(option)) {
			type = TimeSwitch.HOURLY;
			pattern = Pattern.compile(SQLParser.SQL_HOURLY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) return null;
			
			int minute = Integer.parseInt(matcher.group(1));
			int second = Integer.parseInt(matcher.group(2));
			if (minute > 23 || second > 59) return null;
			trig = SimpleTimeStamp.format(0, 0, 0, 0, minute, second, 0);
		} else if ("DAILY".equalsIgnoreCase(option)) {
			type = TimeSwitch.DAILY;
			pattern = Pattern.compile(SQLParser.SQL_DAILY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) return null;
			
			int hour = Integer.parseInt(matcher.group(1));
			int minute = Integer.parseInt(matcher.group(2));
			int second = Integer.parseInt(matcher.group(3));
			if (hour > 23 || minute > 59 || second > 59) return null;
			trig = SimpleTimeStamp.format(0, 0, 0, hour, minute, second, 0);
		} else if ("WEEKLY".equalsIgnoreCase(option)) {
			type = TimeSwitch.WEEKLY;
			pattern = Pattern.compile(SQLParser.SQL_WEEKLY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) return null;
			
			int day = Integer.parseInt(matcher.group(1));
			int hour = Integer.parseInt(matcher.group(2));
			int minute = Integer.parseInt(matcher.group(3));
			int second = Integer.parseInt(matcher.group(4));
			if (day < 1 || day > 7 || hour > 23 || minute > 59 || second > 59) return null;
			trig = SimpleTimeStamp.format(0, 0, day, hour, minute, second, 0);
		} else if ("MONTHLY".equalsIgnoreCase(option)) {
			type = TimeSwitch.MONTHLY;
			pattern = Pattern.compile(SQLParser.SQL_MONTHLY);
			matcher = pattern.matcher(time);
			if(!matcher.matches()) return null;
			
			int day = Integer.parseInt(matcher.group(1));
			int hour = Integer.parseInt(matcher.group(2));
			int minute = Integer.parseInt(matcher.group(3));
			int second = Integer.parseInt(matcher.group(4));
			if (day > 31 || hour > 23 || minute > 59 || second > 59) return null;
			trig = SimpleTimeStamp.format(0, 0, day, hour, minute, second, 0);
		} else {
			return null;
		}
		
		return new Object[] { space, new Integer(type), new Long(trig) };
	}

	private List<String> splitIP(String sql) {
		String[] all = sql.split(",");
		ArrayList<String> array = new ArrayList<String>();
		for(int i = 0; i < all.length; i++) {
//			Pattern pattern = Pattern.compile(SQLParser.IPv4);
//			Matcher matcher = pattern.matcher(all[i]);
//			if(!matcher.matches()) {
//				throw new SQLSyntaxException("invalid host address %s", all[i]);
//			}

			if (!isIPv4(all[i])) {
				throw new SQLSyntaxException("invalid ip address: %s", all[i]);
			}
			array.add(all[i]);
		}
		return array;
	}

	private SpaceHost splitLoadOptimize2(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.OPTIMIZE2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String db = matcher.group(1);
		String table = matcher.group(2);
		SpaceHost sh = new SpaceHost(db, table);
		String left = matcher.group(3);
		List<String> hosts = splitIP(left);
		if (hosts != null) {
			sh.addAllIP(hosts);
		}
		return sh;
	}
	
	public SpaceHost splitLoadOptimize(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.OPTIMIZE1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitLoadOptimize2(sql);
		}
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new SpaceHost(db, table);
	}
	
	public SpaceHost splitLoadIndex(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.LOAD_INDEX1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitLoadIndex2(sql);
		}
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new SpaceHost(db, table);
	}
	
	private SpaceHost splitLoadIndex2(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.LOAD_INDEX2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String db = matcher.group(1);
		String table = matcher.group(2);
		SpaceHost sh = new SpaceHost(db, table);
		String left = matcher.group(3);
		List<String> hosts = splitIP(left);
		if (hosts != null) {
			sh.addAllIP(hosts);
		}
		return sh;
	}
	
	public SpaceHost splitStopIndex(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.STOP_INDEX1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitStopIndex2(sql);
		}
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new SpaceHost(db ,table);
	}
	
	private SpaceHost splitStopIndex2(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.STOP_INDEX2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String db = matcher.group(1);
		String table = matcher.group(2);
		SpaceHost sh = new SpaceHost(db, table);
		
		String left = matcher.group(3);
		List<String> hosts = splitIP(left);
		if(hosts != null) {
			sh.addAllIP(hosts);
		}
		return sh;
	}
	
	public SpaceHost splitLoadChunk(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.LOAD_CHUNK1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitLoadChunk2(sql);
		}
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new SpaceHost(db, table);
	}
	
	private SpaceHost splitLoadChunk2(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.LOAD_CHUNK2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String db = matcher.group(1);
		String table = matcher.group(2);
		SpaceHost sh = new SpaceHost(db, table);
		
		String left = matcher.group(3);
		List<String> hosts = splitIP(left);
		if(hosts != null) {
			sh.addAllIP(hosts);
		}
		return sh;
	}
	
	public SpaceHost splitStopChunk(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.STOP_CHUNK1);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitStopChunk2(sql);
		}
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new SpaceHost(db, table);
	}
	
	private SpaceHost splitStopChunk2(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.STOP_CHUNK2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String db = matcher.group(1);
		String table = matcher.group(2);
		SpaceHost sh = new SpaceHost(db, table);
		
		String left = matcher.group(3);
		List<String> hosts = splitIP(left);
		if(hosts != null) {
			sh.addAllIP(hosts);
		}
		return sh;
	}
	
	public NamingHost splitBuildTask(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.BUILD_TASK);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			return splitBuildTask2(sql);
		}
		String naming = matcher.group(1);
		return new NamingHost(naming);
	}
	
	private NamingHost splitBuildTask2(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.BUILD_TASK2);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String naming = matcher.group(1);
		NamingHost sh = new NamingHost(naming);
		
		String left = matcher.group(2);
		List<String> hosts = splitIP(left);
		if(hosts != null) {
			sh.addAllIP(hosts);
		}
		return sh;
	}
	
	public String splitShowSchema(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SHOW_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String schema = matcher.group(1);
		return schema;
	}
	
	public Space splitShowTable(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SHOW_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;

		String schema = matcher.group(1);
		String table = matcher.group(2);
		return new Space(schema, table);
	}
	
	private int resolveSite(String tag) {
		if ("ALL".equalsIgnoreCase(tag)) {
			return 0;
		} else if ("HOME".equalsIgnoreCase(tag)) {
			return Site.HOME_SITE;
		} else if ("LOG".equalsIgnoreCase(tag)) {
			return Site.LOG_SITE;
		} else if ("DATA".equalsIgnoreCase(tag)) {
			return Site.DATA_SITE;
		} else if ("WORK".equalsIgnoreCase(tag)) {
			return Site.WORK_SITE;
		} else if ("BUILD".equalsIgnoreCase(tag)) {
			return Site.BUILD_SITE;
		} else if ("CALL".equalsIgnoreCase(tag)) {
			return Site.CALL_SITE;
		}
		return -1;
	}

	/**
	 * split "show [home|log|data|..] site from [ip]"
	 * return value: 1. site id; 2. ip address (string)
	 * @param sql
	 * @return
	 */
	public Object[] splitShowSite(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SHOW_SITE1);
		Matcher matcher = pattern.matcher(sql);
		if (matcher.matches()) {
			String tag = matcher.group(1);
			int site = this.resolveSite(tag);
			return new Object[] { new Integer(site) };
		}

		pattern = Pattern.compile(SQLParser.SHOW_SITE2);
		matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid sql:" + sql);
		}
		String tag = matcher.group(1);
		String from = matcher.group(2);
		int site = this.resolveSite(tag);
		if (!this.isIPv4(from)) {
			throw new SQLSyntaxException("invalid ipv4 address:" + from);
		}

		return new Object[] { new Integer(site), from };
	}
	
	public String splitCollectPath(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.SET_COLLECT_PATH);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid sql:" + sql);
		}
		return matcher.group(1).trim();
	}
	
	public String splitCollectTask(String sql) {
		Pattern pattern = Pattern.compile(SQLParser.TEST_COLLECT_TASK);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) {
			throw new SQLSyntaxException("invalid sql:" + sql);
		}
		return matcher.group(1).trim();
	}
	
//	public static void main(String[] args) {
//		String sql = " set collect path    /cloud/sqlconsole/collect/ ";
//		SQLParser parser = new SQLParser();
//		String s = parser.splitCollectPath(sql);
//		System.out.printf("path is:[%s]\n", s);
//		
//		SQLChecker checker = new SQLChecker();
//		System.out.printf("result is: %b\n", checker.isCollectPath(sql));
//	}
	
//	public static void main(String[] args) {
//		SQLChecker checker = new SQLChecker();
//		String sql = "inject into video.system (word, id, weight) values ('char', 1, 1) ('nchar', 2, 1) ('wchar', 3, 1)";
//		Space space = checker.getInjectSpace(sql);
//		System.out.println(space);
//		
//		Table table = new Table(space);
//		short id = 1;
//		table.add(new com.lexst.db.field.CharField(id++, "word"));
//		table.add(new com.lexst.db.field.IntegerField(id++, "id", 0));
//		table.add(new com.lexst.db.field.IntegerField(id++, "weight", 0));
//		
//		SQLCharset set = new SQLCharset();
//		set.setChar(new UTF8());
//		set.setNChar(new UTF16());
//		set.setWChar(new UTF32());
//		
//		checker.isInject(set, table, sql);
//	}
	
//	public static void main(String[] args) {
//		String sql = "create user steven password=linux-system@pentium.com";
//		SQLParser parser = new SQLParser();
//		User user = parser.splitCreateUser(sql);
//		System.out.printf("create username:%s, password:%s\n", user.getHexUsername(), user.getHexPassword());
//		
//		sql = "alter user pentium password = linux-system@126.com.cn";
//		user = parser.splitAlterUser(sql);
//		System.out.printf("alter username:%s, password:%s\n", user.getHexUsername(), user.getHexPassword());
//		
//		sql = "drop sha1 user 51229ED0CFA4C47ADB2941E2867303A27808C09C";
//		
//		SQLChecker checker = new SQLChecker();
//		boolean f = checker.isDropSHA1User(sql);
//		user = parser.splitDropSHA1User(sql);
//		System.out.printf("drop username:%s, password:%s, result:%b\n",
//				user.getHexUsername(), user.getHexPassword(), f);
//	}
	
//	public static void main(String[] args) {
//		String sql = "show data site from 12.9.28.28";
//		SQLParser parser = new SQLParser();
//		Object[] params = parser.splitShowSite(sql);
//	}
	
//	public static void main(String[] args) {
//		String sql = " drop schema ";
//		SQLParser parser = new SQLParser();
//		boolean b = parser.isDropSchemaOption(sql);
//		System.out.printf("result is : %b\n", b);
//		
//		sql = "grant pentium, unix on schema video to lext";
//		Permit permit = parser.splitGrantSchema(sql);
//		
////		sql = "revoke penti, unix, linux, on schema video from lexst";
////		Permit permit = parser.splitRevokeSchema(sql);
//	}
	
//	public static void main(String[] args) {
//		String prefix = "clusters=12 primehost=3 hostmode=share chunksize=10m chunkcopy=3 hostcache=yes  ";
//		SQLParser parser = new SQLParser();
//		Table table = new Table();
//		parser.splitTablePrefix(prefix, table);
//		
//		System.out.printf("cluster number:%d\n", table.getClusters().getNumber());
//		System.out.printf("host mode is:%d\n", table.getMode());
//		System.out.printf("host cache:%b\n", table.isCaching());
//		System.out.printf("chunk size:%d\n", table.getChunkSize());
//		System.out.printf("prime host:%d\n", table.getPrimes());
//		System.out.printf("chunk copy:%d\n", table.getCopy());
//	}
}