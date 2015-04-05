/**
 *
 */
package com.lexst.sql;

import java.util.regex.*;

import com.lexst.db.account.*;
import com.lexst.db.charset.*;
import com.lexst.db.schema.*;
import com.lexst.db.statement.*;

public class SQLChecker {

	private final static String SHOW_CHARSET = "^\\s*(?i)SHOW\\s+(?i)CHARSET\\s*$";
	private final static String SHOW_CHARSET2 = "^\\s*(?i)SHOW\\s+(?i)CHARSET\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s*$";

	private final static String CREATE_SCHEMA =  "^\\s*(?i)CREATE\\s+(?:(?i)SCHEMA|DATABASE)\\s+(.+)$";
	private final static String DROP_SCHEMA = "^\\s*(?i)DROP\\s+(?:(?i)SCHEMA|DATABASE)\\s+(.+)$";
	private final static String SHOW_SCHEMA = "^\\s*(?i)SHOW\\s+(?:(?i)SCHEMA|DATABASE)\\s+(.+)$";

	private final static String CREATE_USER = "^\\s*(?i)CREATE\\s+(?i)USER\\s+(.+)$";
	private final static String DROP_USER = "^\\s*(?i)DROP\\s+(?i)USER\\s+(.+)$";
	private final static String DROP_SHA1USER = "^\\s*(?i)DROP\\s+(?i)SHA1\\s+(?i)USER\\s+(.+)$";
	private final static String ALTER_USER = "^\\s*(?i)ALTER\\s+(?i)USER\\s+(.+)$";

	private final static String GRANT = "^\\s*(?i)GRANT\\s+(.+)$";
	private final static String REVOKE = "^\\s*(?i)REVOKE\\s+(.+)$";

	private final static String CREATE_TABLE = "^\\s*(?i)CREATE\\s+(?i)TABLE\\s+(.+)$";
	private final static String CREATE_INDEX = "^\\s*(?i)CREATE\\s+(?i)INDEX\\s+(.+)$";
	private final static String CREATE_LAYOUT= "^\\s*(?i)CREATE\\s+(?i)LAYOUT\\s+(.+)$";
	private final static String DROP_TABLE =   "^\\s*(?i)DROP\\s+(?i)TABLE\\s+(.+)$";
	private final static String SHOW_TABLE = "^\\s*(?i)SHOW\\s+(?i)TABLE\\s+(.+)$";

	private final static String INSERT_PATTERN = "^\\s*(?i)INSERT\\s+(?i)INTO\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+\\((.+)\\)\\s+(?i)VALUES\\s*\\((.+)\\)\\s*$";
	private final static String INJECT_PATTERN = "^\\s*(?i)INJECT\\s+(?i)INTO\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+\\((.+)\\)\\s+(?i)VALUES\\s*\\((.+)\\)\\s*$";
	
	private final static String SELECT_PATTERN = "^\\s*(?i)SELECT\\s+(.+)\\s+(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(.+)$";
	private final static String DELETE_PATTERN = "^\\s*(?i)DELETE\\s+(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(.+)$";
	private final static String UPDATE_PATTERN = "^\\s*(?i)UPDATE\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\s+(?i)SET\\s+(.+)$";

	private final static String SHOW_GRANTS = "^\\s*(?i)SHOW\\s+(?i)GRANTS\\s*$";

	private final static String SET_CHUNKSIZE = "^\\s*(?i)SET\\s+(?i)CHUNKSIZE\\s+(.+)$";
	private final static String SET_OPTIME = "^\\s*(?i)SET\\s+(?i)OPTIMIZE\\s+(?i)TIME\\s+(.+)$";
	
	private final static String OPTIMIZE = "^\\s*(?i)OPTIMIZE\\s+(.+)$";
	private final static String LOAD_INDEX = "^\\s*(?i)LOAD\\s+(?i)INDEX\\s+(.+)$";
	private final static String UNLOAD_INDEX = "^\\s*(?i)STOP\\s+(?i)INDEX\\s+(.+)$";
	private final static String LOAD_CHUNK = "^\\s*(?i)LOAD\\s+(?i)CHUNK\\s+(.+)$";
	private final static String STOP_CHUNK = "^\\s*(?i)STOP\\s+(?i)CHUNK\\s+(.+)$";
	
	private final static String BUILD_TASK = "^\\s*(?i)BUILD\\s+(?i)TASK\\s+(.+)$";
	
	private final static String DC_PATTERN = "^\\s*(?i)DC\\s+(?i)FROM\\s+(.+)\\s+(?i)TO\\s+(.+)$";
	private final static String DC_SPACE = "^\\s*(?i)DC\\s+(?i)FROM\\s+(?:.*?)(?i)QUERY\\s*:\\s*\\\"(?i)SELECT(?:.+?)(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)(?:.*)$";

	private final static String ADC_PATTERN = "^\\s*(?i)ADC\\s+(?i)FROM\\s+(.+)\\s+(?i)TO\\s+(.+)$";
	private final static String ADC_SPACE = "^\\s*(?i)ADC\\s+(?i)FROM\\s+(?:.*?)(?i)QUERY\\s*:\\s*\\\"(?i)SELECT(?:.+?)(?i)FROM\\s+([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)\\.([a-zA-Z0-9]{1,}[_a-zA-Z0-9]*)(?:.*)$";

	private final static String SHOW_SITE = "^\\s*(?i)SHOW\\s+(?i)(ALL|HOME|LOG|DATA|WORK|BUILD|CALL)\\s+(?i)SITE\\s*(.*)\\s*$";
	
	private final static String SET_COLLECT_PATH = "^\\s*(?i)SET\\s+(?i)COLLECT\\s+(?i)PATH\\s+(.+)\\s*$";
	
	private final static String TEST_COLLECT_TASK = "^\\s*(?i)TEST\\s+(?i)COLLECT\\s+(?i)TASK\\s+(.+)\\s*$";
	
	private SQLParser parser = new SQLParser();

	/**
	 *
	 */
	public SQLChecker() {
		super();
	}
	
	public boolean isSetCollectPath(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SET_COLLECT_PATH);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isTestCollectPath(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.TEST_COLLECT_TASK);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isShowSite(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_SITE);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	public boolean isShowCharset(String sql) {
		// show charset pc
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_CHARSET2);
		Matcher matcher = pattern.matcher(sql);
		if( matcher.matches()) return true;
		// show charset
		pattern = Pattern.compile(SQLChecker.SHOW_CHARSET);
		matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	public boolean isCreateSchema(SQLCharmap map, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		// split database
		Schema db = parser.splitCreateSchema(map, sql);
		return db != null;
	}

	public boolean isDeleteSchema(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if(! matcher.matches()) return false;
		// split drop database
		Schema db = parser.splitDropSchema(sql);
		return db != null;
	}

	public boolean isCreateUser(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		User user = parser.splitCreateUser(sql);
		return user != null;
	}

	public boolean isDropUser(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		User user = parser.splitDropUser(sql);
		return user != null;
	}
	
	public boolean isDropSHA1User(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_SHA1USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		User user = parser.splitDropSHA1User(sql);
		return user != null;
	}

	public boolean isAlterUser(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.ALTER_USER);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		User user = parser.splitAlterUser(sql);
		return user != null;
	}

	public boolean isGrant(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.GRANT);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Permit opt = parser.splitGrant(sql);
		return opt != null;
	}

	public boolean isRevoke(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.REVOKE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Permit opt = parser.splitRevoke(sql);
		return opt != null;
	}

	/**
	 * show user authorither
	 * @param sql
	 * @return
	 */
	public boolean isShowGrants(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_GRANTS);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isCreateTableSyntax(String sql) {
		if(sql == null) return false;
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_TABLE);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	public boolean isCreateIndexSyntax(String sql) {
		if(sql == null) return false;
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_INDEX);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isCreateLayoutSyntax(String sql) {
		if(sql == null) return false;
		Pattern pattern = Pattern.compile(SQLChecker.CREATE_LAYOUT);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isCreateTable(String sqlTable, String sqlIndex, String sqlLayout) {
		if(!isCreateTableSyntax(sqlTable) || !isCreateIndexSyntax(sqlIndex)) {
			return false;
		}
		if(sqlLayout != null && !isCreateLayoutSyntax(sqlLayout)) {
			return false;
		}
		Table table = parser.splitCreateTable(sqlTable, sqlIndex, sqlLayout);
		return table != null;
	}

	public boolean isDeleteTable(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DROP_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Space space = parser.splitDropTable(sql);
		return space != null;
	}

	public boolean isSelectPattern(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SELECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	public Space getSelectSpace(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SELECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;
		String db = matcher.group(2);
		String table = matcher.group(3);
		return new Space(db, table);
	}

	public boolean isSelect(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SELECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Select select = parser.splitSelect(set, table, sql);
		return select != null;
	}

	public boolean isDeletePattern(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DELETE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	public Space getDeleteSpace(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DELETE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new Space(db, table);
	}

	public boolean isDelete(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DELETE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Delete delete = parser.splitDelete(set, table, sql);
		return delete != null;
	}

	public boolean isInsertPattern(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.INSERT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}

	public boolean isInsert(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.INSERT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Insert insert = parser.splitInsert(set, table, sql);
		return insert != null;
	}
	
	public boolean isInjectPattern(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.INJECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isInject(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.INJECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Inject inject = parser.splitInject(set, table, sql);
		return inject != null;
	}

	public Space getInsertSpace(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.INSERT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;
		String schema = matcher.group(1);
		String table = matcher.group(2);
		return new Space(schema, table);
	}
	
	public Space getInjectSpace(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.INJECT_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return null;
		String schema = matcher.group(1);
		String table = matcher.group(2);
		return new Space(schema, table);
	}

	public boolean isUpdate(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.UPDATE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isUpdate(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.UPDATE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		Update update = parser.splitUpdate(set, table, sql);
		return update != null;
	}

	public Space getUpdateSpace(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.UPDATE_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return null;
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new Space(db, table);
	}

	public boolean isDC(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DC_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isDC(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DC_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		DC dc = parser.splitDC(sql, set, table);
		return dc != null;
	}
	
	public boolean isADC(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.ADC_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		return matcher.matches();
	}
	
	public boolean isADC(SQLCharset set, Table table, String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.ADC_PATTERN);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		ADC adc = parser.splitADC(sql, set, table);
		return adc != null;
	}
	
	public Space getDiffuseSpace(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.DC_SPACE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) {
			pattern = Pattern.compile(SQLChecker.ADC_SPACE);
			matcher = pattern.matcher(sql);
			if(!matcher.matches()) return null;
		}
		String db = matcher.group(1);
		String table = matcher.group(2);
		return new Space(db, table);
	}

	public boolean isSetChunkSize(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SET_CHUNKSIZE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return false;
		Object[] objects = parser.splitSetChunkSize(sql);
		return objects != null;
	}
	
	public boolean isSetOptimizeTime(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SET_OPTIME);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return false;
		Object[] objects = parser.splitSetOptimizeTime(sql);
		return objects != null;
	}
	
	public boolean isLoadOptimize(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.OPTIMIZE);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return false;
		SpaceHost sh = parser.splitLoadOptimize(sql);
		return sh != null;
	}

	public boolean isLoadIndex(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.LOAD_INDEX);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return false;
		SpaceHost sh = parser.splitLoadIndex(sql);
		return sh != null;
	}
	
	public boolean isStopIndex(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.UNLOAD_INDEX);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return false;
		SpaceHost sh = parser.splitStopIndex(sql);
		return sh != null;
	}
	
	public boolean isLoadChunk(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.LOAD_CHUNK);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return false;
		SpaceHost sh = parser.splitLoadChunk(sql);
		return sh != null;
	}
	
	public boolean isStopChunk(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.STOP_CHUNK);
		Matcher matcher = pattern.matcher(sql);
		if(!matcher.matches()) return false;
		SpaceHost sh = parser.splitStopChunk(sql);
		return sh != null;
	}
	
	public boolean isBuildTask(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.BUILD_TASK);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		NamingHost n = parser.splitBuildTask(sql);
		return n != null;
	}

	public boolean isShowSchema(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_SCHEMA);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		return parser.splitShowSchema(sql) != null;
	}

	public boolean isShowTable(String sql) {
		Pattern pattern = Pattern.compile(SQLChecker.SHOW_TABLE);
		Matcher matcher = pattern.matcher(sql);
		if (!matcher.matches()) return false;
		return parser.splitShowTable(sql) != null;
	}

//	public static void main(String[] args) {
//		SQLChecker checker = new SQLChecker();
//		String sql = "insert into video.word (word, id, weight) values('char', 1, 1)";
//		Space space = checker.getInsertSpace(sql);
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
//		checker.isInsert(set, table, sql);
//	}

}
