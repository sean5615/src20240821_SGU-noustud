package com.nou.scd.dao;

import java.sql.Connection;
import java.util.Hashtable;
import java.util.Vector;

import javax.servlet.http.HttpSession;

import com.acer.apps.Page;
import com.acer.db.DBManager;
import com.acer.db.query.DBResult;
import com.acer.util.Utility;
import com.nou.UtilityX;

/*
 * (SCDT021) Gateway/*
 *-------------------------------------------------------------------------------*
 * Author    : 國長      2007/05/04
 * Modification Log :
 * Vers     Date           By             Notes
 *--------- -------------- -------------- ----------------------------------------
 * V0.0.1   2007/05/04     國長           建立程式
 *                                        新增 getScdt021ForUse(Hashtable ht)
 * V0.0.2	2007/08/10		north		新增		getMainDataToPrintForSCD216R(Hashtable requestMap)
 =======
 * V0.0.2	2007/08/10     barry          新增 getDataForScd215r(Hashtable ht)
 * V0.0.3	2007/09/03     barry          修改 getDataForScd215r(Hashtable ht)
 * V0.0.4	2008/03/13		sRu				修改 getMainDataToPrintForSCD216R	(Hashtable requestMap)
 * V0.0.5	2011/03/11		klia			新增 getscd504rPrint	(Hashtable requestMap)
 *--------------------------------------------------------------------------------
 */
public class SCDT021GATEWAY {

	/** 資料排序方式 */
	private String orderBy = "";
	private DBManager dbmanager = null;
	private Connection conn = null;
	/* 頁數 */
	private int pageNo = 0;
	/** 每頁筆數 */
	private int pageSize = 0;

	/** 記錄是否分頁 */
	private boolean pageQuery = false;

	/** 用來存放 SQL 語法的物件 */
	private StringBuffer sql = new StringBuffer();

	/**
	 * <pre>
	 *  設定資料排序方式.
	 *  Ex: "AYEAR, SMS DESC"
	 *      先以 AYEAR 排序再以 SMS 倒序序排序
	 * </pre>
	 */
	public void setOrderBy(String orderBy) {
		if (orderBy == null) {
			orderBy = "";
		}
		this.orderBy = orderBy;
	}

	/** 取得總筆數 */
	public int getTotalRowCount() {
		return Page.getTotalRowCount();
	}

	/** 不允許建立空的物件 */
	private SCDT021GATEWAY() {
	}

	/** 建構子，查詢全部資料用 */
	public SCDT021GATEWAY(DBManager dbmanager, Connection conn) {
		this.dbmanager = dbmanager;
		this.conn = conn;
	}

	/** 建構子，查詢分頁資料用 */
	public SCDT021GATEWAY(DBManager dbmanager, Connection conn, int pageNo,
			int pageSize) {
		this.dbmanager = dbmanager;
		this.conn = conn;
		this.pageNo = pageNo;
		this.pageSize = pageSize;
		pageQuery = true;
	}

	/**
	 * 
	 * @param ht
	 *            條件值
	 * @return 回傳 Vector 物件，內容為 Hashtable 的集合，<br>
	 *         每一個 Hashtable 其 KEY 為欄位名稱，KEY 的值為欄位的值<br>
	 *         若該欄位有中文名稱，則其 KEY 請加上 _NAME, EX: SMS 其中文欄位請設為 SMS_NAME
	 * @throws Exception
	 */
	public Vector getScdt021ForUse(Hashtable ht) throws Exception {
		if (ht == null) {
			ht = new Hashtable();
		}
		Vector result = new Vector();
		if (sql.length() > 0) {
			sql.delete(0, sql.length());
		}
		sql.append("SELECT S21.AYEAR, S21.SMS, S21.STNO, S21.KIND, S21.RANK, S21.CENTER_CODE, S21.CRSNO, S21.AWARD_NO "
				+ "FROM SCDT021 S21 " + "WHERE 1 = 1 ");
		if (!Utility.nullToSpace(ht.get("AYEAR")).equals("")) {
			sql.append("AND S21.AYEAR = '"
					+ Utility.nullToSpace(ht.get("AYEAR")) + "' ");
		}
		if (!Utility.nullToSpace(ht.get("SMS")).equals("")) {
			sql.append("AND S21.SMS = '" + Utility.nullToSpace(ht.get("SMS"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("STNO")).equals("")) {
			sql.append("AND S21.STNO = '" + Utility.nullToSpace(ht.get("STNO"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("KIND")).equals("")) {
			sql.append("AND S21.KIND = '" + Utility.nullToSpace(ht.get("KIND"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("RANK")).equals("")) {
			sql.append("AND S21.RANK = '" + Utility.nullToSpace(ht.get("RANK"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("CENTER_CODE")).equals("")) {
			sql.append("AND S21.CENTER_CODE = '"
					+ Utility.nullToSpace(ht.get("CENTER_CODE")) + "' ");
		}
		if (!Utility.nullToSpace(ht.get("CRSNO")).equals("")) {
			sql.append("AND S21.CRSNO = '"
					+ Utility.nullToSpace(ht.get("CRSNO")) + "' ");
		}
		if (!Utility.nullToSpace(ht.get("AWARD_NO")).equals("")) {
			sql.append("AND S21.AWARD_NO = '"
					+ Utility.nullToSpace(ht.get("AWARD_NO")) + "' ");
		}

		if (!orderBy.equals("")) {
			String[] orderByArray = orderBy.split(",");
			orderBy = "";
			for (int i = 0; i < orderByArray.length; i++) {
				orderByArray[i] = "S21." + orderByArray[i].trim();

				if (i == 0) {
					orderBy += "ORDER BY ";
				} else {
					orderBy += ", ";
				}
				orderBy += orderByArray[i].trim();
			}
			sql.append(orderBy.toUpperCase());
			orderBy = "";
		}

		DBResult rs = null;
		try {
			if (pageQuery) {
				// 依分頁取出資料
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(),
						pageNo, pageSize);
			} else {
				// 取出所有資料
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}
			Hashtable rowHt = null;
			while (rs.next()) {
				rowHt = new Hashtable();
				/** 將欄位抄一份過去 */
				for (int i = 1; i <= rs.getColumnCount(); i++)
					rowHt.put(rs.getColumnName(i), rs.getString(i));

				result.add(rowHt);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return result;
	}

	/**
	 * 
	 * @param ht
	 *            條件值
	 * @return 回傳 Vector 物件，內容為 Hashtable 的集合，<br>
	 *         每一個 Hashtable 其 KEY 為欄位名稱，KEY 的值為欄位的值<br>
	 *         若該欄位有中文名稱，則其 KEY 請加上 _NAME, EX: SMS 其中文欄位請設為 SMS_NAME
	 * @throws Exception
	 */
	public Vector getDataForScd215r(Hashtable ht) throws Exception {
		DBResult rs = null;
		Vector result = new Vector();
		if (ht == null) {
			ht = new Hashtable();
		}
		if (sql.length() > 0) {
			sql.delete(0, sql.length());
		}
		sql.append("SELECT a.AYEAR, "
				+ "a.RANK, "
				+ "a.AWARD_NO, "
				+ "a.CENTER_CODE, "
				+ "c.NAME, "
				+ "(SELECT CODE_NAME FROM SYST001 WHERE KIND = 'SEX' AND CODE = c.SEX) SEX, "
				+ "c.TEL_OFFICE, "
				+ "c.MOBILE, "
				+ "c.TEL_HOME, "
				+ "c.CRRSADDR, "
				+ "c.BIRTHDATE, "
				+ "a.STNO, "
				+ "d.AVG_MARK, "
				+ "(SELECT CRSNO_SMSGPA FROM SCDT004 WHERE AYEAR = d.AYEAR AND SMS = d.SMS AND STNO = d.STNO AND CRSNO = a.CRSNO) CRSNO_SMSGPA,"
				+ "(SELECT CODE_NAME FROM SYST001 WHERE KIND = 'CENTER_CODE' AND CODE = a.CENTER_CODE) CENTER_NAME, "
				+ "(SELECT CODE_NAME FROM SYST001 WHERE KIND = 'SMS' AND CODE = a.SMS) SMS_NAME,e.CRS_NAME,F.CRS_NAME || ' (' || F.TUT_CLASS_CODE || ')' AS CRSNO_CLASS  "
				+ "FROM SCDT021 a, STUT003 b, STUT002 c, SCDT008 d,COUT002 e,  "
				+ "(SELECT F1.*, F2.TUT_CLASS_CODE, F3.CRS_NAME FROM (SELECT F0.AYEAR, F0.SMS, F0.STNO, MIN(F0.CRSNO) AS MIN_CRSNO  "
				+ "FROM REGT007 F0  " + "WHERE F0.AYEAR = '"
				+ Utility.nullToSpace(ht.get("NEXT_AYEAR"))
				+ "'  "
				+ "AND F0.SMS = '"
				+ Utility.nullToSpace(ht.get("NEXT_SMS"))
				+ "'  "
				+ "AND F0.UNQUAL_TAKE_MK = 'N'  "
				+ "AND F0.UNTAKECRS_MK = 'N'  "
				+ "AND F0.PAYMENT_STATUS != '1'  "
				+ "GROUP BY F0.AYEAR, F0.SMS, F0.STNO  "
				+ ") F1  "
				+ "INNER JOIN REGT007 F2 ON F2.AYEAR  = F1.AYEAR AND F2.SMS  = F1.SMS    "
				+ "AND F2.STNO  = F1.STNO AND F2.CRSNO = F1.MIN_CRSNO  "
				+ "LEFT JOIN COUT002 F3 ON F3.CRSNO = F1.MIN_CRSNO  "
				+ ") F  "
				+ "WHERE a.STNO = b.STNO (+) "
				+ "AND b.IDNO = c.IDNO (+) "
				+ "AND b.BIRTHDATE = c.BIRTHDATE (+) "
				+ "AND d.AYEAR = a.AYEAR "
				+ "AND d.SMS = a.SMS "
				+ "AND d.STNO = a.STNO "
				+ "AND a.CRSNO = e.CRSNO(+)  "
				+ "AND F.STNO(+) = A.STNO  "
		// "AND F.AYEAR(+)  = '" + Utility.nullToSpace(ht.get("NEXT_AYEAR")) +
		// "' AND F.SMS(+)  = '" + Utility.nullToSpace(ht.get("NEXT_SMS")) +
		// "' AND F.STNO(+) = A.STNO AND F.UNQUAL_TAKE_MK(+) = 'N' AND F.UNTAKECRS_MK(+) = 'N' AND F.PAYMENT_STATUS(+) != '1' "

		);
		if (!Utility.nullToSpace(ht.get("AYEAR")).equals("")) {
			sql.append("AND a.AYEAR = '" + Utility.nullToSpace(ht.get("AYEAR"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("ASYS")).equals("")) {
			sql.append("AND a.ASYS = '" + Utility.nullToSpace(ht.get("ASYS"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("SMS")).equals("")) {
			sql.append("AND a.SMS = '" + Utility.nullToSpace(ht.get("SMS"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("KIND")).equals("")) {
			sql.append("AND a.KIND = '" + Utility.nullToSpace(ht.get("KIND"))
					+ "' ");
		}
		if (!Utility.nullToSpace(ht.get("CENTER_CODE")).equals("")) {
			sql.append("AND a.CENTER_CODE = '"
					+ Utility.nullToSpace(ht.get("CENTER_CODE")) + "' ");
		}
		if (!Utility.nullToSpace(ht.get("RANK")).equals("")) {
			sql.append("AND TO_NUMBER(a.RANK) = TO_NUMBER('"
					+ Utility.nullToSpace(ht.get("RANK")) + "') ");
			sql.append("ORDER BY NVL(a.AWARD_NO,'0'),a.CENTER_CODE,a.CRSNO ");
		} else {
			sql.append("ORDER BY NVL(a.AWARD_NO,'0'),a.RANK, a.CENTER_CODE ");
		}

		try {
			// 取出所有資料
			rs = dbmanager.getSimpleResultSet(conn);
			rs.open();
			rs.executeQuery(sql.toString());
			Hashtable rowHt = null;
			while (rs.next()) {
				rowHt = new Hashtable();
				/** 將欄位抄一份過去 */
				for (int i = 1; i <= rs.getColumnCount(); i++) {
					if (rs.getColumnName(i).equals("AYEAR")) {
						if (rs.getString(i).indexOf("0") == 0)
							rowHt.put(rs.getColumnName(i), rs.getString(i)
									.substring(1));
						else
							rowHt.put(rs.getColumnName(i), rs.getString(i));
					} else {
						rowHt.put(rs.getColumnName(i), rs.getString(i));
					}
				}
				result.add(rowHt);
			}
		} catch (Exception e) {
			throw e;
		}
		return result;
	}

	/**
	 * north 2007/8/10 列印績優生獎狀(SCD216R)
	 *
	 * @param vt -
	 *            回傳值
	 * @param requestMap -
	 *            條件值
	 * @throws Exception
	 * 目的:列印資料主要由名次統計檔取得
	 */
    public Vector getMainDataToPrintForSCD216R(Hashtable requestMap, HttpSession session) throws Exception {
		Vector result = new Vector();	
		StringBuffer sb2 = new StringBuffer();
		sb2.append("SELECT CODE, CODE_NAME FROM SYST001 WHERE KIND = 'SMS' ");
		StringBuffer sb3 = new StringBuffer();
		sb3.append("SELECT STUT003.STNO, STUT002.NAME, STUT002.IDNO, STUT003.ASYS " + "FROM "
				+ "STUT002 LEFT JOIN STUT003 " + "ON "
				+ "STUT003.IDNO = STUT002.IDNO AND "
				+ "STUT003.BIRTHDATE = STUT002.BIRTHDATE ");

		// 將 SQL 清空
		if (sql.length() > 0)
			sql.delete(0, sql.length());

		sql.append("SELECT SCDT021.AYEAR, SCDT021.RANK, SCDT021.AWARD_NO, SCDT021.STNO, "
						+ "SCDT021.CENTER_CODE, DECODE(C.ASYS,'1',A.CENTER_NAME,'2',A.CENTER_NAME,A.CENTER_NAME) AS CENTER_CODE_NAME, " // 空專改制
						+ "B.CODE_NAME AS SMS_NAME,"
						+ "DECODE(S3.ASYS,'1','大學部','2','專科部') AS STNO_ASYS_NAME,"
				+ "C.NAME, D.CRS_NAME,SCDT021.CRSNO AS CRSNO  "
				+ ",DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 1, 4) - 1911) AS PRNYEAR "
				+",DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 5, 2)) AS PRNMONTH "
				+",DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 7, 2)) AS PRNDAY "
						+ "FROM SCDT021, "
						+ "SYST002 A, "
						+ "STUT003 S3, "
						+ "SGUT201 S1, "
						+ "(" + sb2 + ") B, "
						+ "(" + sb3 + ") C , COUT002 D WHERE "
						+ "SCDT021.CENTER_CODE = A.CENTER_CODE(+) AND "
						+ "SCDT021.STNO = S3.STNO AND "
						+ "SCDT021.SMS = B.CODE(+) AND "
						+ "SCDT021.STNO = C.STNO(+) AND "
						+ "SCDT021.AYEAR='" + requestMap.get("AYEAR")+ "' AND "
						+ "SCDT021.SMS='" + requestMap.get("SMS")+ "' AND "
				+ "SCDT021.AYEAR = S1.AYEAR(+) AND "
				+ "SCDT021.SMS = S1.SMS(+) AND "
						+ "SCDT021.KIND='" + requestMap.get("print_type")+ "' "
						+ " AND SCDT021.CRSNO=D.CRSNO(+)  "
						 );

		if(!Utility.nullToSpace(requestMap.get("STNO")).equals(""))
		{
            sql.append("AND SCDT021.STNO = '" + Utility.nullToSpace(requestMap.get("STNO")) + "' ");
        }
		if(!Utility.nullToSpace(requestMap.get("ASYS")).equals(""))
		{
            sql.append("AND C.ASYS = '" + Utility.nullToSpace(requestMap.get("ASYS")) + "' ");
        }
		if(!Utility.nullToSpace(requestMap.get("CENTER_CODE")).equals(""))
		{
            sql.append("AND SCDT021.CENTER_CODE = '" + Utility.nullToSpace(requestMap.get("CENTER_CODE")) + "' ");
        }

		// 以名次排序
		sql.append(" ORDER BY NVL(SCDT021.AWARD_NO,'0'),SCDT021.RANK, SCDT021.CENTER_CODE, SCDT021.CRSNO ");

		DBResult rs = null;
		try {
			if (pageQuery) {
				// 依分頁取出資料
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(),
						pageNo, pageSize);
			} else {
				// 取出所有資料
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}

			//將結果存放至暫存區
			Vector temp = new Vector();
			while (rs.next()) {
				Hashtable content = new Hashtable();
				/** 將欄位抄一份過去 */
				for (int i = 1; i <= rs.getColumnCount(); i++) {
					content.put(rs.getColumnName(i), rs.getString(i));
				}
				temp.add(content);
			}

			/** 處理獎狀編號 */
			Vector AWARD_NO_collection = new Vector();
			for(int i=0; i<temp.size(); i++){
				String STNO = (String)((Hashtable)temp.get(i)).get("STNO");
				String CRSNO = (String)((Hashtable)temp.get(i)).get("CRSNO");
				if(STNO==null || STNO.trim().length()==0)
					STNO="";
				String AWARD_NO = (String)((Hashtable)temp.get(i)).get("AWARD_NO");
				if(AWARD_NO==null || AWARD_NO.trim().length()==0||Utility.nullToSpace(requestMap.get("USE_USER_SET")).equals("Y")){
					//取得現在的最大號
					//com.nou.sgu.bo.SGUGETAWARDNO SGUGETAWARDNO = new com.nou.sgu.bo.SGUGETAWARDNO(dbmanager,conn);
					//SGUGETAWARDNO.setYEAR(com.acer.util.DateUtil.getNowCYear());
					//int ii = SGUGETAWARDNO.mainProcess();
					//AWARD_NO = SGUGETAWARDNO.getUSED_NO();
					Utility ut = new Utility();
					AWARD_NO = ut.fillStr(AWARD_NO,4,'0');					
            		// update scdt021
            		/** 修改條件 */
            		String	condition	=	  "AYEAR	=	'" + requestMap.get("AYEAR") + "' AND "
											+ "SMS = '" + requestMap.get("SMS") + "' AND "
											+ "STNO = '" + STNO + "' AND "
											+ "KIND = '" + requestMap.get("print_type") + "' ";
            		
            		if("4".equals(requestMap.get("print_type"))){
            			condition += " AND CRSNO = '" + CRSNO + "' ";
            		}
    				/** 處理修改動作 */
    				Hashtable ht = new Hashtable();
    				ht.put("AWARD_NO", AWARD_NO);

    				SCDT021DAO	scdt021_AWARD_NO	=	new SCDT021DAO(dbmanager, conn, ht, session);
    				scdt021_AWARD_NO.update(condition);

    				/** Commit Transaction */
    				dbmanager.commit();
				}
				AWARD_NO_collection.add(AWARD_NO);
			}

			//開始組合所想要顯示的結果
			for(int i=0; i<temp.size(); i++){
				Hashtable content = new Hashtable();

				// 學年
				String AYEAR = (String)((Hashtable)temp.get(i)).get("AYEAR");
				if(AYEAR==null || AYEAR.trim().length()==0)
					AYEAR = "";
				else{
					if(AYEAR.charAt(0)=='0')
						AYEAR = AYEAR.substring(1);
				}
				content.put("AYEAR", AYEAR);

				// 名次
				String RANK = (String)((Hashtable)temp.get(i)).get("RANK");
				if(RANK==null || RANK.trim().length()==0)
					RANK = "0";
				content.put("RANK", RANK);

				// 獎狀編號
				String AWARD_NO = (String)AWARD_NO_collection.get(i);
				//if(AWARD_NO==null || AWARD_NO.trim().length()==0)
					//AWARD_NO = "0001";
				content.put("AWARD_NO", AWARD_NO);
				
				// 學生中心別
				String STNO_ASYS_NAME = (String) ((Hashtable) temp.get(i))
						.get("STNO_ASYS_NAME");
				content.put("STNO_ASYS_NAME", STNO_ASYS_NAME);

				// 姓名
				String NAME = (String)((Hashtable)temp.get(i)).get("NAME");
				if(NAME==null || NAME.trim().length()==0)
					NAME = "";
				content.put("NAME", NAME);

				// 中心名稱
				String CENTER_CODE_NAME = (String)((Hashtable)temp.get(i)).get("CENTER_CODE_NAME");
				if(CENTER_CODE_NAME==null || CENTER_CODE_NAME.trim().length()==0)
					RANK = "";
				content.put("CENTER_CODE_NAME", CENTER_CODE_NAME);

				// 學期名稱
				String SMS_NAME = (String)((Hashtable)temp.get(i)).get("SMS_NAME");
				if(SMS_NAME==null || SMS_NAME.trim().length()==0)
					SMS_NAME = "";
				content.put("SMS_NAME", SMS_NAME);

				// 科目名稱
				String CRS_NAME = (String)((Hashtable)temp.get(i)).get("CRS_NAME");
				if(CRS_NAME==null || CRS_NAME.trim().length()==0)
					CRS_NAME = "";
				content.put("CRS_NAME", CRS_NAME);
				
				// 列印日期
				String PRNYEAR = (String)((Hashtable)temp.get(i)).get("PRNYEAR");
				if(PRNYEAR==null || PRNYEAR.trim().length()==0)
					PRNYEAR = "";
				content.put("PRNYEAR", PRNYEAR);
				String PRNMONTH = (String)((Hashtable)temp.get(i)).get("PRNMONTH");
				if(PRNMONTH==null || PRNMONTH.trim().length()==0)
					PRNMONTH = "";
				content.put("PRNMONTH", PRNMONTH);
				String PRNDAY = (String)((Hashtable)temp.get(i)).get("PRNDAY");
				if(PRNDAY==null || PRNDAY.trim().length()==0)
					PRNDAY = "";
				content.put("PRNDAY", PRNDAY);

				result.add(content);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return result;
	}


	/**
	 * north 2024/6/10 列印績優生獎狀(SCD216Q)
	 * 
	 * @param vt
	 *            - 回傳值
	 * @param requestMap
	 *            - 條件值
	 * @throws Exception
	 *             目的:列印資料主要由名次統計檔取得
	 */
	public Vector getMainDataToPrintForSCD216Q(Hashtable requestMap,
			HttpSession session) throws Exception {
		Vector result = new Vector();
		StringBuffer sb2 = new StringBuffer();
		sb2.append("SELECT CODE, CODE_NAME FROM SYST001 WHERE KIND = 'SMS' ");
		StringBuffer sb3 = new StringBuffer();
		sb3.append("SELECT STUT003.STNO, STUT002.NAME, STUT002.IDNO, STUT003.ASYS "
				+ "FROM "
				+ "STUT002 LEFT JOIN STUT003 "
				+ "ON "
				+ "STUT003.IDNO = STUT002.IDNO AND "
				+ "STUT003.BIRTHDATE = STUT002.BIRTHDATE ");

		// 將 SQL 清空
		if (sql.length() > 0)
			sql.delete(0, sql.length());

		sql.append("SELECT SCDT021.AYEAR, SCDT021.RANK, SCDT021.AWARD_NO, SCDT021.STNO, "
				+ "SCDT021.CENTER_CODE, DECODE(C.ASYS,'1',A.CENTER_NAME,'2',A.CENTER_NAME,A.CENTER_NAME) AS CENTER_CODE_NAME, " // 空專改制
				+ "B.CODE_NAME AS SMS_NAME,"
				+ "DECODE(S3.ASYS,'1','大學部','2','專科部') AS STNO_ASYS_NAME,"
				+ "C.NAME, D.CRS_NAME,SCDT021.CRSNO AS CRSNO  "
				+ ",DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 1, 4) - 1911) AS PRNYEAR "
				+ ",DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 5, 2)) AS PRNMONTH "
				+ ",DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 7, 2)) AS PRNDAY "
				+ "FROM SCDT021, "
				+ "STUT004 R1, "
				+ "SYST002 A, "
				+ "STUT003 S3, "
				+ "SGUT201 S1, "
				+ "("
				+ sb2
				+ ") B, "
				+ "("
				+ sb3
				+ ") C , COUT002 D WHERE "
				+ "SCDT021.AYEAR = R1.AYEAR AND "
				+ "SCDT021.SMS = R1.SMS AND "
				+ "SCDT021.STNO = R1.STNO(+) AND "
				+ "R1.CENTER_CODE = A.CENTER_CODE(+) AND "
				+ "SCDT021.STNO = S3.STNO AND "
				+ "SCDT021.SMS = B.CODE(+) AND "
				+ "SCDT021.STNO = C.STNO(+) AND "
				+ "SCDT021.AYEAR='"
				+ requestMap.get("AYEAR")
				+ "' AND "
				+ "SCDT021.SMS='"
				+ requestMap.get("SMS")
				+ "' AND "
				+ "SCDT021.AYEAR = S1.AYEAR(+) AND "
				+ "SCDT021.SMS = S1.SMS(+) AND "
				+ "SCDT021.KIND='"
				+ requestMap.get("print_type")
				+ "' "
				+ " AND SCDT021.CRSNO=D.CRSNO(+)  ");

		if (!Utility.nullToSpace(requestMap.get("STNO")).equals("")) {
			sql.append("AND SCDT021.STNO = '"
					+ Utility.nullToSpace(requestMap.get("STNO")) + "' ");
		}
		if (!Utility.nullToSpace(requestMap.get("ASYS")).equals("")) {
			sql.append("AND C.ASYS = '"
					+ Utility.nullToSpace(requestMap.get("ASYS")) + "' ");
		}
		if (!Utility.nullToSpace(requestMap.get("CENTER_CODE")).equals("")) {
			sql.append("AND SCDT021.CENTER_CODE = '"
					+ Utility.nullToSpace(requestMap.get("CENTER_CODE")) + "' ");
		}
		if (!Utility.nullToSpace(requestMap.get("CRSNO")).equals("")) {
			sql.append("AND SCDT021.CRSNO = '"+ Utility.nullToSpace(requestMap.get("CRSNO")) + "' ");
		}

		// 以名次排序
		sql.append(" ORDER BY NVL(SCDT021.AWARD_NO,'0'),SCDT021.RANK, SCDT021.CENTER_CODE, SCDT021.CRSNO ");

		DBResult rs = null;
		try {
			if (pageQuery) {
				// 依分頁取出資料
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(),
						pageNo, pageSize);
			} else {
				// 取出所有資料
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}

			// 將結果存放至暫存區
			Vector temp = new Vector();
			while (rs.next()) {
				Hashtable content = new Hashtable();
				/** 將欄位抄一份過去 */
				for (int i = 1; i <= rs.getColumnCount(); i++) {
					content.put(rs.getColumnName(i), rs.getString(i));
				}
				temp.add(content);
			}



			// 開始組合所想要顯示的結果
			for (int i = 0; i < temp.size(); i++) {
				Hashtable content = new Hashtable();

				// 學年
				String AYEAR = (String) ((Hashtable) temp.get(i)).get("AYEAR");
				if (AYEAR == null || AYEAR.trim().length() == 0)
					AYEAR = "";
				else {
					if (AYEAR.charAt(0) == '0')
						AYEAR = AYEAR.substring(1);
				}
				content.put("AYEAR", AYEAR);

				// 名次
				String RANK = (String) ((Hashtable) temp.get(i)).get("RANK");
				if (RANK == null || RANK.trim().length() == 0)
					RANK = "0";
				content.put("RANK", RANK);

				// 獎狀編號
				String AWARD_NO = (String) ((Hashtable) temp.get(i)).get("AWARD_NO");
				// if(AWARD_NO==null || AWARD_NO.trim().length()==0)
				// AWARD_NO = "0001";
				content.put("AWARD_NO", AWARD_NO);

				// 學生中心別
				String STNO_ASYS_NAME = (String) ((Hashtable) temp.get(i))
						.get("STNO_ASYS_NAME");
				content.put("STNO_ASYS_NAME", STNO_ASYS_NAME);

				// 姓名
				String NAME = (String) ((Hashtable) temp.get(i)).get("NAME");
				if (NAME == null || NAME.trim().length() == 0)
					NAME = "";
				content.put("NAME", NAME);

				// 中心名稱
				String CENTER_CODE_NAME = (String) ((Hashtable) temp.get(i))
						.get("CENTER_CODE_NAME");
				if (CENTER_CODE_NAME == null
						|| CENTER_CODE_NAME.trim().length() == 0)
					RANK = "";
				content.put("CENTER_CODE_NAME", CENTER_CODE_NAME);

				// 學期名稱
				String SMS_NAME = (String) ((Hashtable) temp.get(i))
						.get("SMS_NAME");
				if (SMS_NAME == null || SMS_NAME.trim().length() == 0)
					SMS_NAME = "";
				content.put("SMS_NAME", SMS_NAME);

				// 科目名稱
				String CRS_NAME = (String) ((Hashtable) temp.get(i))
						.get("CRS_NAME");
				if (CRS_NAME == null || CRS_NAME.trim().length() == 0)
					CRS_NAME = "";
				content.put("CRS_NAME", CRS_NAME);

				// 列印日期
				String PRNYEAR = (String) ((Hashtable) temp.get(i))
						.get("PRNYEAR");
				if (PRNYEAR == null || PRNYEAR.trim().length() == 0)
					PRNYEAR = "";
				content.put("PRNYEAR", PRNYEAR);
				String PRNMONTH = (String) ((Hashtable) temp.get(i))
						.get("PRNMONTH");
				if (PRNMONTH == null || PRNMONTH.trim().length() == 0)
					PRNMONTH = "";
				content.put("PRNMONTH", PRNMONTH);
				String PRNDAY = (String) ((Hashtable) temp.get(i))
						.get("PRNDAY");
				if (PRNDAY == null || PRNDAY.trim().length() == 0)
					PRNDAY = "";
				content.put("PRNDAY", PRNDAY);

				result.add(content);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return result;
	}

	/**
	 * 查詢績優生獎狀狀況
	 */
	public Vector getScd216qQuery(Hashtable ht) throws Exception {
		Vector result = new Vector();

		if (sql.length() > 0)
			sql.delete(0, sql.length());

		sql.append("  SELECT M.AYEAR, M.SMS, M.ASYS, M.KIND, ");
		sql.append("  DECODE(M.ASYS||M.KIND,'11','(大學部)全校前 20 名','21','(專科部)全校前 3 名','12','(大學部)各中心第 1 名','22','(專科部)各中心第 1 名','14','各中心各科第 1 名','24','各中心各科第 1 名') AS KIND_NAME, ");
		sql.append("  M.RANK, M.AWARD_NO, M.STNO, M.CENTER_CODE, ");
		sql.append("  DECODE(C.ASYS,'1',A.CENTER_NAME,'2',A.CENTER_NAME,A.CENTER_NAME) AS CENTER_CODE_NAME, M.AYEAR || '學年度' AS AYEAR_NAME ,B.CODE_NAME AS SMS_NAME, ");
		sql.append("  DECODE(S3.ASYS,'1','大學部','2','專科部') AS STNO_ASYS_NAME,C.NAME, D.CRS_NAME,M.CRSNO AS CRSNO  , ");
		sql.append("  DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 1, 4) - 1911) AS PRNYEAR , ");
		sql.append("  DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 5, 2)) AS PRNMONTH , ");
		sql.append("  DECODE(S1.SCD_PRINT_DATE,'N','',SUBSTR (S1.SCD_PRINT_DATE, 7, 2)) AS PRNDAY ");
		sql.append("  FROM SCDT021 M ");
		sql.append("  JOIN STUT004 R1 ON R1.AYEAR = M.AYEAR AND R1.SMS = M.SMS AND R1.STNO = M.STNO ");
		sql.append("  left join SYST002 A  ON R1.CENTER_CODE = A.CENTER_CODE ");
		sql.append("  left join STUT003 S3 ON M.STNO = S3.STNO ");
		sql.append("  left join SGUT201 S1 ON M.AYEAR = S1.AYEAR AND M.SMS = S1.SMS ");
		sql.append("  left join (SELECT CODE, CODE_NAME FROM SYST001 WHERE KIND = 'SMS' ) B ON M.SMS = B.CODE ");
		sql.append("  left join (SELECT STUT003.STNO, STUT002.NAME, STUT002.IDNO, STUT003.ASYS FROM STUT002 LEFT JOIN STUT003 ON STUT003.IDNO = STUT002.IDNO AND STUT003.BIRTHDATE = STUT002.BIRTHDATE ) C  ON M.STNO = C.STNO ");
		sql.append("  left join COUT002 D ON M.CRSNO=D.CRSNO ");
		sql.append("   WHERE M.STNO = '"+ Utility.dbStr(UtilityX.checkNullEmpty(ht.get("STNO"), " "))+ "' ");
		//自112學年度下學期起開放學生線上列印，判斷獎狀列印日期大於'20240801' 同時要小於等於系統日期始可查詢列印
		sql.append("   AND S1.SCD_PRINT_DATE >= '20240801' AND S1.SCD_PRINT_DATE <= TO_CHAR(SYSDATE,'YYYYMMDD')  ");
		sql.append("   AND M.AWARD_NO IS NOT NULL ");
		sql.append("  order by M.AYEAR||M.SMS DESC,M.KIND, NVL(M.AWARD_NO,'0'),M.RANK, M.CENTER_CODE, M.CRSNO  ");

		DBResult rs = null;
		try {
			if (pageQuery) {
				// 依分頁取出資料
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(),
						pageNo, pageSize);
			} else {
				// 取出所有資料
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}
			Hashtable rowHt = null;
			while (rs.next()) {
				rowHt = new Hashtable();
				/** 將欄位抄一份過去 */
				for (int i = 1; i <= rs.getColumnCount(); i++) {
					rowHt.put(rs.getColumnName(i), rs.getString(i));
				}

				result.add(rowHt);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return result;
	}

	/**
	 * 取得學生姓名
	 * 
	 * @param ht
	 * @return
	 * @throws Exception
	 */
	public Vector getScd046qStnoName(Hashtable ht) throws Exception {
		if (ht == null) {
			ht = new Hashtable();
		}
		Vector result = new Vector();
		if (sql.length() > 0) {
			sql.delete(0, sql.length());
		}

		sql.append(" SELECT R1.*,NVL(R2.STTYPE,M.STTYPE) AS STTYPE, M.ENROLL_STATUS ");
		sql.append(" FROM STUT003 M  ");
		sql.append(" 		LEFT JOIN STUT002 R1  ");
		sql.append(" 		  ON R1.IDNO = M.IDNO  ");
		sql.append(" 		LEFT JOIN SCDT021 R2 ON R2.AYEAR = '"
				+ UtilityX.checkNullEmpty(ht.get("NOW_AYEAR"), " ")
				+ "' AND R2.SMS = '"
				+ UtilityX.checkNullEmpty(ht.get("NOW_SMS"), " ")
				+ "' AND R2.STNO = M.STNO ");
		sql.append(" WHERE M.STNO = '")
				.append(Utility.nullToSpace(ht.get("STNO"))).append("' ");

		DBResult rs = null;
		try {
			// 取出所有資料
			rs = dbmanager.getSimpleResultSet(conn);
			rs.open();
			rs.executeQuery(sql.toString());

			Hashtable rowHt = null;
			while (rs.next()) {
				rowHt = new Hashtable();
				/** 將欄位抄一份過去 */
				for (int i = 1; i <= rs.getColumnCount(); i++)
					rowHt.put(rs.getColumnName(i), rs.getString(i));

				result.add(rowHt);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		return result;
	}

	/**
	 * north 2007/8/10 列印績優生獎狀(SCD216R)
	 * 
	 * @param vt
	 *            - 回傳值
	 * @param requestMap
	 *            - 條件值
	 * @throws Exception
	 *             目的:列印資料主要由名次統計檔取得
	 */
	public Vector getMainDataToPrintForSCD216R2(Hashtable requestMap,
			HttpSession session) throws Exception {
		Vector result = new Vector();

		// 學期
		StringBuffer sb2 = new StringBuffer();
		sb2.append("SELECT CODE, CODE_NAME FROM SYST001 WHERE KIND = 'SMS' ");
		// 姓名
		StringBuffer sb3 = new StringBuffer();

		sb3.append("SELECT STUT003.STNO, STUT002.NAME, STUT002.IDNO, STUT003.ASYS "
				+ "FROM "
				+ "STUT002 LEFT JOIN STUT003 "
				+ "ON "
				+ "STUT003.IDNO = STUT002.IDNO AND "
				+ "STUT003.BIRTHDATE = STUT002.BIRTHDATE ");

		// 將 SQL 清空
		if (sql.length() > 0)
			sql.delete(0, sql.length());

		sql.append("SELECT SCDT021.AYEAR, SCDT021.RANK, SCDT021.AWARD_NO, SCDT021.STNO, "
				+ "SCDT021.CENTER_CODE,DECODE(C.ASYS,'1',A.CENTER_NAME,'2',A.J_CENTER_NAME,A.CENTER_NAME) AS CENTER_CODE_NAME,"
				+ "B.CODE_NAME AS "
				+ "SMS_NAME, C.NAME, D.CRS_NAME "
				+ "FROM SCDT021, "
				+ "SYST002 A, "
				+ "("
				+ sb2
				+ ") B, "
				+ "("
				+ sb3
				+ ") C , COUT002 D WHERE "
				+ "SCDT021.CENTER_CODE = A.CENTER_CODE(+) AND "
				+ "SCDT021.SMS = B.CODE(+) AND "
				+ "SCDT021.STNO = C.STNO(+) AND "
				+ "SCDT021.AYEAR='"
				+ requestMap.get("AYEAR")
				+ "' AND "
				+ "SCDT021.SMS='"
				+ requestMap.get("SMS")
				+ "' AND "
				+ "SCDT021.KIND='"
				+ requestMap.get("print_type")
				+ "' "
				+ " AND SCDT021.CRSNO=D.CRSNO(+)  ");

		if (!Utility.nullToSpace(requestMap.get("STNO")).equals("")) {
			sql.append("AND SCDT021.STNO = '"
					+ Utility.nullToSpace(requestMap.get("STNO")) + "' ");
		}
		if (!Utility.nullToSpace(requestMap.get("ASYS")).equals("")) {
			sql.append("AND C.ASYS = '"
					+ Utility.nullToSpace(requestMap.get("ASYS")) + "' ");
		}
		if (!Utility.nullToSpace(requestMap.get("CENTER_CODE")).equals("")) {
			sql.append("AND SCDT021.CENTER_CODE = '"
					+ Utility.nullToSpace(requestMap.get("CENTER_CODE")) + "' ");
		}

		// 以名次排序
		sql.append(" ORDER BY SCDT021.RANK, SCDT021.CENTER_CODE ");

		DBResult rs = null;
		try {
			rs = dbmanager.getSimpleResultSet(conn);
			rs.open();
			rs.executeQuery(sql.toString());

			while (rs.next()) {
				Hashtable content = new Hashtable();
				/** 將欄位抄一份過去 */
				for (int i = 1; i <= rs.getColumnCount(); i++)
					content.put(rs.getColumnName(i), rs.getString(i));

				result.add(content);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
		}

		return result;
	}

	/**
	 * 
	 * @param ht
	 *            條件值
	 * @return 回傳 Vector 物件，內容為 Hashtable 的集合，<br>
	 *         每一個 Hashtable 其 KEY 為欄位名稱，KEY 的值為欄位的值<br>
	 *         若該欄位有中文名稱，則其 KEY 請加上 _NAME, EX: SMS 其中文欄位請設為 SMS_NAME
	 * @throws Exception
	 */
	public Vector getscd504rPrint(Hashtable ht) throws Exception {
		DBResult rs = null;
		Vector vt = new Vector();
		try {
			if (sql.length() > 0)
				sql.delete(0, sql.length());

			// 取得查詢頁面的查詢條件
			String AYEAR = "";
			if (!Utility.checkNull(ht.get("AYEAR"), "").equals(""))
				AYEAR = Utility.dbStr(ht.get("AYEAR"));

			String SMS = "";
			if (!Utility.checkNull(ht.get("SMS"), "").equals(""))
				SMS = Utility.dbStr(ht.get("SMS"));

			sql.append("select distinct s21.CRSNO,c2.CRS_NAME,NVL(A01.NUM,0) as C01,NVL(A02.NUM,0) as C02,NVL(A15.NUM,0) as C15,NVL(A03.NUM,0) as C03,NVL(A04.NUM,0) as C04, \n"
					+ "NVL(A05.NUM,0) as C05,NVL(A06.NUM,0) as C06,NVL(A07.NUM,0) as C07,NVL(A08.NUM,0) as C08,NVL(A09.NUM,0) as C09, \n"
					+ "NVL(A10.NUM,0) as C10,NVL(A11.NUM,0) as C11,NVL(A12.NUM,0) as C12,NVL(A13.NUM,0) as C13,NVL(A14.NUM,0) as C14,NVL(AC.NUM,0) as TOTALNUM \n"
					+ "from scdt021 s21 \n"
					+ "join cout002 c2 on c2.CRSNO = s21.CRSNO\n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '01' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A01 on A01.ayear = s21.ayear and A01.sms = s21.sms and A01.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '02' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A02 on A02.ayear = s21.ayear and A02.sms = s21.sms and A02.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '15' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A15 on A15.ayear = s21.ayear and A15.sms = s21.sms and A15.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '03' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A03 on A03.ayear = s21.ayear and A03.sms = s21.sms and A03.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '04' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A04 on A04.ayear = s21.ayear and A04.sms = s21.sms and A04.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '05' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A05 on A05.ayear = s21.ayear and A05.sms = s21.sms and A05.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '06' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A06 on A06.ayear = s21.ayear and A06.sms = s21.sms and A06.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '07' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A07 on A07.ayear = s21.ayear and A07.sms = s21.sms and A07.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '08' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A08 on A08.ayear = s21.ayear and A08.sms = s21.sms and A08.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '09' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A09 on A09.ayear = s21.ayear and A09.sms = s21.sms and A09.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '10' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A10 on A10.ayear = s21.ayear and A10.sms = s21.sms and A10.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '11' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A11 on A11.ayear = s21.ayear and A11.sms = s21.sms and A11.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '12' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A12 on A12.ayear = s21.ayear and A12.sms = s21.sms and A12.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '13' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A13 on A13.ayear = s21.ayear and A13.sms = s21.sms and A13.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' and a.CENTER_CODE = '14' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") A14 on A14.ayear = s21.ayear and A14.sms = s21.sms and A14.CRSNO = s21.CRSNO \n"
					+ "left join \n"
					+ "(select a.ayear,a.sms,a.CRSNO,NVL(count(a.STNO),0) as NUM \n"
					+ "from scdt021 a \n"
					+ "where a.kind = '4' \n"
					+ "group by a.ayear,a.sms,a.CRSNO \n"
					+ ") AC on AC.ayear = s21.ayear and AC.sms = s21.sms and AC.CRSNO = s21.CRSNO \n"
					+ "where s21.ayear = '"
					+ AYEAR
					+ "' and s21.sms = '"
					+ SMS
					+ "' \n" + "and s21.kind = '4' \n" + "order by s21.CRSNO");
			if (pageQuery) {
				rs = Page.getPageResultSet(dbmanager, conn, sql.toString(),
						pageNo, pageSize);
			} else {
				rs = dbmanager.getSimpleResultSet(conn);
				rs.open();
				rs.executeQuery(sql.toString());
			}

			Hashtable rowHt = null;
			while (rs.next()) {
				rowHt = new Hashtable();
				for (int i = 1; i <= rs.getColumnCount(); i++)
					rowHt.put(rs.getColumnName(i), rs.getString(i));
				vt.add(rowHt);
			}

		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null)
				rs.close();
		}
		return vt;
	}
}