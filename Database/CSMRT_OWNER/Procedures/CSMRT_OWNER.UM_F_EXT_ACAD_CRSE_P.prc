CREATE OR REPLACE PROCEDURE             "UM_F_EXT_ACAD_CRSE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_EXT_ACAD_CRSE.
--
--V01   SMT-xxxx 07/23/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_EXT_ACAD_CRSE';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_EXT_ACAD_CRSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_EXT_ACAD_CRSE');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_EXT_ACAD_CRSE disable constraint PK_UM_F_EXT_ACAD_CRSE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_EXT_ACAD_CRSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_EXT_ACAD_CRSE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_EXT_ACAD_CRSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_EXT_ACAD_CRSE';				
insert /*+ append */ into UM_F_EXT_ACAD_CRSE 
WITH G
     AS (SELECT SETID,
                GRADING_SCHEME,
                EFFDT,
                SRC_SYS_ID,
                DESCR,
                DESCRSHORT,
                ROW_NUMBER ()
                OVER (PARTITION BY SETID, GRADING_SCHEME, SRC_SYS_ID
                      ORDER BY EFFDT DESC)
                    G_ORDER
           FROM CSSTG_OWNER.PS_GRADESCHEME_TBL
          WHERE DATA_ORIGIN <> 'D')
SELECT EMPLID           PERSON_ID,
       C.EXT_ORG_ID,
       EXT_COURSE_NBR,
       C.SRC_SYS_ID,
	   INSTITUTION             INSTITUTION_CD,
       nvl(C.DESCR, '-')        EXT_COURSE_DESCR,
       nvl(P.PERSON_SID,2147483646) PERSON_SID, 
       nvl (ORG.EXT_ORG_SID, 2147483646) EXT_ORG_SID,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
       nvl(CAR.EXT_ACAD_CAR_SID, 2147483646) EXT_ACAD_CAR_SID, 
       nvl(S.TST_DATA_SRC_SID, 2147483646) TST_DATA_SRC_SID, 
       nvl(UT.ACAD_UNIT_TYPE_SID, 2147483646) ACAD_UNIT_TYPE_SID, 
       nvl(LVL.EXT_ACAD_LVL_SID, 2147483646) EXT_ACAD_LVL_SID, 
       nvl (T.EXT_TERM_SID, 2147483646) EXT_TERM_SID,
       BEGIN_DT,
       END_DT,
	   CAN_TRNS_TYPE,
	   nvl(X1.XLATSHORTNAME, '-')  CAN_TRNS_TYPE_SD,
       nvl(X1.XLATLONGNAME, '-')  CAN_TRNS_TYPE_LD,
	   COURSE_LEVEL,
	   nvl(X2.XLATSHORTNAME, '-')  COURSE_LEVEL_SD,
       nvl(X2.XLATLONGNAME, '-')  COURSE_LEVEL_LD,
	   CRSE_GRADE_INPUT,
       CRSE_GRADE_OFF,
       EXT_CRSE_TYPE,
	   nvl(X3.XLATSHORTNAME, '-')  EXT_CRSE_TYPE_SD,
       nvl(X3.XLATLONGNAME, '-')  EXT_CRSE_TYPE_LD,
	   EXT_DATA_NBR,
	   EXT_SUBJECT_AREA,
	   NVL (TERM_YEAR, 0)         EXT_TERM_YEAR_SID,
	   GRADING_BASIS,
	   nvl(X4.XLATSHORTNAME, '-')  GRADING_BASIS_SD,
       nvl(X4.XLATLONGNAME, '-')  GRADING_BASIS_LD,
	   C.GRADING_SCHEME,
       nvl(G.DESCRSHORT, '-') GRADING_SCHEME_SD,
       nvl(G.DESCR, '-')      GRADING_SCHEME_LD,
	   SCHOOL_SUBJECT,
       SCHOOL_CRSE_NBR,
	   TRANS_CREDIT_FLAG,
	   UNT_TAKEN,
	   LASTUPDDTTM,
       LASTUPDOPRID,
       'N' LOAD_ERROR, 
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM, 
       1234 BATCH_SID
  FROM CSSTG_OWNER.PS_EXT_COURSE  C
  LEFT OUTER JOIN G
    ON C.INSTITUTION = G.SETID
   AND C.GRADING_SCHEME = G.GRADING_SCHEME
   AND C.SRC_SYS_ID = G.SRC_SYS_ID
   AND G.G_ORDER = 1
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_PERSON P
    on C.EMPLID = P.PERSON_ID
   and C.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_EXT_ORG ORG
    on ORG.EXT_ORG_ID = C.EXT_ORG_ID
   and C.SRC_SYS_ID = ORG.SRC_SYS_ID
   and ORG.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_INSTITUTION I
    on C.INSTITUTION = I.INSTITUTION_CD  
   and C.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_EXT_ACAD_CAR CAR
    on C.EXT_CAREER = CAR.EXT_ACAD_CAR_ID  
   and C.SRC_SYS_ID = CAR.SRC_SYS_ID
   and CAR.DATA_ORIGIN <> 'D'   
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_TST_DATA_SRC S
    on C.LS_DATA_SOURCE = S.TST_DATA_SRC_ID
   and C.SRC_SYS_ID = S.SRC_SYS_ID
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_ACAD_UNIT_TYP UT
    on C.UNT_TYPE = UT.ACAD_UNIT_TYPE_ID
   and C.SRC_SYS_ID = UT.SRC_SYS_ID
   and UT.DATA_ORIGIN <> 'D'  
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_EXT_ACAD_LVL LVL
    on C.EXT_ACAD_LEVEL = LVL.EXT_ACAD_LVL_ID
   and C.SRC_SYS_ID = LVL.SRC_SYS_ID
   and LVL.DATA_ORIGIN <> 'D'  
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_EXT_TERM T
    on T.EXT_TERM_TYPE_ID = 'QTR'
   and C.EXT_TERM = T.EXT_TERM_ID
   and C.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D' 
  LEFT OUTER JOIN CSMRT_OWNER.UM_D_XLATITEM X1
    on 'CAN_TRNS_TYPE' = X1.FIELDNAME
   and C.CAN_TRNS_TYPE = X1.FIELDVALUE
   and C.SRC_SYS_ID = X1.SRC_SYS_ID 
  LEFT OUTER JOIN CSMRT_OWNER.UM_D_XLATITEM X2
    on 'COURSE_LEVEL' = X2.FIELDNAME
   and C.COURSE_LEVEL = X2.FIELDVALUE
   and C.SRC_SYS_ID = X2.SRC_SYS_ID 
  LEFT OUTER JOIN CSMRT_OWNER.UM_D_XLATITEM X3
    on 'EXT_CRSE_TYPE' = X3.FIELDNAME
   and C.EXT_CRSE_TYPE = X3.FIELDVALUE
   and C.SRC_SYS_ID = X3.SRC_SYS_ID
  LEFT OUTER JOIN CSMRT_OWNER.UM_D_XLATITEM X4
    on 'EXT_CRSE_TYPE' = X4.FIELDNAME
   and C.EXT_CRSE_TYPE = X4.FIELDVALUE
   and C.SRC_SYS_ID = X4.SRC_SYS_ID
 WHERE C.DATA_ORIGIN <> 'D'
 ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_EXT_ACAD_CRSE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_EXT_ACAD_CRSE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_EXT_ACAD_CRSE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_EXT_ACAD_CRSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_EXT_ACAD_CRSE enable constraint PK_UM_F_EXT_ACAD_CRSE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_EXT_ACAD_CRSE');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_F_EXT_ACAD_CRSE_P;
/
