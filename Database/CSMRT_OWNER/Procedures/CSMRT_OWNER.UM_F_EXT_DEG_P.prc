CREATE OR REPLACE PROCEDURE             "UM_F_EXT_DEG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_EXT_DEG.
--
 --V01  SMT-xxxx 07/20/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_EXT_DEG';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_EXT_DEG');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_EXT_DEG disable constraint PK_UM_F_EXT_DEG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_EXT_DEG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_EXT_DEG';				
insert /*+ append */ into UM_F_EXT_DEG 
WITH XDEG AS
(
SELECT /*+ PARALLEL(8) INLINE */
       EMPLID            PERSON_ID,
       EXT_ORG_ID,
       EXT_DEGREE_NBR,
       SRC_SYS_ID,
       DESCR,
       DEGREE_DT        EXT_DEG_DT,
       DEGREE_STATUS    EXT_DEG_STAT_ID,
       FIELD_OF_STUDY_1,
       FIELD_OF_STUDY_2,
       HONORS_CATEGORY,         
       DEGREE, 
       EXT_SUBJ_AREA_1,
       EXT_SUBJ_AREA_2,
       LS_DATA_SOURCE,
       EXT_CAREER,
       EXT_DATA_NBR
  FROM CSSTG_OWNER.PS_EXT_DEGREE  XD
 WHERE XD.DATA_ORIGIN <> 'D'
)
SELECT XDEG.PERSON_ID,
       XDEG.EXT_ORG_ID,
       XDEG.EXT_DEGREE_NBR,
       XDEG.SRC_SYS_ID,
       XDEG.DESCR,
       NVL (P.PERSON_SID, 2147483646) PERSON_SID,
       NVL (ORG.EXT_ORG_SID, 2147483646) EXT_ORG_SID,
       NVL (XDS.TST_DATA_SRC_SID, 2147483646) EXT_DATA_SRC_SID,
       NVL (DEG.DEG_SID, 2147483646) EXT_DEG_SID,
       NVL (SUB1.EXT_SUBJECT_AREA_SID, 2147483646) EXT_SUBJECT_AREA_SID_1, 
       NVL (SUB2.EXT_SUBJECT_AREA_SID, 2147483646) EXT_SUBJECT_AREA_SID_2,               
       XDEG.EXT_CAREER, 
       NVL(X1.XLATSHORTNAME, '-')  EXT_CAREER_SD,
       NVL(X1.XLATLONGNAME, '-')  EXT_CAREER_LD,                     
       EXT_DATA_NBR,
       EXT_DEG_DT,
       EXT_DEG_STAT_ID,
       NVL(X2.XLATSHORTNAME, '-') EXT_DEG_STAT_SD,
       NVL(X2.XLATLONGNAME, '-') EXT_DEG_STAT_LD, 
       FIELD_OF_STUDY_1,
       FIELD_OF_STUDY_2,
       HONORS_CATEGORY, 
       NVL(X3.XLATSHORTNAME, '-') HONORS_CATEGORY_SD,
       NVL(X3.XLATLONGNAME, '-') HONORS_CATEGORY_LD,         
	   'N' LOAD_ERROR, 
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM, 
       1234 BATCH_SID
  FROM XDEG
  left outer join CSMRT_OWNER.PS_D_EXT_ORG ORG
    on XDEG.EXT_ORG_ID = ORG.EXT_ORG_ID
   and XDEG.SRC_SYS_ID = ORG.SRC_SYS_ID
   and ORG.DATA_ORIGIN <> 'D'    
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on XDEG.PERSON_ID = P.PERSON_ID
   and XDEG.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'  
  left outer join CSMRT_OWNER.PS_D_TST_DATA_SRC XDS
    on XDEG.LS_DATA_SOURCE = XDS.TST_DATA_SRC_ID
   and XDEG.SRC_SYS_ID = XDS.SRC_SYS_ID
   and XDS.DATA_ORIGIN <> 'D'  
  left outer join CSMRT_OWNER.PS_D_DEG DEG
    on XDEG.DEGREE = DEG.DEG_CD
   and XDEG.SRC_SYS_ID = DEG.SRC_SYS_ID
   and DEG.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.UM_D_EXT_SUBJECT_AREA SUB1
    on XDEG.EXT_SUBJ_AREA_1 = SUB1.EXT_SUBJECT_AREA
   and XDEG.SRC_SYS_ID = SUB1.SRC_SYS_ID
   and SUB1.DATA_ORIGIN <> 'D'  
  left outer join CSMRT_OWNER.UM_D_EXT_SUBJECT_AREA SUB2
    on XDEG.EXT_SUBJ_AREA_2 = SUB2.EXT_SUBJECT_AREA
   and XDEG.SRC_SYS_ID = SUB2.SRC_SYS_ID
   and SUB2.DATA_ORIGIN <> 'D'   
  left outer join CSMRT_OWNER.UM_D_XLATITEM X1
    on 'EXT_CAREER' = X1.FIELDNAME
   and XDEG.EXT_CAREER = X1.FIELDVALUE
   and XDEG.SRC_SYS_ID = X1.SRC_SYS_ID  
  left outer join CSMRT_OWNER.UM_D_XLATITEM X2
    on 'DEGREE_STATUS' = X2.FIELDNAME
   and XDEG.EXT_DEG_STAT_ID = X2.FIELDVALUE
   and XDEG.SRC_SYS_ID = X2.SRC_SYS_ID  
  left outer join CSMRT_OWNER.UM_D_XLATITEM X3
    on 'HONORS_CATEGORY' = X3.FIELDNAME
   and XDEG.HONORS_CATEGORY = X3.FIELDVALUE
   and XDEG.SRC_SYS_ID = X3.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_EXT_DEG rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_EXT_DEG',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_EXT_DEG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_EXT_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_EXT_DEG enable constraint PK_UM_F_EXT_DEG';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_EXT_DEG');

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

END UM_F_EXT_DEG_P;
/
