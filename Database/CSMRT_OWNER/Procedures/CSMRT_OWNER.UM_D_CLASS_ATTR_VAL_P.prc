DROP PROCEDURE CSMRT_OWNER.UM_D_CLASS_ATTR_VAL_P
/

--
-- UM_D_CLASS_ATTR_VAL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_CLASS_ATTR_VAL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_D_CLASS_ATTR_VAL from PeopleSoft table UM_D_CLASS_ATTR_VAL.
--
 --V01  SMT-xxxx 03/23/2018,    Srikanth,Pabbu
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CLASS_ATTR_VAL';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_CLASS_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_CLASS_ATTR_VAL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_CLASS_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_CLASS_ATTR_VAL');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_ATTR_VAL disable constraint PK_UM_D_CLASS_ATTR_VAL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_CLASS_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_CLASS_ATTR_VAL';				 
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_D_CLASS_ATTR_VAL 
 with Q1 as (  
select /*+ INLINE PARALLEL(8) */
       CRSE_ATTR, SRC_SYS_ID, EFFDT, EFF_STATUS, 
       DESCRSHORT CRSE_ATTR_SD, DESCR CRSE_ATTR_LD,  
       DATA_ORIGIN,
       row_number() over (partition by CRSE_ATTR, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_CRSE_ATTR_TBL
 where DATA_ORIGIN <> 'D'), 
       Q2 as (  
select /*+ INLINE PARALLEL(8) */
       CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID, EFFDT, 
       DESCR CRSE_ATTR_VALUE_LD, DESCRFORMAL CRSE_ATTR_VALUE_FD, 
       CATALOG_PRINT CATALOG_PRINT_FLG, SCHEDULE_PRINT SCHEDULE_PRINT_FLG, 
       DATA_ORIGIN,
       row_number() over (partition by CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_CRSE_ATTR_VALUE
 where DATA_ORIGIN <> 'D'), 
       Q3 as (  
select /*+ INLINE PARALLEL(8) */
       CRSE_ID CRSE_CD, CRSE_OFFER_NBR CRSE_OFFER_NUM, STRM TERM_CD, SESSION_CODE SESSION_CD, CLASS_SECTION CLASS_SECTION_CD, 
       CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID,  
       DATA_ORIGIN
  from CSSTG_OWNER.PS_CLASS_ATTRIBUTE
 where DATA_ORIGIN <> 'D'), 
       S as (
select /*+ INLINE PARALLEL(8) */
       C.CRSE_CD, C.CRSE_OFFER_NUM, C.TERM_CD, C.SESSION_CD, C.CLASS_SECTION_CD, 
       nvl(Q3.CRSE_ATTR,'-') CRSE_ATTR, nvl(Q3.CRSE_ATTR_VALUE,'-') CRSE_ATTR_VALUE, C.SRC_SYS_ID, 
       C.INSTITUTION_CD, C.CLASS_SID, 
       Q1.CRSE_ATTR_SD CRSE_ATTR_SD, Q1.CRSE_ATTR_LD CRSE_ATTR_LD, 
       Q2.CRSE_ATTR_VALUE_LD CRSE_ATTR_VALUE_LD, Q2.CRSE_ATTR_VALUE_FD CRSE_ATTR_VALUE_FD, 
       Q2.CATALOG_PRINT_FLG CATALOG_PRINT_FLG, Q2.SCHEDULE_PRINT_FLG SCHEDULE_PRINT_FLG, 
       C.DATA_ORIGIN DATA_ORIGIN  
  from CSMRT_OWNER.UM_D_CLASS C
  left outer join Q3
    on C.CRSE_CD = Q3.CRSE_CD
   and C.CRSE_OFFER_NUM = Q3.CRSE_OFFER_NUM
   and C.TERM_CD = Q3.TERM_CD
   and C.SESSION_CD = Q3.SESSION_CD
   and C.CLASS_SECTION_CD = Q3.CLASS_SECTION_CD
   and C.SRC_SYS_ID = Q3.SRC_SYS_ID
  left outer join Q1
    on Q3.CRSE_ATTR = Q1.CRSE_ATTR
   and Q3.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q_ORDER = 1
  left outer join Q2
    on Q3.CRSE_ATTR = Q2.CRSE_ATTR
   and Q3.CRSE_ATTR_VALUE = Q2.CRSE_ATTR_VALUE
   and Q3.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
    )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
select ROWNUM CLASS_ATTR_VAL_SID, 
       CRSE_CD, CRSE_OFFER_NUM, TERM_CD, SESSION_CD, CLASS_SECTION_CD, CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID, 
       INSTITUTION_CD, CLASS_SID, CRSE_ATTR_SD, CRSE_ATTR_LD, CRSE_ATTR_VALUE_LD, CRSE_ATTR_VALUE_FD, CATALOG_PRINT_FLG, SCHEDULE_PRINT_FLG, 
       DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from S
;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_ATTR_VAL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_ATTR_VAL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_CLASS_ATTR_VAL';				
insert into CSMRT_OWNER.UM_D_CLASS_ATTR_VAL 
select /*+ INLINE PARALLEL(8) */ 
-1 CLASS_ATTR_VAL_SID, 
'-' CRSE_CD, 
0 CRSE_OFFER_NUM, 
'-' TERM_CD, 
'-' SESSION_CD, 
'-' CLASS_SECTION_CD, 
'-' CRSE_ATTR, 
'-' CRSE_ATTR_VALUE, 
'CS90' SRC_SYS_ID, 
'-' INSTITUTION_CD, 
2147483646 CLASS_SID, 
'' CRSE_ATTR_SD, 
'' CRSE_ATTR_LD, 
'' CRSE_ATTR_VALUE_LD, 
'' CRSE_ATTR_VALUE_FD, 
'' CATALOG_PRINT_FLG, 
'' SCHEDULE_PRINT_FLG, 
'S' DATA_ORIGIN, 
SYSDATE CREATED_EW_DTTM, 
SYSDATE LASTUPD_EW_DTTM
  from DUAL
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_ATTR_VAL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_ATTR_VAL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_CLASS_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_ATTR_VAL enable constraint PK_UM_D_CLASS_ATTR_VAL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_CLASS_ATTR_VAL');

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

END UM_D_CLASS_ATTR_VAL_P;
/
