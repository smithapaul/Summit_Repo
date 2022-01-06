CREATE OR REPLACE PROCEDURE             "UM_F_SAA_ADB_COND_CO_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_SAA_ADB_COND from PeopleSoft table UM_F_SAA_ADB_COND.
--
 --V01  SMT-xxxx 06/21/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SAA_ADB_COND';
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

strMessage01    := 'Gathering table Stats CSMRT_OWNER.UM_T(n)_SAA_ADB_CRSEAVL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

DBMS_STATS.gather_table_stats(ownname => 'CSMRT_OWNER', tabname => 'UM_T1_SAA_ADB_CRSEAVL' , cascade => TRUE, degree => 8, estimate_percent => DBMS_STATS.auto_sample_size, method_opt => 'FOR ALL COLUMNS SIZE 1' );

DBMS_STATS.gather_table_stats(ownname => 'CSMRT_OWNER', tabname => 'UM_T2_SAA_ADB_CRSEAVL' , cascade => TRUE, degree => 8, estimate_percent => DBMS_STATS.auto_sample_size, method_opt => 'FOR ALL COLUMNS SIZE 1' );

DBMS_STATS.gather_table_stats(ownname => 'CSMRT_OWNER', tabname => 'UM_T3_SAA_ADB_CRSEAVL' , cascade => TRUE, degree => 8, estimate_percent => DBMS_STATS.auto_sample_size, method_opt => 'FOR ALL COLUMNS SIZE 1' );

DBMS_STATS.gather_table_stats(ownname => 'CSMRT_OWNER', tabname => 'UM_T4_SAA_ADB_CRSEAVL' , cascade => TRUE, degree => 8, estimate_percent => DBMS_STATS.auto_sample_size, method_opt => 'FOR ALL COLUMNS SIZE 1' );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_COND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SAA_ADB_COND', TRUE);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_COND disable constraint PK_UM_F_SAA_ADB_COND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SAA_ADB_COND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SAA_ADB_COND';				

insert /*+ append */ into UM_F_SAA_ADB_COND
with
GRP1 as (
select /*+ parallel(8) inline */ 
       RQRMNT_GROUP, EFFDT, SRC_SYS_ID,
       row_number() over (partition by RQRMNT_GROUP, SRC_SYS_ID order by EFFDT desc) RQ_GRP_ORDER
  from CSSTG_OWNER.PS_RQ_GRP_TBL
 where DATA_ORIGIN <> 'D'),
GRP2 as (
select /*+ parallel(8) inline */ distinct 
       GRP1.RQRMNT_GROUP, DTL.CRSE_ID
  from GRP1
  join CSSTG_OWNER.PS_RQ_GRP_DETL_TBL DTL
    on GRP1.RQRMNT_GROUP = DTL.RQRMNT_GROUP
   and GRP1.EFFDT = DTL.EFFDT
   and GRP1.SRC_SYS_ID = DTL.SRC_SYS_ID
   and DTL.DATA_ORIGIN <> 'D'
   and GRP1.RQ_GRP_ORDER = 1
   and DTL.RQ_GRP_LINE_TYPE = 'CRSE'
   and DTL.REQUISITE_TYPE = 'CO'),
AVL as (   
select /*+ parallel(8) inline */ distinct
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       CRSE_ID, 
       SRC_SYS_ID, 
       INSTITUTION_CD,
       CRSE_RQRMNT_GROUP
  from UM_T1_SAA_ADB_CRSEAVL T1
 where PRE_REQ_MET_FLG = 'Y'      -- Filter anything else???    -- ROWNUM filter??? 
 union all 
select /*+ parallel(8) inline */ distinct
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       CRSE_ID, 
       SRC_SYS_ID, 
       INSTITUTION_CD,
       CRSE_RQRMNT_GROUP
  from UM_T2_SAA_ADB_CRSEAVL T1
 where PRE_REQ_MET_FLG = 'Y'
 union all 
select /*+ parallel(8) inline */ distinct
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       CRSE_ID, 
       SRC_SYS_ID, 
       INSTITUTION_CD,
       CRSE_RQRMNT_GROUP
  from UM_T3_SAA_ADB_CRSEAVL T1
 where PRE_REQ_MET_FLG = 'Y'
 union all 
select /*+ parallel(8) inline */ distinct
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       CRSE_ID, 
       SRC_SYS_ID, 
       INSTITUTION_CD,
       CRSE_RQRMNT_GROUP
  from UM_T4_SAA_ADB_CRSEAVL T1
 where PRE_REQ_MET_FLG = 'Y'), 
PRE as (
select /*+ parallel(8) inline */ distinct
       T1.EMPLID, 
       T1.ANALYSIS_DB_SEQ, 
       T1.SAA_CAREER_RPT, 
       'CRSE' COND_CODE, 
       GRP2.CRSE_ID COND_DATA, 
       T1.SRC_SYS_ID, 
       T1.INSTITUTION_CD 
--  from UM_T_SAA_ADB_CRSEAVL T1
  from AVL T1
  join GRP2
    on T1.CRSE_RQRMNT_GROUP = GRP2.RQRMNT_GROUP
--  join UM_T_SAA_ADB_CRSEAVL T2
  join AVL T2
    on T1.EMPLID = T2.EMPLID 
   and T1.ANALYSIS_DB_SEQ = T2.ANALYSIS_DB_SEQ
   and T1.SAA_CAREER_RPT = T2.SAA_CAREER_RPT
   and GRP2.CRSE_ID = T2.CRSE_ID
   and T1.SRC_SYS_ID = T2.SRC_SYS_ID
 minus
select /*+ parallel(8) inline */ 
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       COND_CODE, 
       COND_DATA, 
       SRC_SYS_ID, 
       INSTITUTION_CD 
  from UM_F_SAA_ADB_COND)
select /*+ parallel(8) inline */ 
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       COND_CODE, 
       COND_DATA, 
       SRC_SYS_ID, 
       INSTITUTION_CD, 
       SYSDATE CREATED_EW_DTTM
  from PRE 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SAA_ADB_COND rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_COND',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_COND',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_COND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_COND enable constraint PK_UM_F_SAA_ADB_COND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SAA_ADB_COND');

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

END UM_F_SAA_ADB_COND_CO_P;
/
