CREATE OR REPLACE PROCEDURE             "UM_F_SAA_ADB_CRSEAVL_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_SAA_ADB_CRSEAVL from PeopleSoft table UM_F_SAA_ADB_CRSEAVL.
--
 --V01  SMT-xxxx 06/19/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SAA_ADB_CRSEAVL';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SAA_ADB_CRSEAVL', TRUE);


strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL disable constraint PK_UM_F_SAA_ADB_CRSEAVL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL';				
insert /*+ append */ into UM_F_SAA_ADB_CRSEAVL
with RES as (
select /*+ parallel(8) inline */ 
       R.EMPLID, R.ANALYSIS_DB_SEQ, R.SAA_CAREER_RPT, R.SAA_ENTRY_SEQ, R.SRC_SYS_ID, 
       R.INSTITUTION_CD, T.TERM_CD
  from UM_F_SAA_ADB_RESULTS R
  join PS_D_TERM T
    on R.TERM_SID = T.TERM_SID
 where R.CRSES_REQUIRED < 999
   and R.ITEM_R_STATUS in ('FAIL')
   and R.TSCRPT_TYPE in ('DADV','LADV')),
T_AVL as (   
select /*+ parallel(8) inline */ distinct
       EMPLID, ANALYSIS_DB_SEQ, SAA_CAREER_RPT, SAA_ENTRY_SEQ, SAA_COURSE_SEQ, SRC_SYS_ID, 
       INSTITUTION_CD, CRSE_SID, PLAN_TERM_SID, COURSE_LIST, R_COURSE_SEQUENCE, SUBJECT, CATALOG_NBR, 
       CRSE_ID, CRSE_OFFER_NUM, CRSE_RQRMNT_GROUP, CO_REQ_ONLY_FLG, CRS_TOPIC_ID, EARN_CREDIT, PRE_REQ_MET_FLG
  from UM_T1_SAA_ADB_CRSEAVL T1
 union all 
select /*+ parallel(8) inline */ distinct
       EMPLID, ANALYSIS_DB_SEQ, SAA_CAREER_RPT, SAA_ENTRY_SEQ, SAA_COURSE_SEQ, SRC_SYS_ID, 
       INSTITUTION_CD, CRSE_SID, PLAN_TERM_SID, COURSE_LIST, R_COURSE_SEQUENCE, SUBJECT, CATALOG_NBR, 
       CRSE_ID, CRSE_OFFER_NUM, CRSE_RQRMNT_GROUP, CO_REQ_ONLY_FLG, CRS_TOPIC_ID, EARN_CREDIT, PRE_REQ_MET_FLG
  from UM_T2_SAA_ADB_CRSEAVL T1
 union all 
select /*+ parallel(8) inline */ distinct
       EMPLID, ANALYSIS_DB_SEQ, SAA_CAREER_RPT, SAA_ENTRY_SEQ, SAA_COURSE_SEQ, SRC_SYS_ID, 
       INSTITUTION_CD, CRSE_SID, PLAN_TERM_SID, COURSE_LIST, R_COURSE_SEQUENCE, SUBJECT, CATALOG_NBR, 
       CRSE_ID, CRSE_OFFER_NUM, CRSE_RQRMNT_GROUP, CO_REQ_ONLY_FLG, CRS_TOPIC_ID, EARN_CREDIT, PRE_REQ_MET_FLG
  from UM_T3_SAA_ADB_CRSEAVL T1
 union all 
select /*+ parallel(8) inline */ distinct
       EMPLID, ANALYSIS_DB_SEQ, SAA_CAREER_RPT, SAA_ENTRY_SEQ, SAA_COURSE_SEQ, SRC_SYS_ID, 
       INSTITUTION_CD, CRSE_SID, PLAN_TERM_SID, COURSE_LIST, R_COURSE_SEQUENCE, SUBJECT, CATALOG_NBR, 
       CRSE_ID, CRSE_OFFER_NUM, CRSE_RQRMNT_GROUP, CO_REQ_ONLY_FLG, CRS_TOPIC_ID, EARN_CREDIT, PRE_REQ_MET_FLG
  from UM_T4_SAA_ADB_CRSEAVL T1),   
AVL1 as (
select /*+ parallel(8) inline */
       T.EMPLID, 
       T.ANALYSIS_DB_SEQ, 
       T.SAA_CAREER_RPT, 
       T.SAA_ENTRY_SEQ, 
       T.SAA_COURSE_SEQ, 
       T.SRC_SYS_ID, 
       T.INSTITUTION_CD,
       RES.TERM_CD, 
       T.CRSE_SID, 
       T.PLAN_TERM_SID, 
       T.COURSE_LIST, 
       T.R_COURSE_SEQUENCE, 
       T.SUBJECT, 
       T.CATALOG_NBR, 
       T.CRSE_ID,
       T.CRSE_OFFER_NUM, 
       T.CRS_TOPIC_ID, 
       T.EARN_CREDIT, 
       (case when T.PRE_REQ_MET_FLG not in ('Y')
             then T.PRE_REQ_MET_FLG
             when T.CO_REQ_ONLY_FLG not in ('Y','N')
             then T.PRE_REQ_MET_FLG
             else GET_PRE_REQ_MET_FLG(trim(replace(replace(replace(G.SQL_STR_CO,'AAAAA',T.EMPLID),22222,T.ANALYSIS_DB_SEQ),'CCCCC',T.SAA_CAREER_RPT))) 
         end) PRE_REQ_MET_FLG, 
       count(distinct case when PLAN_TERM_SID >= 2147483646 then PLAN_TERM_SID else NULL end) over (partition by T.EMPLID, T.ANALYSIS_DB_SEQ, T.SAA_CAREER_RPT, T.CRSE_SID) DEMAND_COUNT, 
       count(distinct case when PLAN_TERM_SID <  2147483646 then PLAN_TERM_SID else NULL end) over (partition by T.EMPLID, T.ANALYSIS_DB_SEQ, T.SAA_CAREER_RPT, T.CRSE_SID) PLAN_COUNT, 
       SYSDATE CREATED_EW_DTTM
--  from UM_T_SAA_ADB_CRSEAVL T
  from T_AVL T
  join RES
    on RES.EMPLID = T.EMPLID
   and RES.ANALYSIS_DB_SEQ = T.ANALYSIS_DB_SEQ
   and RES.SAA_CAREER_RPT = T.SAA_CAREER_RPT
   and RES.SAA_ENTRY_SEQ = T.SAA_ENTRY_SEQ
   and RES.SRC_SYS_ID = T.SRC_SYS_ID
  left outer join CSMRT_OWNER.UM_D_RQ_GRP G   
    on T.CRSE_RQRMNT_GROUP = G.RQRMNT_GROUP
   and T.SRC_SYS_ID = G.SRC_SYS_ID
   and G.DATA_ORIGIN <> 'D'),
ENRL as (
select /*+ parallel(8) inline */ E.INSTITUTION, E.ACAD_CAREER, E.EMPLID, C.SUBJECT, C.CRSE_ID, max(E.STRM) MAX_STRM 
  from CSSTG_OWNER.PS_STDNT_ENRL E
  join CSSTG_OWNER.PS_CLASS_TBL C
    on E.INSTITUTION = C.INSTITUTION
   and E.ACAD_CAREER = C.ACAD_CAREER 
   and E.STRM = C.STRM
   and E.CLASS_NBR = C.CLASS_NBR
   and E.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
 where E.DATA_ORIGIN <> 'D'
   and E.STDNT_ENRL_STATUS = 'E'
   and E.STRM >= '1010'
 group by E.INSTITUTION, E.ACAD_CAREER, E.EMPLID, C.SUBJECT, C.CRSE_ID)
select /*+ parallel(8) */
       AVL1.EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       SAA_ENTRY_SEQ, 
       SAA_COURSE_SEQ, 
       SRC_SYS_ID, 
       INSTITUTION_CD, 
       CRSE_SID, 
       PLAN_TERM_SID, 
       COURSE_LIST, 
       R_COURSE_SEQUENCE, 
       AVL1.SUBJECT, 
       CATALOG_NBR, 
       AVL1.CRSE_ID, 
       CRSE_OFFER_NUM, 
       CRS_TOPIC_ID, 
       EARN_CREDIT, 
       PRE_REQ_MET_FLG,
       (case when AVL1.TERM_CD <= ENRL.MAX_STRM then 'Y' else 'N' end) ENRL_FLG, 
       DEMAND_COUNT, 
       PLAN_COUNT, 
       CREATED_EW_DTTM
  from AVL1
  left outer join ENRL
    on AVL1.INSTITUTION_CD = ENRL.INSTITUTION 
   and AVL1.SAA_CAREER_RPT = ENRL.ACAD_CAREER 
   and AVL1.EMPLID = ENRL.EMPLID 
   and AVL1.SUBJECT = ENRL.SUBJECT 
   and AVL1.CRSE_ID = ENRL.CRSE_ID 
 where not (PLAN_COUNT > 0 and DEMAND_COUNT > 0 and PLAN_TERM_SID = 2147483646)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SAA_ADB_CRSEAVL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_CRSEAVL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_CRSEAVL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL enable constraint PK_UM_F_SAA_ADB_CRSEAVL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SAA_ADB_CRSEAVL');
/*
Materialized View logic goes here.
*/

strMessage01    := 'Altering Session Enabling Parallel DML';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

-- ALTER SESSION ENABLE PARALLEL DML;
strSqlDynamic   := 'ALTER SESSION ENABLE PARALLEL DML';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

				
--ALTER MATERIALIZED VIEW UM_F_SAA_RES_CRSEAVL_MV
--PARALLEL 8
--DISABLE QUERY REWRITE;
strMessage01    := 'Disabling Query Rewrite on Materialized View CSMRT_OWNER.UM_F_SAA_RES_CRSEAVL_MV';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);				
strSqlDynamic   := 'ALTER MATERIALIZED VIEW CSMRT_OWNER.UM_F_SAA_RES_CRSEAVL_MV PARALLEL 8 DISABLE QUERY REWRITE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Updating Materialized View CSMRT_OWNER.UM_F_SAA_RES_CRSEAVL_MV for table CSMRT_OWNER.UM_F_SAA_ADB_CRSEAVL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);				

DBMS_MVIEW.REFRESH('UM_F_SAA_RES_CRSEAVL_MV','C', ATOMIC_REFRESH => FALSE);


--ALTER MATERIALIZED VIEW UM_F_SAA_RES_CRSEAVL_MV 
--NOPARALLEL
--ENABLE QUERY REWRITE;
strMessage01    := 'Enabling Query Rewrite on Materialized View CSMRT_OWNER.UM_F_SAA_RES_CRSEAVL_MV';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);				
strSqlDynamic   := 'ALTER MATERIALIZED VIEW CSMRT_OWNER.UM_F_SAA_RES_CRSEAVL_MV NOPARALLEL ENABLE QUERY REWRITE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
/*
End of Materialized View logic.
*/



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

END UM_F_SAA_ADB_CRSEAVL_P;
/
