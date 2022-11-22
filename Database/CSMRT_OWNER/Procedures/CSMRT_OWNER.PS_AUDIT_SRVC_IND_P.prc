DROP PROCEDURE CSMRT_OWNER.PS_AUDIT_SRVC_IND_P
/

--
-- PS_AUDIT_SRVC_IND_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_AUDIT_SRVC_IND_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_AUDIT_SRVC_IND from PeopleSoft table PS_AUDIT_SRVC_IND.
--
-- V01  SMT-6999 07/13/2017,    Jim Doucette
--                              Converted from PS_AUDIT_SRVC_IND.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_AUDIT_SRVC_IND';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_AUDIT_SRVC_IND'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_AUDIT_SRVC_IND@SASOURCE S)
 where TABLE_NAME = 'PS_AUDIT_SRVC_IND'
;

strSqlCommand := 'commit';
commit;

strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_AUDIT_SRVC_IND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_AUDIT_SRVC_IND'
;

strSqlCommand := 'commit';
commit;

strSqlCommand := 'insert';
INSERT /*+ append parallel(8) enable_parallel_dml */  into CSSTG_OWNER.PS_T_AUDIT_SRVC_IND
SELECT /*+ full(S) */
       nvl(trim(AUDIT_OPRID),'-'), 
       AUDIT_STAMP,
       nvl(trim(AUDIT_ACTN),'-'), 
       nvl(trim(EMPLID),'-'), 
       nvl(trim(EXT_ORG_ID),'-'),
       SRVC_IND_DTTM,
       trim(OPRID),
       trim(INSTITUTION),
       trim(SRVC_IND_CD),
       trim(SRVC_IND_REASON),
       trim(SRVC_IND_ACT_TERM),
       trim(SCC_SI_END_TERM),
       SCC_SI_END_DT,
       SRVC_IND_ACTIVE_DT,
       trim(POS_SRVC_INDICATOR),
       trim(SRVC_IND_REFRNCE),
       trim(DEPTID),
       POSITION_NBR,
       trim(CONTACT),
       trim(CONTACT_ID),
       trim(CURRENCY_CD),
       AMOUNT,
       SEQ_3C,
       trim(PLACED_METHOD),
       trim(PLACED_PERSON),
       trim(PLACED_PERSON_ID),
       trim(PLACED_PROCESS),
       trim(RELEASE_PROCESS),
       PROCESS_INSTANCE,
       to_char(substr(trim(COMM_COMMENTS), 1, 4000)) COMM_COMMENTS,
       to_number(ORA_ROWSCN) SRC_SCN
  FROM SYSADM.PS_AUDIT_SRVC_IND@SASOURCE S
 WHERE SRVC_IND_DTTM IS NOT NULL 
   AND TRIM (EMPLID) IS NOT NULL;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_AUDIT_SRVC_IND'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into CSSTG_OWNER.PS_AUDIT_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_AUDIT_SRVC_IND';
merge /*+ use_hash(S,T) parallel(8) enable_parallel_dml */ into CSSTG_OWNER.PS_AUDIT_SRVC_IND T 
using (WITH SRVC
      AS (SELECT /*+ inline parallel(8) */
                AUDIT_OPRID, 
                AUDIT_STAMP,
                AUDIT_ACTN,
                EMPLID,
                EXT_ORG_ID,
                SRVC_IND_DTTM,
                OPRID,
                INSTITUTION,
                SRVC_IND_CD,
                SRVC_IND_REASON,
                SRVC_IND_ACT_TERM,
                SCC_SI_END_TERM,
                SCC_SI_END_DT,
                SRVC_IND_ACTIVE_DT,
                POS_SRVC_INDICATOR,
                SRVC_IND_REFRNCE,
                DEPTID,
                POSITION_NBR,
                CONTACT,
                CONTACT_ID,
                CURRENCY_CD,
                AMOUNT,
                SEQ_3C,
                PLACED_METHOD,
                PLACED_PERSON,
                PLACED_PERSON_ID,
                PLACED_PROCESS,
                RELEASE_PROCESS,
                PROCESS_INSTANCE,
                COMM_COMMENTS,
                SRC_SCN,
                ROW_NUMBER ()
                   OVER (PARTITION BY AUDIT_OPRID, AUDIT_STAMP, AUDIT_ACTN, EMPLID, SRVC_IND_DTTM ORDER BY SRC_SCN desc, AUDIT_STAMP desc)
                   SRVC_ORDER
           FROM CSSTG_OWNER.PS_T_AUDIT_SRVC_IND 
           ) 
SELECT /*+ inline parallel(8) */
       AUDIT_OPRID,
       AUDIT_STAMP,
       AUDIT_ACTN,
       EMPLID,
       EXT_ORG_ID,
       SRVC_IND_DTTM,
       OPRID,
       INSTITUTION,
       SRVC_IND_CD,
       SRVC_IND_REASON,
       SRVC_IND_ACT_TERM,
       SCC_SI_END_TERM,
       SCC_SI_END_DT,
       SRVC_IND_ACTIVE_DT,
       POS_SRVC_INDICATOR,
       SRVC_IND_REFRNCE,
       DEPTID,
       POSITION_NBR,
       CONTACT,
       CONTACT_ID,
       CURRENCY_CD,
       AMOUNT,
       SEQ_3C,
       PLACED_METHOD,
       PLACED_PERSON,
       PLACED_PERSON_ID,
       PLACED_PROCESS,
       RELEASE_PROCESS,
       PROCESS_INSTANCE,
       COMM_COMMENTS,
       SRC_SCN             
  FROM SRVC
 WHERE SRVC_ORDER = 1
   AND SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_AUDIT_SRVC_IND')  ) S 
 on ( 
    T.AUDIT_OPRID = S.AUDIT_OPRID and 
    T.AUDIT_STAMP = S.AUDIT_STAMP and 
    T.AUDIT_ACTN = S.AUDIT_ACTN and 
    T.EMPLID = S.EMPLID and 
    T.SRVC_IND_DTTM = S.SRVC_IND_DTTM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EXT_ORG_ID = S.EXT_ORG_ID,
    T.OPRID = S.OPRID,
    T.INSTITUTION = S.INSTITUTION,
    T.SRVC_IND_CD = S.SRVC_IND_CD,
    T.SRVC_IND_REASON = S.SRVC_IND_REASON,
    T.SRVC_IND_ACT_TERM = S.SRVC_IND_ACT_TERM,
    T.SCC_SI_END_TERM = S.SCC_SI_END_TERM,
    T.SCC_SI_END_DT = S.SCC_SI_END_DT,
    T.SRVC_IND_ACTIVE_DT = S.SRVC_IND_ACTIVE_DT,
    T.POS_SRVC_INDICATOR = S.POS_SRVC_INDICATOR,
    T.SRVC_IND_REFRNCE = S.SRVC_IND_REFRNCE,
    T.DEPTID = S.DEPTID,
    T.POSITION_NBR = S.POSITION_NBR,
    T.CONTACT = S.CONTACT,
    T.CONTACT_ID = S.CONTACT_ID,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.AMOUNT = S.AMOUNT,
    T.SEQ_3C = S.SEQ_3C,
    T.PLACED_METHOD = S.PLACED_METHOD,
    T.PLACED_PERSON = S.PLACED_PERSON,
    T.PLACED_PERSON_ID = S.PLACED_PERSON_ID,
    T.PLACED_PROCESS = S.PLACED_PROCESS,
    T.RELEASE_PROCESS = S.RELEASE_PROCESS,
    T.PROCESS_INSTANCE = S.PROCESS_INSTANCE,
    T.COMM_COMMENTS = S.COMM_COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EXT_ORG_ID <> S.EXT_ORG_ID or 
    nvl(trim(T.OPRID),0) <> nvl(trim(S.OPRID),0) or 
    nvl(trim(T.INSTITUTION),0) <> nvl(trim(S.INSTITUTION),0) or 
    nvl(trim(T.SRVC_IND_CD),0) <> nvl(trim(S.SRVC_IND_CD),0) or 
    nvl(trim(T.SRVC_IND_REASON),0) <> nvl(trim(S.SRVC_IND_REASON),0) or 
    nvl(trim(T.SRVC_IND_ACT_TERM),0) <> nvl(trim(S.SRVC_IND_ACT_TERM),0) or 
    nvl(trim(T.SCC_SI_END_TERM),0) <> nvl(trim(S.SCC_SI_END_TERM),0) or 
    nvl(trim(T.SCC_SI_END_DT),0) <> nvl(trim(S.SCC_SI_END_DT),0) or 
    nvl(trim(T.SRVC_IND_ACTIVE_DT),0) <> nvl(trim(S.SRVC_IND_ACTIVE_DT),0) or 
    nvl(trim(T.POS_SRVC_INDICATOR),0) <> nvl(trim(S.POS_SRVC_INDICATOR),0) or 
    nvl(trim(T.SRVC_IND_REFRNCE),0) <> nvl(trim(S.SRVC_IND_REFRNCE),0) or 
    nvl(trim(T.DEPTID),0) <> nvl(trim(S.DEPTID),0) or 
    nvl(trim(T.POSITION_NBR),0) <> nvl(trim(S.POSITION_NBR),0) or 
    nvl(trim(T.CONTACT),0) <> nvl(trim(S.CONTACT),0) or 
    nvl(trim(T.CONTACT_ID),0) <> nvl(trim(S.CONTACT_ID),0) or 
    nvl(trim(T.CURRENCY_CD),0) <> nvl(trim(S.CURRENCY_CD),0) or 
    nvl(trim(T.AMOUNT),0) <> nvl(trim(S.AMOUNT),0) or 
    nvl(trim(T.SEQ_3C),0) <> nvl(trim(S.SEQ_3C),0) or 
    nvl(trim(T.PLACED_METHOD),0) <> nvl(trim(S.PLACED_METHOD),0) or 
    nvl(trim(T.PLACED_PERSON),0) <> nvl(trim(S.PLACED_PERSON),0) or 
    nvl(trim(T.PLACED_PERSON_ID),0) <> nvl(trim(S.PLACED_PERSON_ID),0) or 
    nvl(trim(T.PLACED_PROCESS),0) <> nvl(trim(S.PLACED_PROCESS),0) or 
    nvl(trim(T.RELEASE_PROCESS),0) <> nvl(trim(S.RELEASE_PROCESS),0) or 
    nvl(trim(T.PROCESS_INSTANCE),0) <> nvl(trim(S.PROCESS_INSTANCE),0) or 
    nvl(trim(T.COMM_COMMENTS),0) <> nvl(trim(S.COMM_COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.AUDIT_OPRID,
    T.AUDIT_STAMP,
    T.AUDIT_ACTN, 
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.SRVC_IND_DTTM,
    T.SRC_SYS_ID, 
    T.OPRID,
    T.INSTITUTION,
    T.SRVC_IND_CD,
    T.SRVC_IND_REASON,
    T.SRVC_IND_ACT_TERM,
    T.SCC_SI_END_TERM,
    T.SCC_SI_END_DT,
    T.SRVC_IND_ACTIVE_DT, 
    T.POS_SRVC_INDICATOR, 
    T.SRVC_IND_REFRNCE, 
    T.DEPTID, 
    T.POSITION_NBR, 
    T.CONTACT,
    T.CONTACT_ID, 
    T.CURRENCY_CD,
    T.AMOUNT, 
    T.SEQ_3C, 
    T.PLACED_METHOD,
    T.PLACED_PERSON,
    T.PLACED_PERSON_ID, 
    T.PLACED_PROCESS, 
    T.RELEASE_PROCESS,
    T.PROCESS_INSTANCE, 
    T.COMM_COMMENTS,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.AUDIT_OPRID,
    S.AUDIT_STAMP,
    S.AUDIT_ACTN, 
    S.EMPLID, 
    S.EXT_ORG_ID, 
    S.SRVC_IND_DTTM,
    'CS90', 
    S.OPRID,
    S.INSTITUTION,
    S.SRVC_IND_CD,
    S.SRVC_IND_REASON,
    S.SRVC_IND_ACT_TERM,
    S.SCC_SI_END_TERM,
    S.SCC_SI_END_DT,
    S.SRVC_IND_ACTIVE_DT, 
    S.POS_SRVC_INDICATOR, 
    S.SRVC_IND_REFRNCE, 
    S.DEPTID, 
    S.POSITION_NBR, 
    S.CONTACT,
    S.CONTACT_ID, 
    S.CURRENCY_CD,
    S.AMOUNT, 
    S.SEQ_3C, 
    S.PLACED_METHOD,
    S.PLACED_PERSON,
    S.PLACED_PERSON_ID, 
    S.PLACED_PROCESS, 
    S.RELEASE_PROCESS,
    S.PROCESS_INSTANCE, 
    S.COMM_COMMENTS,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);
    
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_AUDIT_SRVC_IND rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_AUDIT_SRVC_IND',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_AUDIT_SRVC_IND';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_AUDIT_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_AUDIT_SRVC_IND';
update /*+ parallel(8) enable_parallel_dml */ CSSTG_OWNER.PS_AUDIT_SRVC_IND T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select /*+ parallel(8) */
        AUDIT_OPRID, AUDIT_STAMP, AUDIT_ACTN, EMPLID, SRVC_IND_DTTM
   from CSSTG_OWNER.PS_AUDIT_SRVC_IND T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_AUDIT_SRVC_IND') = 'Y'
  minus
 select /*+ parallel(8) */
        AUDIT_OPRID, AUDIT_STAMP, AUDIT_ACTN, EMPLID, SRVC_IND_DTTM
   from CSSTG_OWNER.PS_T_AUDIT_SRVC_IND S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_AUDIT_SRVC_IND') = 'Y'
   ) S
 where T.AUDIT_OPRID = S.AUDIT_OPRID 
   and T.AUDIT_STAMP = S.AUDIT_STAMP
   and T.AUDIT_ACTN = S.AUDIT_ACTN
   and T.EMPLID = S.EMPLID
   and T.SRVC_IND_DTTM = S.SRVC_IND_DTTM
   and T.SRC_SYS_ID = 'CS90') 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_AUDIT_SRVC_IND rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_AUDIT_SRVC_IND',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_AUDIT_SRVC_IND'
;

strSqlCommand := 'commit';
commit;

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

END PS_AUDIT_SRVC_IND_P;
/
