DROP PROCEDURE CSMRT_OWNER.PS_COMMUNICATION_P
/

--
-- PS_COMMUNICATION_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_COMMUNICATION_P AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_COMMUNICATION'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_COMMUNICATION', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_COMMUNICATION'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_COMMUNICATION from PeopleSoft table PS_COMMUNICATION.
--
-- V01  SMT-xxxx 05/30/2017,    Jim Doucette
--                              Converted from PS_COMMUNICATION.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_COMMUNICATION';
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
 where TABLE_NAME = 'PS_COMMUNICATION'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_COMMUNICATION@SASOURCE S)
 where TABLE_NAME = 'PS_COMMUNICATION'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_COMMUNICATION';
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
 where TABLE_NAME = 'PS_COMMUNICATION'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_COMMUNICATION
select /*+ full(S) */
    nvl(trim(COMMON_ID),'-') COMMON_ID,
    nvl(SEQ_3C,0) SEQ_3C,
    'CS90' SRC_SYS_ID,
    nvl(trim(SA_ID_TYPE),'-') SA_ID_TYPE,
    nvl(COMM_DTTM, to_date('01-JAN-1900')) COMM_DTTM,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(trim(ADMIN_FUNCTION),'-') ADMIN_FUNCTION,
    nvl(trim(COMM_CATEGORY),'-') COMM_CATEGORY,
    nvl(trim(COMM_CONTEXT),'-') COMM_CONTEXT,
    nvl(trim(COMM_METHOD),'-') COMM_METHOD,
    nvl(trim(INCLUDE_ENCL),'-') INCLUDE_ENCL,
    nvl(trim(DEPTID),'-') DEPTID,
    nvl(trim(COMM_ID),'-') COMM_ID,
    COMM_DT,
    COMM_BEGIN_TM,
    COMM_END_TM,
    nvl(trim(COMPLETED_COMM),'-') COMPLETED_COMM,
    nvl(trim(COMPLETED_ID),'-') COMPLETED_ID,
    COMPLETED_DT,
    nvl(trim(COMM_DIRECTION),'-') COMM_DIRECTION,
    nvl(trim(UNSUCCESSFUL),'-') UNSUCCESSFUL,
    nvl(trim(OUTCOME_REASON),'-') OUTCOME_REASON,
    nvl(trim(SCC_LETTER_CD),'-') SCC_LETTER_CD,
    LETTER_PRINTED_DT,
    LETTER_PRINTED_TM,
    nvl(CHECKLIST_SEQ_3C,0) CHECKLIST_SEQ_3C,
    nvl(CHECKLIST_SEQ,0) CHECKLIST_SEQ,
    nvl(trim(COMMENT_PRINT_FLAG),'-') COMMENT_PRINT_FLAG,
    nvl(ORG_CONTACT,0) ORG_CONTACT,
    nvl(ORG_DEPARTMENT,0) ORG_DEPARTMENT,
    nvl(ORG_LOCATION,0) ORG_LOCATION,
    nvl(PROCESS_INSTANCE,0) PROCESS_INSTANCE,
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID,
    nvl(VAR_DATA_SEQ,0) VAR_DATA_SEQ,
    nvl(trim(EMPLID_RELATED),'-') EMPLID_RELATED,
    nvl(trim(JOINT_COMM),'-') JOINT_COMM,
    nvl(trim(SCC_COMM_LANG),'-') SCC_COMM_LANG,
    nvl(trim(SCC_COMM_MTHD),'-') SCC_COMM_MTHD,
    nvl(trim(SCC_COMM_PROC),'-') SCC_COMM_PROC,
    to_char(substr(trim(COMM_COMMENTS), 1, 4000)) COMM_COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
  from SYSADM.PS_COMMUNICATION@SASOURCE S
 where COMMON_ID between '00000000' and '99999999'
   and length(COMMON_ID) = 8
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_COMMUNICATION'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_COMMUNICATION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_COMMUNICATION';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_COMMUNICATION T
using (select /*+ full(S) */
    nvl(trim(COMMON_ID),'-') COMMON_ID,
    nvl(SEQ_3C,0) SEQ_3C,
    nvl(trim(SA_ID_TYPE),'-') SA_ID_TYPE,
    to_date(to_char(case when COMM_DTTM < '01-JAN-1800' then NULL else COMM_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') COMM_DTTM,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(trim(ADMIN_FUNCTION),'-') ADMIN_FUNCTION,
    nvl(trim(COMM_CATEGORY),'-') COMM_CATEGORY,
    nvl(trim(COMM_CONTEXT),'-') COMM_CONTEXT,
    nvl(trim(COMM_METHOD),'-') COMM_METHOD,
    nvl(trim(INCLUDE_ENCL),'-') INCLUDE_ENCL,
    nvl(trim(DEPTID),'-') DEPTID,
    nvl(trim(COMM_ID),'-') COMM_ID,
    to_date(to_char(case when COMM_DT < '01-JAN-1800' then NULL else COMM_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') COMM_DT,
    to_date(to_char(case when COMM_BEGIN_TM < '01-JAN-1800' then NULL else COMM_BEGIN_TM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') COMM_BEGIN_TM,
    to_date(to_char(case when COMM_END_TM < '01-JAN-1800' then NULL else COMM_END_TM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') COMM_END_TM,
    nvl(trim(COMPLETED_COMM),'-') COMPLETED_COMM,
    nvl(trim(COMPLETED_ID),'-') COMPLETED_ID,
    to_date(to_char(case when COMPLETED_DT < '01-JAN-1800' then NULL else COMPLETED_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') COMPLETED_DT,
    nvl(trim(COMM_DIRECTION),'-') COMM_DIRECTION,
    nvl(trim(UNSUCCESSFUL),'-') UNSUCCESSFUL,
    nvl(trim(OUTCOME_REASON),'-') OUTCOME_REASON,
    nvl(trim(SCC_LETTER_CD),'-') SCC_LETTER_CD,
    to_date(to_char(case when LETTER_PRINTED_DT < '01-JAN-1800' then NULL else LETTER_PRINTED_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LETTER_PRINTED_DT,
    to_date(to_char(case when LETTER_PRINTED_TM < '01-JAN-1800' then NULL else LETTER_PRINTED_TM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') LETTER_PRINTED_TM,
    nvl(CHECKLIST_SEQ_3C,0) CHECKLIST_SEQ_3C,
    nvl(CHECKLIST_SEQ,0) CHECKLIST_SEQ,
    nvl(trim(COMMENT_PRINT_FLAG),'-') COMMENT_PRINT_FLAG,
    nvl(ORG_CONTACT,0) ORG_CONTACT,
    nvl(ORG_DEPARTMENT,0) ORG_DEPARTMENT,
    nvl(ORG_LOCATION,0) ORG_LOCATION,
    nvl(PROCESS_INSTANCE,0) PROCESS_INSTANCE,
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID,
    nvl(VAR_DATA_SEQ,0) VAR_DATA_SEQ,
    nvl(trim(EMPLID_RELATED),'-') EMPLID_RELATED,
    nvl(trim(JOINT_COMM),'-') JOINT_COMM,
    nvl(trim(SCC_COMM_LANG),'-') SCC_COMM_LANG,
    nvl(trim(SCC_COMM_MTHD),'-') SCC_COMM_MTHD,
    nvl(trim(SCC_COMM_PROC),'-') SCC_COMM_PROC,
    COMM_COMMENTS COMM_COMMENTS
from CSSTG_OWNER.PS_T_COMMUNICATION S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_COMMUNICATION') ) S
   on (
    T.COMMON_ID = S.COMMON_ID and
    T.SEQ_3C = S.SEQ_3C and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SA_ID_TYPE = S.SA_ID_TYPE,
    T.COMM_DTTM = S.COMM_DTTM,
    T.INSTITUTION = S.INSTITUTION,
    T.ADMIN_FUNCTION = S.ADMIN_FUNCTION,
    T.COMM_CATEGORY = S.COMM_CATEGORY,
    T.COMM_CONTEXT = S.COMM_CONTEXT,
    T.COMM_METHOD = S.COMM_METHOD,
    T.INCLUDE_ENCL = S.INCLUDE_ENCL,
    T.DEPTID = S.DEPTID,
    T.COMM_ID = S.COMM_ID,
    T.COMM_DT = S.COMM_DT,
    T.COMM_BEGIN_TM = S.COMM_BEGIN_TM,
    T.COMM_END_TM = S.COMM_END_TM,
    T.COMPLETED_COMM = S.COMPLETED_COMM,
    T.COMPLETED_ID = S.COMPLETED_ID,
    T.COMPLETED_DT = S.COMPLETED_DT,
    T.COMM_DIRECTION = S.COMM_DIRECTION,
    T.UNSUCCESSFUL = S.UNSUCCESSFUL,
    T.OUTCOME_REASON = S.OUTCOME_REASON,
    T.SCC_LETTER_CD = S.SCC_LETTER_CD,
    T.LETTER_PRINTED_DT = S.LETTER_PRINTED_DT,
    T.LETTER_PRINTED_TM = S.LETTER_PRINTED_TM,
    T.CHECKLIST_SEQ_3C = S.CHECKLIST_SEQ_3C,
    T.CHECKLIST_SEQ = S.CHECKLIST_SEQ,
    T.COMMENT_PRINT_FLAG = S.COMMENT_PRINT_FLAG,
    T.ORG_CONTACT = S.ORG_CONTACT,
    T.ORG_DEPARTMENT = S.ORG_DEPARTMENT,
    T.ORG_LOCATION = S.ORG_LOCATION,
    T.PROCESS_INSTANCE = S.PROCESS_INSTANCE,
    T.EXT_ORG_ID = S.EXT_ORG_ID,
    T.VAR_DATA_SEQ = S.VAR_DATA_SEQ,
    T.EMPLID_RELATED = S.EMPLID_RELATED,
    T.JOINT_COMM = S.JOINT_COMM,
    T.SCC_COMM_LANG = S.SCC_COMM_LANG,
    T.SCC_COMM_MTHD = S.SCC_COMM_MTHD,
    T.SCC_COMM_PROC = S.SCC_COMM_PROC,
    T.COMM_COMMENTS = S.COMM_COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.SA_ID_TYPE <> S.SA_ID_TYPE or
    T.COMM_DTTM <> S.COMM_DTTM or
    T.INSTITUTION <> S.INSTITUTION or
    T.ADMIN_FUNCTION <> S.ADMIN_FUNCTION or
    T.COMM_CATEGORY <> S.COMM_CATEGORY or
    T.COMM_CONTEXT <> S.COMM_CONTEXT or
    T.COMM_METHOD <> S.COMM_METHOD or
    T.INCLUDE_ENCL <> S.INCLUDE_ENCL or
    T.DEPTID <> S.DEPTID or
    T.COMM_ID <> S.COMM_ID or
    nvl(trim(T.COMM_DT),0) <> nvl(trim(S.COMM_DT),0) or
    nvl(trim(T.COMM_BEGIN_TM),0) <> nvl(trim(S.COMM_BEGIN_TM),0) or
    nvl(trim(T.COMM_END_TM),0) <> nvl(trim(S.COMM_END_TM),0) or
    T.COMPLETED_COMM <> S.COMPLETED_COMM or
    T.COMPLETED_ID <> S.COMPLETED_ID or
    nvl(trim(T.COMPLETED_DT),0) <> nvl(trim(S.COMPLETED_DT),0) or
    T.COMM_DIRECTION <> S.COMM_DIRECTION or
    T.UNSUCCESSFUL <> S.UNSUCCESSFUL or
    T.OUTCOME_REASON <> S.OUTCOME_REASON or
    T.SCC_LETTER_CD <> S.SCC_LETTER_CD or
    nvl(trim(T.LETTER_PRINTED_DT),0) <> nvl(trim(S.LETTER_PRINTED_DT),0) or
    nvl(trim(T.LETTER_PRINTED_TM),0) <> nvl(trim(S.LETTER_PRINTED_TM),0) or
    T.CHECKLIST_SEQ_3C <> S.CHECKLIST_SEQ_3C or
    T.CHECKLIST_SEQ <> S.CHECKLIST_SEQ or
    T.COMMENT_PRINT_FLAG <> S.COMMENT_PRINT_FLAG or
    T.ORG_CONTACT <> S.ORG_CONTACT or
    T.ORG_DEPARTMENT <> S.ORG_DEPARTMENT or
    T.ORG_LOCATION <> S.ORG_LOCATION or
    T.PROCESS_INSTANCE <> S.PROCESS_INSTANCE or
    T.EXT_ORG_ID <> S.EXT_ORG_ID or
    T.VAR_DATA_SEQ <> S.VAR_DATA_SEQ or
    T.EMPLID_RELATED <> S.EMPLID_RELATED or
    T.JOINT_COMM <> S.JOINT_COMM or
    T.SCC_COMM_LANG <> S.SCC_COMM_LANG or
    T.SCC_COMM_MTHD <> S.SCC_COMM_MTHD or
    T.SCC_COMM_PROC <> S.SCC_COMM_PROC or
    nvl(trim(T.COMM_COMMENTS),0) <> nvl(trim(S.COMM_COMMENTS),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.COMMON_ID,
    T.SEQ_3C,
    T.SRC_SYS_ID,
    T.SA_ID_TYPE,
    T.COMM_DTTM,
    T.INSTITUTION,
    T.ADMIN_FUNCTION,
    T.COMM_CATEGORY,
    T.COMM_CONTEXT,
    T.COMM_METHOD,
    T.INCLUDE_ENCL,
    T.DEPTID,
    T.COMM_ID,
    T.COMM_DT,
    T.COMM_BEGIN_TM,
    T.COMM_END_TM,
    T.COMPLETED_COMM,
    T.COMPLETED_ID,
    T.COMPLETED_DT,
    T.COMM_DIRECTION,
    T.UNSUCCESSFUL,
    T.OUTCOME_REASON,
    T.SCC_LETTER_CD,
    T.LETTER_PRINTED_DT,
    T.LETTER_PRINTED_TM,
    T.CHECKLIST_SEQ_3C,
    T.CHECKLIST_SEQ,
    T.COMMENT_PRINT_FLAG,
    T.ORG_CONTACT,
    T.ORG_DEPARTMENT,
    T.ORG_LOCATION,
    T.PROCESS_INSTANCE,
    T.EXT_ORG_ID,
    T.VAR_DATA_SEQ,
    T.EMPLID_RELATED,
    T.JOINT_COMM,
    T.SCC_COMM_LANG,
    T.SCC_COMM_MTHD,
    T.SCC_COMM_PROC,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMM_COMMENTS
    )
values (
    S.COMMON_ID,
    S.SEQ_3C,
    'CS90',
    S.SA_ID_TYPE,
    S.COMM_DTTM,
    S.INSTITUTION,
    S.ADMIN_FUNCTION,
    S.COMM_CATEGORY,
    S.COMM_CONTEXT,
    S.COMM_METHOD,
    S.INCLUDE_ENCL,
    S.DEPTID,
    S.COMM_ID,
    S.COMM_DT,
    S.COMM_BEGIN_TM,
    S.COMM_END_TM,
    S.COMPLETED_COMM,
    S.COMPLETED_ID,
    S.COMPLETED_DT,
    S.COMM_DIRECTION,
    S.UNSUCCESSFUL,
    S.OUTCOME_REASON,
    S.SCC_LETTER_CD,
    S.LETTER_PRINTED_DT,
    S.LETTER_PRINTED_TM,
    S.CHECKLIST_SEQ_3C,
    S.CHECKLIST_SEQ,
    S.COMMENT_PRINT_FLAG,
    S.ORG_CONTACT,
    S.ORG_DEPARTMENT,
    S.ORG_LOCATION,
    S.PROCESS_INSTANCE,
    S.EXT_ORG_ID,
    S.VAR_DATA_SEQ,
    S.EMPLID_RELATED,
    S.JOINT_COMM,
    S.SCC_COMM_LANG,
    S.SCC_COMM_MTHD,
    S.SCC_COMM_PROC,
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMM_COMMENTS);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_COMMUNICATION rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_COMMUNICATION',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_COMMUNICATION';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_COMMUNICATION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_COMMUNICATION';
update CSSTG_OWNER.PS_COMMUNICATION T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COMMON_ID, SEQ_3C
   from CSSTG_OWNER.PS_COMMUNICATION T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_COMMUNICATION') = 'Y'
  minus
 select COMMON_ID, SEQ_3C
   from SYSADM.PS_COMMUNICATION@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_COMMUNICATION') = 'Y'
   ) S
 where T.COMMON_ID = S.COMMON_ID
   and T.SEQ_3C = S.SEQ_3C
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_COMMUNICATION rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_COMMUNICATION',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_COMMUNICATION'
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

END PS_COMMUNICATION_P;
/
