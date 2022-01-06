CREATE OR REPLACE PROCEDURE             PS_PERSON_COMMENT_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PERSON_COMMENT from PeopleSoft table PS_PERSON_COMMENT.
--
-- V01  SMT-xxxx 8/15/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_PERSON_COMMENT';
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
 where TABLE_NAME = 'PS_PERSON_COMMENT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PERSON_COMMENT@SASOURCE S)
 where TABLE_NAME = 'PS_PERSON_COMMENT'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_PERSON_COMMENT';
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
 where TABLE_NAME = 'PS_PERSON_COMMENT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_PERSON_COMMENT
select /*+ full(S) */
    nvl(trim(COMMON_ID),'-') COMMON_ID, 
    nvl(SEQ_3C,0) SEQ_3C, 
    trim(SA_ID_TYPE) SA_ID_TYPE, 
    COMMENT_DTTM,
    trim(ADMIN_FUNCTION) ADMIN_FUNCTION, 
    trim(CMNT_CATEGORY) CMNT_CATEGORY, 
    trim(DEPTID) DEPTID, 
    trim(CMNT_ID) CMNT_ID, 
    COMMENT_DT,
    trim(INSTITUTION) INSTITUTION, 
    VAR_DATA_SEQ, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_PERSON_COMMENT@SASOURCE S
where COMMON_ID between '00000000' and '99999999'
  and length(trim(COMMON_ID)) = 8
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_PERSON_COMMENT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_PERSON_COMMENT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_PERSON_COMMENT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_PERSON_COMMENT T 
    using (select /*+ full(S) */
    COMMON_ID, 
    SEQ_3C, 
    SA_ID_TYPE, 
    COMMENT_DTTM,
    ADMIN_FUNCTION, 
    CMNT_CATEGORY, 
    DEPTID, 
    CMNT_ID, 
    COMMENT_DT,
    INSTITUTION, 
    VAR_DATA_SEQ, 
    COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from CSSTG_OWNER.PS_T_PERSON_COMMENT S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_COMMENT') 
) S 
 on ( 
    T.COMMON_ID = S.COMMON_ID and 
    T.SEQ_3C = S.SEQ_3C and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SA_ID_TYPE = S.SA_ID_TYPE,
    T.COMMENT_DTTM = S.COMMENT_DTTM,
    T.ADMIN_FUNCTION = S.ADMIN_FUNCTION,
    T.CMNT_CATEGORY = S.CMNT_CATEGORY,
    T.DEPTID = S.DEPTID,
    T.CMNT_ID = S.CMNT_ID,
    T.COMMENT_DT = S.COMMENT_DT,
    T.INSTITUTION = S.INSTITUTION,
    T.VAR_DATA_SEQ = S.VAR_DATA_SEQ,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.SA_ID_TYPE <> S.SA_ID_TYPE or 
    nvl(trim(T.COMMENT_DTTM),0) <> nvl(trim(S.COMMENT_DTTM),0) or 
    T.ADMIN_FUNCTION <> S.ADMIN_FUNCTION or 
    T.CMNT_CATEGORY <> S.CMNT_CATEGORY or 
    T.DEPTID <> S.DEPTID or 
    T.CMNT_ID <> S.CMNT_ID or 
    nvl(trim(T.COMMENT_DT),0) <> nvl(trim(S.COMMENT_DT),0) or 
    T.INSTITUTION <> S.INSTITUTION or 
    T.VAR_DATA_SEQ <> S.VAR_DATA_SEQ or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.COMMON_ID,
    T.SEQ_3C, 
    T.SRC_SYS_ID, 
    T.SA_ID_TYPE, 
    T.COMMENT_DTTM, 
    T.ADMIN_FUNCTION, 
    T.CMNT_CATEGORY,
    T.DEPTID, 
    T.CMNT_ID,
    T.COMMENT_DT, 
    T.INSTITUTION,
    T.VAR_DATA_SEQ, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.COMMON_ID,
    S.SEQ_3C, 
    'CS90', 
    S.SA_ID_TYPE, 
    S.COMMENT_DTTM, 
    S.ADMIN_FUNCTION, 
    S.CMNT_CATEGORY,
    S.DEPTID, 
    S.CMNT_ID,
    S.COMMENT_DT, 
    S.INSTITUTION,
    S.VAR_DATA_SEQ, 
     'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_PERSON_COMMENT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERSON_COMMENT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PERSON_COMMENT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_PERSON_COMMENT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_PERSON_COMMENT';
update CSSTG_OWNER.PS_PERSON_COMMENT T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COMMON_ID, SEQ_3C
   from CSSTG_OWNER.PS_PERSON_COMMENT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_COMMENT') = 'Y'
  minus
 select COMMON_ID, SEQ_3C
   from SYSADM.PS_PERSON_COMMENT@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_COMMENT') = 'Y'
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

strMessage01    := '# of PS_PERSON_COMMENT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERSON_COMMENT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PERSON_COMMENT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END PS_PERSON_COMMENT_P;
/
