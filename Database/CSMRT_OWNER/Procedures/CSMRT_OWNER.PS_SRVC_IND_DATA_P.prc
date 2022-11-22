DROP PROCEDURE CSMRT_OWNER.PS_SRVC_IND_DATA_P
/

--
-- PS_SRVC_IND_DATA_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_SRVC_IND_DATA_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SRVC_IND_DATA from PeopleSoft table PS_SRVC_IND_DATA.
--
-- V01  SMT-xxxx 9/26/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SRVC_IND_DATA';
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
 where TABLE_NAME = 'PS_SRVC_IND_DATA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_SRVC_IND_DATA@SASOURCE S)
 where TABLE_NAME = 'PS_SRVC_IND_DATA'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_SRVC_IND_DATA';
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
 where TABLE_NAME = 'PS_SRVC_IND_DATA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_SRVC_IND_DATA
select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    SRVC_IND_DTTM, 
    nvl(trim(OPRID),'-') OPRID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(SRVC_IND_CD),'-') SRVC_IND_CD, 
    nvl(trim(SRVC_IND_REASON),'-') SRVC_IND_REASON, 
    nvl(trim(SRVC_IND_ACT_TERM),'-') SRVC_IND_ACT_TERM, 
    SRVC_IND_ACTIVE_DT,
    nvl(trim(SCC_SI_END_TERM),'-') SCC_SI_END_TERM, 
    SCC_SI_END_DT, 
    nvl(trim(POS_SRVC_INDICATOR),'-') POS_SRVC_INDICATOR, 
    replace(nvl(trim(SRVC_IND_REFRNCE),'-'), '  ', ' ') SRVC_IND_REFRNCE, 
    nvl(trim(DEPTID),'-') DEPTID, 
    nvl(trim(POSITION_NBR),'-') POSITION_NBR, 
    nvl(trim(CONTACT),'-') CONTACT, 
    nvl(trim(CONTACT_ID),'-') CONTACT_ID, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(AMOUNT,0) AMOUNT, 
    nvl(SEQ_3C,0) SEQ_3C, 
    nvl(trim(PLACED_METHOD),'-') PLACED_METHOD, 
    nvl(trim(PLACED_PERSON),'-') PLACED_PERSON, 
    nvl(trim(PLACED_PERSON_ID),'-') PLACED_PERSON_ID, 
    nvl(trim(PLACED_PROCESS),'-') PLACED_PROCESS, 
    nvl(trim(RELEASE_PROCESS),'-') RELEASE_PROCESS, 
    to_char(substr(trim(COMM_COMMENTS), 1, 4000)) COMM_COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_SRVC_IND_DATA@SASOURCE S 
where EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_SRVC_IND_DATA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_SRVC_IND_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_SRVC_IND_DATA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_SRVC_IND_DATA T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    SRVC_IND_DTTM, 
    nvl(trim(OPRID),'-') OPRID, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(SRVC_IND_CD),'-') SRVC_IND_CD, 
    nvl(trim(SRVC_IND_REASON),'-') SRVC_IND_REASON, 
    nvl(trim(SRVC_IND_ACT_TERM),'-') SRVC_IND_ACT_TERM, 
    SRVC_IND_ACTIVE_DT,
    nvl(trim(SCC_SI_END_TERM),'-') SCC_SI_END_TERM, 
    SCC_SI_END_DT, 
    nvl(trim(POS_SRVC_INDICATOR),'-') POS_SRVC_INDICATOR, 
    nvl(trim(SRVC_IND_REFRNCE),'-') SRVC_IND_REFRNCE, 
    nvl(trim(DEPTID),'-') DEPTID, 
    nvl(trim(POSITION_NBR),'-') POSITION_NBR, 
    nvl(trim(CONTACT),'-') CONTACT, 
    nvl(trim(CONTACT_ID),'-') CONTACT_ID, 
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD, 
    nvl(AMOUNT,0) AMOUNT, 
    nvl(SEQ_3C,0) SEQ_3C, 
    nvl(trim(PLACED_METHOD),'-') PLACED_METHOD, 
    nvl(trim(PLACED_PERSON),'-') PLACED_PERSON, 
    nvl(trim(PLACED_PERSON_ID),'-') PLACED_PERSON_ID, 
    nvl(trim(PLACED_PROCESS),'-') PLACED_PROCESS, 
    nvl(trim(RELEASE_PROCESS),'-') RELEASE_PROCESS, 
    COMM_COMMENTS
from CSSTG_OWNER.PS_T_SRVC_IND_DATA S 
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SRVC_IND_DATA') ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.SRVC_IND_DTTM = S.SRVC_IND_DTTM and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.OPRID = S.OPRID,
    T.INSTITUTION = S.INSTITUTION,
    T.SRVC_IND_CD = S.SRVC_IND_CD,
    T.SRVC_IND_REASON = S.SRVC_IND_REASON,
    T.SRVC_IND_ACT_TERM = S.SRVC_IND_ACT_TERM,
    T.SRVC_IND_ACTIVE_DT = S.SRVC_IND_ACTIVE_DT,
    T.SCC_SI_END_TERM = S.SCC_SI_END_TERM,
    T.SCC_SI_END_DT = S.SCC_SI_END_DT,
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
    T.COMM_COMMENTS = S.COMM_COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.OPRID <> S.OPRID or 
    T.INSTITUTION <> S.INSTITUTION or 
    T.SRVC_IND_CD <> S.SRVC_IND_CD or 
    T.SRVC_IND_REASON <> S.SRVC_IND_REASON or 
    T.SRVC_IND_ACT_TERM <> S.SRVC_IND_ACT_TERM or 
    nvl(trim(T.SRVC_IND_ACTIVE_DT),0) <> nvl(trim(S.SRVC_IND_ACTIVE_DT),0) or 
    T.SCC_SI_END_TERM <> S.SCC_SI_END_TERM or 
    nvl(trim(T.SCC_SI_END_DT),0) <> nvl(trim(S.SCC_SI_END_DT),0) or 
    T.POS_SRVC_INDICATOR <> S.POS_SRVC_INDICATOR or 
    T.SRVC_IND_REFRNCE <> S.SRVC_IND_REFRNCE or 
    T.DEPTID <> S.DEPTID or 
    T.POSITION_NBR <> S.POSITION_NBR or 
    T.CONTACT <> S.CONTACT or 
    T.CONTACT_ID <> S.CONTACT_ID or 
    T.CURRENCY_CD <> S.CURRENCY_CD or 
    T.AMOUNT <> S.AMOUNT or 
    T.SEQ_3C <> S.SEQ_3C or 
    T.PLACED_METHOD <> S.PLACED_METHOD or 
    T.PLACED_PERSON <> S.PLACED_PERSON or 
    T.PLACED_PERSON_ID <> S.PLACED_PERSON_ID or 
    T.PLACED_PROCESS <> S.PLACED_PROCESS or 
    T.RELEASE_PROCESS <> S.RELEASE_PROCESS or 
    nvl(trim(T.COMM_COMMENTS),0) <> nvl(trim(S.COMM_COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.SRVC_IND_DTTM,
    T.SRC_SYS_ID, 
    T.OPRID,
    T.INSTITUTION,
    T.SRVC_IND_CD,
    T.SRVC_IND_REASON,
    T.SRVC_IND_ACT_TERM,
    T.SRVC_IND_ACTIVE_DT, 
    T.SCC_SI_END_TERM,
    T.SCC_SI_END_DT,
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
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMM_COMMENTS
    ) 
values (
    S.EMPLID, 
    S.SRVC_IND_DTTM,
    'CS90', 
    S.OPRID,
    S.INSTITUTION,
    S.SRVC_IND_CD,
    S.SRVC_IND_REASON,
    S.SRVC_IND_ACT_TERM,
    S.SRVC_IND_ACTIVE_DT, 
    S.SCC_SI_END_TERM,
    S.SCC_SI_END_DT,
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
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMM_COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_SRVC_IND_DATA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SRVC_IND_DATA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_SRVC_IND_DATA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_SRVC_IND_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_SRVC_IND_DATA';
update CSSTG_OWNER.PS_SRVC_IND_DATA T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, SRVC_IND_DTTM
   from CSSTG_OWNER.PS_SRVC_IND_DATA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SRVC_IND_DATA') = 'Y'
  minus
 select EMPLID, SRVC_IND_DTTM
   from SYSADM.PS_SRVC_IND_DATA@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SRVC_IND_DATA') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.SRVC_IND_DTTM = S.SRVC_IND_DTTM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SRVC_IND_DATA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SRVC_IND_DATA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_SRVC_IND_DATA'
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

END PS_SRVC_IND_DATA_P;
/
