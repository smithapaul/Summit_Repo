DROP PROCEDURE CSMRT_OWNER.AM_PS_PERSON_CHECKLST_P
/

--
-- AM_PS_PERSON_CHECKLST_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_PERSON_CHECKLST_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_PERSON_CHECKLST from PeopleSoft table PS_PERSON_CHECKLST.
--
-- V01  SMT-xxxx 05/30/2017,    Jim Doucette
--                              Converted from PS_PERSON_CHECKLST.SQL
--VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_PERSON_CHECKLST';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_PERSON_CHECKLST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_PERSON_CHECKLST@AMSOURCE S)
 where TABLE_NAME = 'PS_PERSON_CHECKLST'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_PERSON_CHECKLST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_PERSON_CHECKLST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_PERSON_CHECKLST
select /*+ full(S) */
    nvl(trim(COMMON_ID),'-') COMMON_ID,
    nvl(SEQ_3C,0) SEQ_3C,
    'CS90' SRC_SYS_ID,
    nvl(trim(SA_ID_TYPE),'-') SA_ID_TYPE,
    nvl(CHECKLIST_DTTM, to_date('01-JAN-1900')) CHECKLIST_DTTM,
    nvl(trim(ADMIN_FUNCTION),'-') ADMIN_FUNCTION,
    nvl(trim(CHECKLIST_CD),'-') CHECKLIST_CD,
    nvl(trim(CHECKLIST_STATUS),'-') CHECKLIST_STATUS,
    STATUS_DT,
    nvl(trim(STATUS_CHANGE_ID),'-') STATUS_CHANGE_ID,
    DUE_DT,
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD,
    nvl(DUE_AMT,0) DUE_AMT,
    nvl(trim(DEPTID),'-') DEPTID,
    nvl(trim(RESPONSIBLE_ID),'-') RESPONSIBLE_ID,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(TRACKING_SEQ,0) TRACKING_SEQ,
    nvl(ORG_CONTACT,0) ORG_CONTACT,
    nvl(trim(CONTACT_NAME),'-') CONTACT_NAME,
    nvl(VAR_DATA_SEQ,0) VAR_DATA_SEQ,
    substr(to_char(trim(COMM_COMMENTS)),1,4000) COMM_COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_PERSON_CHECKLST@AMSOURCE S
where COMMON_ID between '00000000' and '99999999'
  and length(COMMON_ID) = 8
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_PERSON_CHECKLST'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_PERSON_CHECKLST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_PERSON_CHECKLST';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_PERSON_CHECKLST T
using (select /*+ full(S) */
    nvl(trim(COMMON_ID),'-') COMMON_ID,
    nvl(SEQ_3C,0) SEQ_3C,
    nvl(trim(SA_ID_TYPE),'-') SA_ID_TYPE,
    to_date(to_char(case when CHECKLIST_DTTM < '01-JAN-1800' then NULL else CHECKLIST_DTTM end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') CHECKLIST_DTTM,
    nvl(trim(ADMIN_FUNCTION),'-') ADMIN_FUNCTION,
    nvl(trim(CHECKLIST_CD),'-') CHECKLIST_CD,
    nvl(trim(CHECKLIST_STATUS),'-') CHECKLIST_STATUS,
    to_date(to_char(case when STATUS_DT < '01-JAN-1800' then NULL else STATUS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') STATUS_DT,
    nvl(trim(STATUS_CHANGE_ID),'-') STATUS_CHANGE_ID,
    to_date(to_char(case when DUE_DT < '01-JAN-1800' then NULL else DUE_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DUE_DT,
    nvl(trim(CURRENCY_CD),'-') CURRENCY_CD,
    nvl(DUE_AMT,0) DUE_AMT,
    nvl(trim(DEPTID),'-') DEPTID,
    nvl(trim(RESPONSIBLE_ID),'-') RESPONSIBLE_ID,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(TRACKING_SEQ,0) TRACKING_SEQ,
    nvl(ORG_CONTACT,0) ORG_CONTACT,
    nvl(trim(CONTACT_NAME),'-') CONTACT_NAME,
    nvl(VAR_DATA_SEQ,0) VAR_DATA_SEQ,
    COMM_COMMENTS COMM_COMMENTS
from AMSTG_OWNER.PS_T_PERSON_CHECKLST S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_CHECKLST') 
AND LENGTH(COMMON_ID) = 8 AND COMMON_ID BETWEEN '00000000' AND '99999999'
) S
   on (
    T.COMMON_ID = S.COMMON_ID and
    T.SEQ_3C = S.SEQ_3C and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SA_ID_TYPE = S.SA_ID_TYPE,
    T.CHECKLIST_DTTM = S.CHECKLIST_DTTM,
    T.ADMIN_FUNCTION = S.ADMIN_FUNCTION,
    T.CHECKLIST_CD = S.CHECKLIST_CD,
    T.CHECKLIST_STATUS = S.CHECKLIST_STATUS,
    T.STATUS_DT = S.STATUS_DT,
    T.STATUS_CHANGE_ID = S.STATUS_CHANGE_ID,
    T.DUE_DT = S.DUE_DT,
    T.CURRENCY_CD = S.CURRENCY_CD,
    T.DUE_AMT = S.DUE_AMT,
    T.DEPTID = S.DEPTID,
    T.RESPONSIBLE_ID = S.RESPONSIBLE_ID,
    T.INSTITUTION = S.INSTITUTION,
    T.TRACKING_SEQ = S.TRACKING_SEQ,
    T.ORG_CONTACT = S.ORG_CONTACT,
    T.CONTACT_NAME = S.CONTACT_NAME,
    T.VAR_DATA_SEQ = S.VAR_DATA_SEQ,
    T.COMM_COMMENTS = S.COMM_COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.SA_ID_TYPE <> S.SA_ID_TYPE or
    T.CHECKLIST_DTTM <> S.CHECKLIST_DTTM or
    T.ADMIN_FUNCTION <> S.ADMIN_FUNCTION or
    T.CHECKLIST_CD <> S.CHECKLIST_CD or
    T.CHECKLIST_STATUS <> S.CHECKLIST_STATUS or
    nvl(trim(T.STATUS_DT),0) <> nvl(trim(S.STATUS_DT),0) or
    T.STATUS_CHANGE_ID <> S.STATUS_CHANGE_ID or
    nvl(trim(T.DUE_DT),0) <> nvl(trim(S.DUE_DT),0) or
    T.CURRENCY_CD <> S.CURRENCY_CD or
    T.DUE_AMT <> S.DUE_AMT or
    T.DEPTID <> S.DEPTID or
    T.RESPONSIBLE_ID <> S.RESPONSIBLE_ID or
    T.INSTITUTION <> S.INSTITUTION or
    T.TRACKING_SEQ <> S.TRACKING_SEQ or
    T.ORG_CONTACT <> S.ORG_CONTACT or
    T.CONTACT_NAME <> S.CONTACT_NAME or
    T.VAR_DATA_SEQ <> S.VAR_DATA_SEQ or
    nvl(trim(T.COMM_COMMENTS),0) <> nvl(trim(S.COMM_COMMENTS),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.COMMON_ID,
    T.SEQ_3C,
    T.SRC_SYS_ID,
    T.SA_ID_TYPE,
    T.CHECKLIST_DTTM,
    T.ADMIN_FUNCTION,
    T.CHECKLIST_CD,
    T.CHECKLIST_STATUS,
    T.STATUS_DT,
    T.STATUS_CHANGE_ID,
    T.DUE_DT,
    T.CURRENCY_CD,
    T.DUE_AMT,
    T.DEPTID,
    T.RESPONSIBLE_ID,
    T.INSTITUTION,
    T.TRACKING_SEQ,
    T.ORG_CONTACT,
    T.CONTACT_NAME,
    T.VAR_DATA_SEQ,
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
    S.CHECKLIST_DTTM,
    S.ADMIN_FUNCTION,
    S.CHECKLIST_CD,
    S.CHECKLIST_STATUS,
    S.STATUS_DT,
    S.STATUS_CHANGE_ID,
    S.DUE_DT,
    S.CURRENCY_CD,
    S.DUE_AMT,
    S.DEPTID,
    S.RESPONSIBLE_ID,
    S.INSTITUTION,
    S.TRACKING_SEQ,
    S.ORG_CONTACT,
    S.CONTACT_NAME,
    S.VAR_DATA_SEQ,
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


strMessage01    := '# of PS_PERSON_CHECKLST rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERSON_CHECKLST',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_PERSON_CHECKLST';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_PERSON_CHECKLST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_PERSON_CHECKLST';
update AMSTG_OWNER.PS_PERSON_CHECKLST T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COMMON_ID, SEQ_3C
   from AMSTG_OWNER.PS_PERSON_CHECKLST T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_CHECKLST') = 'Y'
  minus
 select COMMON_ID, SEQ_3C
   from SYSADM.PS_PERSON_CHECKLST@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_CHECKLST') = 'Y'
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

strMessage01    := '# of PS_PERSON_CHECKLST rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_PERSON_CHECKLST',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_PERSON_CHECKLST'
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

END AM_PS_PERSON_CHECKLST_P;
/
