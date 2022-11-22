DROP PROCEDURE CSMRT_OWNER.AM_PS_LOAN_ORIG_MSG_P
/

--
-- AM_PS_LOAN_ORIG_MSG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_LOAN_ORIG_MSG_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_LOAN_ORIG_MSG from PeopleSoft table PS_LOAN_ORIG_MSG.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_LOAN_ORIG_MSG.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_LOAN_ORIG_MSG';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_LOAN_ORIG_MSG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_LOAN_ORIG_MSG@AMSOURCE S)
 where TABLE_NAME = 'PS_LOAN_ORIG_MSG'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_LOAN_ORIG_MSG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_LOAN_ORIG_MSG';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_LOAN_ORIG_MSG T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(LOAN_TYPE),'-') LOAN_TYPE,
nvl(LN_APPL_SEQ,0) LN_APPL_SEQ,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(LN_ORIG_ACTN_SEQ,0) LN_ORIG_ACTN_SEQ,
nvl(LNORIG_MSG_SEQ,0) LNORIG_MSG_SEQ,
nvl(trim(LN_ACTION_MSG),'-') LN_ACTION_MSG
  from SYSADM.PS_LOAN_ORIG_MSG@AMSOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_MSG')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.AID_YEAR = S.AID_YEAR and
T.ACAD_CAREER = S.ACAD_CAREER and
T.LOAN_TYPE = S.LOAN_TYPE and
T.LN_APPL_SEQ = S.LN_APPL_SEQ and
T.ITEM_TYPE = S.ITEM_TYPE and
T.LN_ORIG_ACTN_SEQ = S.LN_ORIG_ACTN_SEQ and
T.LNORIG_MSG_SEQ = S.LNORIG_MSG_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.LN_ACTION_MSG = S.LN_ACTION_MSG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.LN_ACTION_MSG <> S.LN_ACTION_MSG or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.AID_YEAR,
T.ACAD_CAREER,
T.LOAN_TYPE,
T.LN_APPL_SEQ,
T.ITEM_TYPE,
T.LN_ORIG_ACTN_SEQ,
T.LNORIG_MSG_SEQ,
T.SRC_SYS_ID,
T.LN_ACTION_MSG,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.INSTITUTION,
S.AID_YEAR,
S.ACAD_CAREER,
S.LOAN_TYPE,
S.LN_APPL_SEQ,
S.ITEM_TYPE,
S.LN_ORIG_ACTN_SEQ,
S.LNORIG_MSG_SEQ,
'CS90',
S.LN_ACTION_MSG,
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIG_MSG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIG_MSG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_LOAN_ORIG_MSG';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_LOAN_ORIG_MSG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_LOAN_ORIG_MSG';
update AMSTG_OWNER.PS_LOAN_ORIG_MSG T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ, ITEM_TYPE, LN_ORIG_ACTN_SEQ, LNORIG_MSG_SEQ
   from AMSTG_OWNER.PS_LOAN_ORIG_MSG T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_MSG') = 'Y'
  minus
 select EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, LOAN_TYPE, LN_APPL_SEQ, ITEM_TYPE, LN_ORIG_ACTN_SEQ, LNORIG_MSG_SEQ
   from SYSADM.PS_LOAN_ORIG_MSG@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_LOAN_ORIG_MSG') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.INSTITUTION = S.INSTITUTION
   and T.AID_YEAR = S.AID_YEAR
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.LOAN_TYPE = S.LOAN_TYPE
   and T.LN_APPL_SEQ = S.LN_APPL_SEQ
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.LN_ORIG_ACTN_SEQ = S.LN_ORIG_ACTN_SEQ
   and T.LNORIG_MSG_SEQ = S.LNORIG_MSG_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_LOAN_ORIG_MSG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_LOAN_ORIG_MSG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_LOAN_ORIG_MSG'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN
        numSqlCode := SQLCODE;
        strSqlErrm := SQLERRM;

        ROLLBACK;
  
        strMessage01 := 'Error code: ' || TO_CHAR(SQLCODE) || ' Error Message: ' || SQLERRM;
        strMessage02 := TO_CHAR(SQLCODE);
  
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_FAILURE
                       (i_SqlCommand    => strSqlCommand,
                        i_ErrorText     => strMessage01,
                        i_ErrorCode     => strMessage02,
                        i_ErrorMessage  => strSqlErrm
                       );
               
        strMessage01 := 'Error...'
                        || strNewLine   || 'SQL Command:   ' || strSqlCommand
                        || strNewLine   || 'Error code:    ' || numSqlCode
                        || strNewLine   || 'Error Message: ' || strSqlErrm;

        COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
        RAISE_APPLICATION_ERROR( -20001, strMessage01);

END AM_PS_LOAN_ORIG_MSG_P;
/
