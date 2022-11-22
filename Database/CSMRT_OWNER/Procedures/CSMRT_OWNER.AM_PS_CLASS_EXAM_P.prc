DROP PROCEDURE CSMRT_OWNER.AM_PS_CLASS_EXAM_P
/

--
-- AM_PS_CLASS_EXAM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_CLASS_EXAM_P" IS

------------------------------------------------------------------------
--Preethi Lodha
--
-- Loads stage table PS_CLASS_EXAM from PeopleSoft table PS_CLASS_EXAM.
--
-- V01  SMT-xxxx 07/12/2017,    Preethi Lodha
--                              Converted from PS_CLASS_EXAM.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_CLASS_EXAM';
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
 where TABLE_NAME = 'PS_CLASS_EXAM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLASS_EXAM@AMSOURCE S)
 where TABLE_NAME = 'PS_CLASS_EXAM'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_CLASS_EXAM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_CLASS_EXAM';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CLASS_EXAM T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR,
nvl(trim(STRM),'-') STRM,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(trim(CLASS_SECTION),'-') CLASS_SECTION,
nvl(CLASS_EXAM_SEQ,0) CLASS_EXAM_SEQ,
nvl(trim(EXAM_TIME_CODE),'-') EXAM_TIME_CODE,
to_date(to_char(case when EXAM_DT < '01-JAN-1800' then NULL else EXAM_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EXAM_DT,
to_date(to_char(case when EXAM_START_TIME < '01-JAN-1800' then NULL else EXAM_START_TIME end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EXAM_START_TIME,
to_date(to_char(case when EXAM_END_TIME < '01-JAN-1800' then NULL else EXAM_END_TIME end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EXAM_END_TIME,
nvl(trim(FACILITY_ID),'-') FACILITY_ID,
nvl(trim(CLASS_EXAM_TYPE),'-') CLASS_EXAM_TYPE,
nvl(trim(COMBINED_EXAM),'-') COMBINED_EXAM
from SYSADM.PS_CLASS_EXAM@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_EXAM') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and
T.STRM = S.STRM and
T.SESSION_CODE = S.SESSION_CODE and
T.CLASS_SECTION = S.CLASS_SECTION and
T.CLASS_EXAM_SEQ = S.CLASS_EXAM_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EXAM_TIME_CODE = S.EXAM_TIME_CODE,
T.EXAM_DT = S.EXAM_DT,
T.EXAM_START_TIME = S.EXAM_START_TIME,
T.EXAM_END_TIME = S.EXAM_END_TIME,
T.FACILITY_ID = S.FACILITY_ID,
T.CLASS_EXAM_TYPE = S.CLASS_EXAM_TYPE,
T.COMBINED_EXAM = S.COMBINED_EXAM,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EXAM_TIME_CODE <> S.EXAM_TIME_CODE or
T.EXAM_DT <> S.EXAM_DT or
T.EXAM_START_TIME <> S.EXAM_START_TIME or
T.EXAM_END_TIME <> S.EXAM_END_TIME or
T.FACILITY_ID <> S.FACILITY_ID or
T.CLASS_EXAM_TYPE <> S.CLASS_EXAM_TYPE or
T.COMBINED_EXAM <> S.COMBINED_EXAM or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.CRSE_OFFER_NBR,
T.STRM,
T.SESSION_CODE,
T.CLASS_SECTION,
T.CLASS_EXAM_SEQ,
T.SRC_SYS_ID,
T.EXAM_TIME_CODE,
T.EXAM_DT,
T.EXAM_START_TIME,
T.EXAM_END_TIME,
T.FACILITY_ID,
T.CLASS_EXAM_TYPE,
T.COMBINED_EXAM,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.CRSE_ID,
S.CRSE_OFFER_NBR,
S.STRM,
S.SESSION_CODE,
S.CLASS_SECTION,
S.CLASS_EXAM_SEQ,
'CS90',
S.EXAM_TIME_CODE,
S.EXAM_DT,
S.EXAM_START_TIME,
S.EXAM_END_TIME,
S.FACILITY_ID,
S.CLASS_EXAM_TYPE,
S.COMBINED_EXAM,
'N',
'S',
sysdate,
sysdate,
1234);
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_EXAM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_EXAM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLASS_EXAM';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_EXAM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_EXAM';
update AMSTG_OWNER.PS_CLASS_EXAM T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_EXAM_SEQ
   from AMSTG_OWNER.PS_CLASS_EXAM T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_EXAM') = 'Y'
  minus
 select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_EXAM_SEQ
   from SYSADM.PS_CLASS_EXAM@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_EXAM') = 'Y' 
   ) S
 where T.CRSE_ID = S.CRSE_ID    
    AND T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
    AND T.STRM = S.STRM
    AND T.SESSION_CODE = S.SESSION_CODE
    AND T.CLASS_SECTION = S.CLASS_SECTION
    AND T.CLASS_EXAM_SEQ = S.CLASS_EXAM_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_EXAM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_EXAM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLASS_EXAM'
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

END AM_PS_CLASS_EXAM_P;
/
