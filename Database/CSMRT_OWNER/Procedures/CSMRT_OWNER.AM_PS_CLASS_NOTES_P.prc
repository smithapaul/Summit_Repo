DROP PROCEDURE CSMRT_OWNER.AM_PS_CLASS_NOTES_P
/

--
-- AM_PS_CLASS_NOTES_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_CLASS_NOTES_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CLASS_NOTES from PeopleSoft table PS_CLASS_NOTES.
--
-- V01  SMT-xxxx 8/18/2017,    Preethi Lodha
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_CLASS_NOTES';
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
 where TABLE_NAME = 'PS_CLASS_NOTES'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CLASS_NOTES@AMSOURCE S)
 where TABLE_NAME = 'PS_CLASS_NOTES'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_CLASS_NOTES';
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
 where TABLE_NAME = 'PS_CLASS_NOTES'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  AMSTG_OWNER.PS_T_CLASS_NOTES
   SELECT /*+ full(S) */
         CRSE_ID,
          CRSE_OFFER_NBR,
          STRM,
          SESSION_CODE,
          CLASS_SECTION,
          CLASS_NOTES_SEQ,
          'CS90' SRC_SYS_ID,
          PRINT_AT,
          CLASS_NOTE_NBR,
          PRINT_NOTE_W_O_CLS,
          '1234' BATCH_SID,
          TO_CHAR (SUBSTR (TRIM (DESCRLONG), 1, 4000)) DESCRLONG,
          TO_NUMBER (ORA_ROWSCN) SRC_SCN
     FROM SYSADM.PS_CLASS_NOTES@AMSOURCE;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_CLASS_NOTES'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_CLASS_NOTES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_CLASS_NOTES';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CLASS_NOTES T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR,
nvl(trim(STRM),'-') STRM,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(trim(CLASS_SECTION),'-') CLASS_SECTION,
nvl(CLASS_NOTES_SEQ,0) CLASS_NOTES_SEQ,
nvl(trim(PRINT_AT),'-') PRINT_AT,
nvl(trim(CLASS_NOTE_NBR),'-') CLASS_NOTE_NBR,
nvl(trim(PRINT_NOTE_W_O_CLS),'-') PRINT_NOTE_W_O_CLS,
DESCRLONG DESCRLONG
from AMSTG_OWNER.PS_T_CLASS_NOTES S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_NOTES') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and
T.STRM = S.STRM and
T.SESSION_CODE = S.SESSION_CODE and
T.CLASS_SECTION = S.CLASS_SECTION and
T.CLASS_NOTES_SEQ = S.CLASS_NOTES_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.PRINT_AT = S.PRINT_AT,
T.CLASS_NOTE_NBR = S.CLASS_NOTE_NBR,
T.PRINT_NOTE_W_O_CLS = S.PRINT_NOTE_W_O_CLS,
T.DESCRLONG = S.DESCRLONG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.PRINT_AT <> S.PRINT_AT or
T.CLASS_NOTE_NBR <> S.CLASS_NOTE_NBR or
T.PRINT_NOTE_W_O_CLS <> S.PRINT_NOTE_W_O_CLS or
nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.CRSE_OFFER_NBR,
T.STRM,
T.SESSION_CODE,
T.CLASS_SECTION,
T.CLASS_NOTES_SEQ,
T.SRC_SYS_ID,
T.PRINT_AT,
T.CLASS_NOTE_NBR,
T.PRINT_NOTE_W_O_CLS,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID,
T.DESCRLONG
)
values (
S.CRSE_ID,
S.CRSE_OFFER_NBR,
S.STRM,
S.SESSION_CODE,
S.CLASS_SECTION,
S.CLASS_NOTES_SEQ,
'CS90',
S.PRINT_AT,
S.CLASS_NOTE_NBR,
S.PRINT_NOTE_W_O_CLS,
'N',
'S',
sysdate,
sysdate,
1234,
S.DESCRLONG);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_CLASS_NOTES rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_NOTES',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CLASS_NOTES';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_NOTES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CLASS_NOTES';
update AMSTG_OWNER.PS_CLASS_NOTES T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_NOTES_SEQ
   from AMSTG_OWNER.PS_CLASS_NOTES T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_NOTES') = 'Y'
  minus
 select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_NOTES_SEQ
   from SYSADM.PS_CLASS_NOTES@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_NOTES') = 'Y'
   ) S
 where T.CRSE_ID = S.CRSE_ID
   and T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
   and T.STRM = S.STRM
   and T.SESSION_CODE = S.SESSION_CODE
   and T.CLASS_SECTION = S.CLASS_SECTION
   and T.CLASS_NOTES_SEQ = S.CLASS_NOTES_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CLASS_NOTES rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CLASS_NOTES',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CLASS_NOTES'
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

END AM_PS_CLASS_NOTES_P;
/
