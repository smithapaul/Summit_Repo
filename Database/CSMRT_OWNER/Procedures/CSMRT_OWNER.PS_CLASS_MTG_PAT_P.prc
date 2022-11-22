DROP PROCEDURE CSMRT_OWNER.PS_CLASS_MTG_PAT_P
/

--
-- PS_CLASS_MTG_PAT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CLASS_MTG_PAT_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CLASS_MTG_PAT'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_CLASS_MTG_PAT', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CLASS_MTG_PAT'
   */


   ------------------------------------------------------------------------
   --
   --
   -- Loads stage table PS_CLASS_MTG_PAT from PeopleSoft table PS_CLASS_MTG_PAT.
   --
   -- V01  SMT-xxxx 08/09/2017,    Preethi Lodha
   --                              Converted from PS_CLASS_MTG_PAT.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_CLASS_MTG_PAT';
   intProcessSid      INTEGER;
   dtProcessStart     DATE := SYSDATE;
   strMessage01       VARCHAR2 (4000);
   strMessage02       VARCHAR2 (512);
   strMessage03       VARCHAR2 (512) := '';
   strNewLine         VARCHAR2 (2) := CHR (13) || CHR (10);
   strSqlCommand      VARCHAR2 (32767) := '';
   strSqlDynamic      VARCHAR2 (32767) := '';
   strClientInfo      VARCHAR2 (100);
   intRowCount        INTEGER;
   intTotalRowCount   INTEGER := 0;
   numSqlCode         NUMBER;
   strSqlErrm         VARCHAR2 (4000);
   intTries           INTEGER;
BEGIN
   strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
   DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT (
      i_MartId             => strMartId,
      i_ProcessName        => strProcessName,
      i_ProcessStartTime   => dtProcessStart,
      o_ProcessSid         => intProcessSid);

   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_CLASS_MTG_PAT';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_CLASS_MTG_PAT@SASOURCE S)
    WHERE TABLE_NAME = 'PS_CLASS_MTG_PAT';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_CLASS_MTG_PAT';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_CLASS_MTG_PAT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CLASS_MTG_PAT T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR,
nvl(trim(STRM),'-') STRM,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(trim(CLASS_SECTION),'-') CLASS_SECTION,
nvl(CLASS_MTG_NBR,0) CLASS_MTG_NBR,
nvl(trim(FACILITY_ID),'-') FACILITY_ID,
to_date(to_char(case when MEETING_TIME_START < '01-JAN-1800' then NULL else MEETING_TIME_START end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') MEETING_TIME_START,
to_date(to_char(case when MEETING_TIME_END < '01-JAN-1800' then NULL else MEETING_TIME_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') MEETING_TIME_END,
nvl(trim(MON),'-') MON,
nvl(trim(TUES),'-') TUES,
nvl(trim(WED),'-') WED,
nvl(trim(THURS),'-') THURS,
nvl(trim(FRI),'-') FRI,
nvl(trim(SAT),'-') SAT,
nvl(trim(SUN),'-') SUN,
to_date(to_char(case when START_DT < '01-JAN-1800' then NULL else START_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') START_DT,
to_date(to_char(case when END_DT < '01-JAN-1800' then NULL else END_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') END_DT,
nvl(CRS_TOPIC_ID,0) CRS_TOPIC_ID,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(STND_MTG_PAT),'-') STND_MTG_PAT,
nvl(trim(PRINT_TOPIC_ON_XCR),'-') PRINT_TOPIC_ON_XCR
from SYSADM.PS_CLASS_MTG_PAT@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_MTG_PAT') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and
T.STRM = S.STRM and
T.SESSION_CODE = S.SESSION_CODE and
T.CLASS_SECTION = S.CLASS_SECTION and
T.CLASS_MTG_NBR = S.CLASS_MTG_NBR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.FACILITY_ID = S.FACILITY_ID,
T.MEETING_TIME_START = S.MEETING_TIME_START,
T.MEETING_TIME_END = S.MEETING_TIME_END,
T.MON = S.MON,
T.TUES = S.TUES,
T.WED = S.WED,
T.THURS = S.THURS,
T.FRI = S.FRI,
T.SAT = S.SAT,
T.SUN = S.SUN,
T.START_DT = S.START_DT,
T.END_DT = S.END_DT,
T.CRS_TOPIC_ID = S.CRS_TOPIC_ID,
T.DESCR = S.DESCR,
T.STND_MTG_PAT = S.STND_MTG_PAT,
T.PRINT_TOPIC_ON_XCR = S.PRINT_TOPIC_ON_XCR,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.FACILITY_ID <> S.FACILITY_ID or
--nvl(trim(T.MEETING_TIME_START),0) <> nvl(trim(S.MEETING_TIME_START),0) or
nvl(to_char(T.MEETING_TIME_START,'HH24:MI:SS'),'00:00:00') <> nvl(to_char(S.MEETING_TIME_START,'HH24:MI:SS'),'00:00:00') or     -- Jan 2018 
--nvl(trim(T.MEETING_TIME_END),0) <> nvl(trim(S.MEETING_TIME_END),0) or
nvl(to_char(T.MEETING_TIME_END,'HH24:MI:SS'),'00:00:00') <> nvl(to_char(S.MEETING_TIME_END,'HH24:MI:SS'),'00:00:00') or         -- Jan 2018 
T.MON <> S.MON or
T.TUES <> S.TUES or
T.WED <> S.WED or
T.THURS <> S.THURS or
T.FRI <> S.FRI or
T.SAT <> S.SAT or
T.SUN <> S.SUN or
nvl(trim(T.START_DT),0) <> nvl(trim(S.START_DT),0) or
nvl(trim(T.END_DT),0) <> nvl(trim(S.END_DT),0) or
T.CRS_TOPIC_ID <> S.CRS_TOPIC_ID or
T.DESCR <> S.DESCR or
T.STND_MTG_PAT <> S.STND_MTG_PAT or
T.PRINT_TOPIC_ON_XCR <> S.PRINT_TOPIC_ON_XCR or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.CRSE_OFFER_NBR,
T.STRM,
T.SESSION_CODE,
T.CLASS_SECTION,
T.CLASS_MTG_NBR,
T.SRC_SYS_ID,
T.FACILITY_ID,
T.MEETING_TIME_START,
T.MEETING_TIME_END,
T.MON,
T.TUES,
T.WED,
T.THURS,
T.FRI,
T.SAT,
T.SUN,
T.START_DT,
T.END_DT,
T.CRS_TOPIC_ID,
T.DESCR,
T.STND_MTG_PAT,
T.PRINT_TOPIC_ON_XCR,
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
S.CLASS_MTG_NBR,
'CS90',
S.FACILITY_ID,
S.MEETING_TIME_START,
S.MEETING_TIME_END,
S.MON,
S.TUES,
S.WED,
S.THURS,
S.FRI,
S.SAT,
S.SUN,
S.START_DT,
S.END_DT,
S.CRS_TOPIC_ID,
S.DESCR,
S.STND_MTG_PAT,
S.PRINT_TOPIC_ON_XCR,
'N',
'S',
sysdate,
sysdate,
1234);


   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CLASS_MTG_PAT rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CLASS_MTG_PAT',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_CLASS_MTG_PAT';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_MTG_PAT';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CLASS_MTG_PAT';

update CSSTG_OWNER.PS_CLASS_MTG_PAT T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_MTG_NBR
   from CSSTG_OWNER.PS_CLASS_MTG_PAT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_MTG_PAT') = 'Y'
  minus
 select CRSE_ID, CRSE_OFFER_NBR, STRM, SESSION_CODE, CLASS_SECTION, CLASS_MTG_NBR
   from SYSADM.PS_CLASS_MTG_PAT@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CLASS_MTG_PAT') = 'Y' 
   ) S
 where T.CRSE_ID = S.CRSE_ID   
  AND T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
  AND T.STRM = S.STRM
  AND T.SESSION_CODE = S.SESSION_CODE
  AND T.CLASS_SECTION = S.CLASS_SECTION
  AND T.CLASS_MTG_NBR = S.CLASS_MTG_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CLASS_MTG_PAT rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CLASS_MTG_PAT',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_CLASS_MTG_PAT';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

   strMessage01 := strProcessName || ' is complete.';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);
EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);
END PS_CLASS_MTG_PAT_P;
/
