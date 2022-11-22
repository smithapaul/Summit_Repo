DROP PROCEDURE CSMRT_OWNER.PS_CRSE_COMPONENT_P
/

--
-- PS_CRSE_COMPONENT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_CRSE_COMPONENT_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CRSE_COMPONENT'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_CRSE_COMPONENT', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CRSE_COMPONENT'
   */


   ------------------------------------------------------------------------
   --
   --
   -- Loads stage table PS_CRSE_COMPONENT from PeopleSoft table PS_CRSE_COMPONENT.
   --
   -- V01  SMT-xxxx 08/09/2017,    Preethi Lodha
   --                              Converted from PS_CRSE_COMPONENT.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_CRSE_COMPONENT';
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
    WHERE TABLE_NAME = 'PS_CRSE_COMPONENT';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_CRSE_COMPONENT@SASOURCE S)
    WHERE TABLE_NAME = 'PS_CRSE_COMPONENT';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_CRSE_COMPONENT';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_CRSE_COMPONENT';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CRSE_COMPONENT T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(SSR_COMPONENT),'-') SSR_COMPONENT,
nvl(trim(OPTIONAL_SECTION),'-') OPTIONAL_SECTION,
nvl(DEFAULT_SECT_SIZE,0) DEFAULT_SECT_SIZE,
nvl(CONTACT_HOURS,0) CONTACT_HOURS,
nvl(trim(FINAL_EXAM),'-') FINAL_EXAM,
nvl(EXAM_SEAT_SPACING,0) EXAM_SEAT_SPACING,
nvl(trim(DYN_DT_INCLUDE),'-') DYN_DT_INCLUDE,
nvl(trim(AUTO_CREATE_CMPNT),'-') AUTO_CREATE_CMPNT,
nvl(trim(ATTEND_GENERATE),'-') ATTEND_GENERATE,
nvl(WEEK_WORKLOAD_HRS,0) WEEK_WORKLOAD_HRS,
nvl(OEE_WORKLOAD_HRS,0) OEE_WORKLOAD_HRS,
nvl(trim(LMS_FILE_TYPE),'-') LMS_FILE_TYPE,
nvl(trim(LMS_PROVIDER),'-') LMS_PROVIDER
from SYSADM.PS_CRSE_COMPONENT@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_COMPONENT') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.EFFDT = S.EFFDT and
T.SSR_COMPONENT = S.SSR_COMPONENT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.OPTIONAL_SECTION = S.OPTIONAL_SECTION,
T.DEFAULT_SECT_SIZE = S.DEFAULT_SECT_SIZE,
T.CONTACT_HOURS = S.CONTACT_HOURS,
T.FINAL_EXAM = S.FINAL_EXAM,
T.EXAM_SEAT_SPACING = S.EXAM_SEAT_SPACING,
T.DYN_DT_INCLUDE = S.DYN_DT_INCLUDE,
T.AUTO_CREATE_CMPNT = S.AUTO_CREATE_CMPNT,
T.ATTEND_GENERATE = S.ATTEND_GENERATE,
T.WEEK_WORKLOAD_HRS = S.WEEK_WORKLOAD_HRS,
T.OEE_WORKLOAD_HRS = S.OEE_WORKLOAD_HRS,
T.LMS_FILE_TYPE = S.LMS_FILE_TYPE,
T.LMS_PROVIDER = S.LMS_PROVIDER,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.OPTIONAL_SECTION <> S.OPTIONAL_SECTION or
T.DEFAULT_SECT_SIZE <> S.DEFAULT_SECT_SIZE or
T.CONTACT_HOURS <> S.CONTACT_HOURS or
T.FINAL_EXAM <> S.FINAL_EXAM or
T.EXAM_SEAT_SPACING <> S.EXAM_SEAT_SPACING or
T.DYN_DT_INCLUDE <> S.DYN_DT_INCLUDE or
T.AUTO_CREATE_CMPNT <> S.AUTO_CREATE_CMPNT or
T.ATTEND_GENERATE <> S.ATTEND_GENERATE or
T.WEEK_WORKLOAD_HRS <> S.WEEK_WORKLOAD_HRS or
T.OEE_WORKLOAD_HRS <> S.OEE_WORKLOAD_HRS or
T.LMS_FILE_TYPE <> S.LMS_FILE_TYPE or
T.LMS_PROVIDER <> S.LMS_PROVIDER or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.EFFDT,
T.SSR_COMPONENT,
T.SRC_SYS_ID,
T.OPTIONAL_SECTION,
T.DEFAULT_SECT_SIZE,
T.CONTACT_HOURS,
T.FINAL_EXAM,
T.EXAM_SEAT_SPACING,
T.DYN_DT_INCLUDE,
T.AUTO_CREATE_CMPNT,
T.ATTEND_GENERATE,
T.WEEK_WORKLOAD_HRS,
T.OEE_WORKLOAD_HRS,
T.LMS_FILE_TYPE,
T.LMS_PROVIDER,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.CRSE_ID,
S.EFFDT,
S.SSR_COMPONENT,
'CS90',
S.OPTIONAL_SECTION,
S.DEFAULT_SECT_SIZE,
S.CONTACT_HOURS,
S.FINAL_EXAM,
S.EXAM_SEAT_SPACING,
S.DYN_DT_INCLUDE,
S.AUTO_CREATE_CMPNT,
S.ATTEND_GENERATE,
S.WEEK_WORKLOAD_HRS,
S.OEE_WORKLOAD_HRS,
S.LMS_FILE_TYPE,
S.LMS_PROVIDER,
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
         '# of PS_CRSE_COMPONENT rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CRSE_COMPONENT',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_CRSE_COMPONENT';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CRSE_COMPONENT';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CRSE_COMPONENT';

update CSSTG_OWNER.PS_CRSE_COMPONENT T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, EFFDT, SSR_COMPONENT
   from CSSTG_OWNER.PS_CRSE_COMPONENT T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_COMPONENT') = 'Y'
  minus
 select CRSE_ID, EFFDT, SSR_COMPONENT
   from SYSADM.PS_CRSE_COMPONENT@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_COMPONENT') = 'Y' 
   ) S
 where T.CRSE_ID = S.CRSE_ID   
  AND T.EFFDT = S.EFFDT
  AND T.SSR_COMPONENT = S.SSR_COMPONENT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CRSE_COMPONENT rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CRSE_COMPONENT',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_CRSE_COMPONENT';

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
END PS_CRSE_COMPONENT_P;
/
