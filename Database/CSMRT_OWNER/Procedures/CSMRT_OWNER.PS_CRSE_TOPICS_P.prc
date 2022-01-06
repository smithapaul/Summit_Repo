CREATE OR REPLACE PROCEDURE             "PS_CRSE_TOPICS_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CRSE_TOPICS'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_CRSE_TOPICS', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_CRSE_TOPICS'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_CRSE_TOPICS from PeopleSoft table PS_CRSE_TOPICS.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_CRSE_TOPICS.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_CRSE_TOPICS';
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
    WHERE TABLE_NAME = 'PS_CRSE_TOPICS';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_CRSE_TOPICS@SASOURCE S)
    WHERE TABLE_NAME = 'PS_CRSE_TOPICS';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_CRSE_TOPICS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_CRSE_TOPICS';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CRSE_TOPICS T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(CRS_TOPIC_ID,0) CRS_TOPIC_ID,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(CRSE_REPEATABLE),'-') CRSE_REPEATABLE,
nvl(UNITS_REPEAT_LIMIT,0) UNITS_REPEAT_LIMIT,
nvl(CRSE_REPEAT_LIMIT,0) CRSE_REPEAT_LIMIT,
nvl(trim(DESCRFORMAL),'-') DESCRFORMAL,
nvl(CRS_TOPIC_LINK,0) CRS_TOPIC_LINK
from SYSADM.PS_CRSE_TOPICS@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_TOPICS') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.EFFDT = S.EFFDT and
T.CRS_TOPIC_ID = S.CRS_TOPIC_ID and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.CRSE_REPEATABLE = S.CRSE_REPEATABLE,
T.UNITS_REPEAT_LIMIT = S.UNITS_REPEAT_LIMIT,
T.CRSE_REPEAT_LIMIT = S.CRSE_REPEAT_LIMIT,
T.DESCRFORMAL = S.DESCRFORMAL,
T.CRS_TOPIC_LINK = S.CRS_TOPIC_LINK,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.CRSE_REPEATABLE <> S.CRSE_REPEATABLE or
T.UNITS_REPEAT_LIMIT <> S.UNITS_REPEAT_LIMIT or
T.CRSE_REPEAT_LIMIT <> S.CRSE_REPEAT_LIMIT or
T.DESCRFORMAL <> S.DESCRFORMAL or
T.CRS_TOPIC_LINK <> S.CRS_TOPIC_LINK or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.EFFDT,
T.CRS_TOPIC_ID,
T.SRC_SYS_ID,
T.DESCR,
T.DESCRSHORT,
T.CRSE_REPEATABLE,
T.UNITS_REPEAT_LIMIT,
T.CRSE_REPEAT_LIMIT,
T.DESCRFORMAL,
T.CRS_TOPIC_LINK,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.CRSE_ID,
S.EFFDT,
S.CRS_TOPIC_ID,
'CS90',
S.DESCR,
S.DESCRSHORT,
S.CRSE_REPEATABLE,
S.UNITS_REPEAT_LIMIT,
S.CRSE_REPEAT_LIMIT,
S.DESCRFORMAL,
S.CRS_TOPIC_LINK,
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
         '# of PS_CRSE_TOPICS rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CRSE_TOPICS',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_CRSE_TOPICS';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CRSE_TOPICS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CRSE_TOPICS';


update CSSTG_OWNER.PS_CRSE_TOPICS T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, EFFDT, CRS_TOPIC_ID
   from CSSTG_OWNER.PS_CRSE_TOPICS T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_TOPICS') = 'Y'
  minus
 select CRSE_ID, EFFDT, CRS_TOPIC_ID
   from SYSADM.PS_CRSE_TOPICS@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_TOPICS') = 'Y' 
   ) S
 where T.CRSE_ID = S.CRSE_ID   
  AND T.EFFDT = S.EFFDT
  AND T.CRS_TOPIC_ID = S.CRS_TOPIC_ID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CRSE_TOPICS rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CRSE_TOPICS',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_CRSE_TOPICS';

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
END PS_CRSE_TOPICS_P;
/
