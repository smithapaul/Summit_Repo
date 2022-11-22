DROP PROCEDURE CSMRT_OWNER.AM_PS_STND_MTGPAT_TBL_P
/

--
-- AM_PS_STND_MTGPAT_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_STND_MTGPAT_TBL_P" IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_STND_MTGPAT_TBL from PeopleSoft table PS_STND_MTGPAT_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_STND_MTGPAT_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_STND_MTGPAT_TBL';
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

   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_STND_MTGPAT_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_STND_MTGPAT_TBL@AMSOURCE S)
    WHERE TABLE_NAME = 'PS_STND_MTGPAT_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into AMSTG_OWNER.PS_STND_MTGPAT_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into AMSTG_OWNER.PS_STND_MTGPAT_TBL';

   MERGE /*+ use_hash(S,T) */
        INTO  AMSTG_OWNER.PS_STND_MTGPAT_TBL T
        USING (SELECT /*+ full(S) */
                     NVL (TRIM (INSTITUTION), '-') INSTITUTION,
                      NVL (TRIM (ACAD_GROUP), '-') ACAD_GROUP,
                      TO_DATE (
                         TO_CHAR (
                            CASE
                               WHEN EFFDT < '01-JAN-1800' THEN NULL
                               ELSE EFFDT
                            END,
                            'MM/DD/YYYY HH24:MI:SS'),
                         'MM/DD/YYYY HH24:MI:SS')
                         EFFDT,
                      NVL (TRIM (STND_MTG_PAT), '-') STND_MTG_PAT,
                      NVL (TRIM (DESCR), '-') DESCR,
                      NVL (TRIM (DESCRSHORT), '-') DESCRSHORT,
                      NVL (TRIM (MON), '-') MON,
                      NVL (TRIM (TUES), '-') TUES,
                      NVL (TRIM (WED), '-') WED,
                      NVL (TRIM (THURS), '-') THURS,
                      NVL (TRIM (FRI), '-') FRI,
                      NVL (TRIM (SAT), '-') SAT,
                      NVL (TRIM (SUN), '-') SUN,
                      NVL (NORM_DURATION, 0) NORM_DURATION
                 FROM SYSADM.PS_STND_MTGPAT_TBL@AMSOURCE S
                WHERE ORA_ROWSCN > (SELECT OLD_MAX_SCN
                                      FROM AMSTG_OWNER.UM_STAGE_JOBS
                                     WHERE TABLE_NAME = 'PS_STND_MTGPAT_TBL')) S
           ON (    T.INSTITUTION = S.INSTITUTION
               AND T.ACAD_GROUP = S.ACAD_GROUP
               AND T.EFFDT = S.EFFDT
               AND T.STND_MTG_PAT = S.STND_MTG_PAT
               AND T.SRC_SYS_ID = 'CS90')
   WHEN MATCHED
   THEN
      UPDATE SET
         T.DESCR = S.DESCR,
         T.DESCRSHORT = S.DESCRSHORT,
         T.MON = S.MON,
         T.TUES = S.TUES,
         T.WED = S.WED,
         T.THURS = S.THURS,
         T.FRI = S.FRI,
         T.SAT = S.SAT,
         T.SUN = S.SUN,
         T.NORM_DURATION = S.NORM_DURATION,
         T.DATA_ORIGIN = 'S',
         T.LASTUPD_EW_DTTM = SYSDATE,
         T.BATCH_SID = 1234
              WHERE    T.DESCR <> S.DESCR
                    OR T.DESCRSHORT <> S.DESCRSHORT
                    OR T.MON <> S.MON
                    OR T.TUES <> S.TUES
                    OR T.WED <> S.WED
                    OR T.THURS <> S.THURS
                    OR T.FRI <> S.FRI
                    OR T.SAT <> S.SAT
                    OR T.SUN <> S.SUN
                    OR T.NORM_DURATION <> S.NORM_DURATION
                    OR T.DATA_ORIGIN = 'D'
   WHEN NOT MATCHED
   THEN
      INSERT     (T.INSTITUTION,
                  T.ACAD_GROUP,
                  T.EFFDT,
                  T.STND_MTG_PAT,
                  T.SRC_SYS_ID,
                  T.DESCR,
                  T.DESCRSHORT,
                  T.MON,
                  T.TUES,
                  T.WED,
                  T.THURS,
                  T.FRI,
                  T.SAT,
                  T.SUN,
                  T.NORM_DURATION,
                  T.LOAD_ERROR,
                  T.DATA_ORIGIN,
                  T.CREATED_EW_DTTM,
                  T.LASTUPD_EW_DTTM,
                  T.BATCH_SID)
          VALUES (S.INSTITUTION,
                  S.ACAD_GROUP,
                  S.EFFDT,
                  S.STND_MTG_PAT,
                  'CS90',
                  S.DESCR,
                  S.DESCRSHORT,
                  S.MON,
                  S.TUES,
                  S.WED,
                  S.THURS,
                  S.FRI,
                  S.SAT,
                  S.SUN,
                  S.NORM_DURATION,
                  'N',
                  'S',
                  SYSDATE,
                  SYSDATE,
                  1234);

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_STND_MTGPAT_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STND_MTGPAT_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_STND_MTGPAT_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_STND_MTGPAT_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on AMSTG_OWNER.PS_STND_MTGPAT_TBL';

   UPDATE AMSTG_OWNER.PS_STND_MTGPAT_TBL T
      SET T.DATA_ORIGIN = 'D', T.LASTUPD_EW_DTTM = SYSDATE
    WHERE     T.DATA_ORIGIN <> 'D'
          AND EXISTS
                 (SELECT 1
                    FROM (SELECT INSTITUTION,
                                 ACAD_GROUP,
                                 EFFDT,
                                 STND_MTG_PAT
                            FROM AMSTG_OWNER.PS_STND_MTGPAT_TBL T2
                           WHERE (SELECT DELETE_FLG
                                    FROM AMSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_STND_MTGPAT_TBL') =
                                    'Y'
                          MINUS
                          SELECT INSTITUTION,
                                 ACAD_GROUP,
                                 EFFDT,
                                 STND_MTG_PAT
                            FROM SYSADM.PS_STND_MTGPAT_TBL@AMSOURCE
                           WHERE (SELECT DELETE_FLG
                                    FROM AMSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_STND_MTGPAT_TBL') =
                                    'Y') S
                   WHERE     T.INSTITUTION = S.INSTITUTION
                         AND T.ACAD_GROUP = S.ACAD_GROUP
                         AND T.EFFDT = S.EFFDT
                         AND T.STND_MTG_PAT = S.STND_MTG_PAT
                         AND T.SRC_SYS_ID = 'CS90');

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_STND_MTGPAT_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STND_MTGPAT_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_STND_MTGPAT_TBL';

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
END AM_PS_STND_MTGPAT_TBL_P;
/
