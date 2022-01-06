CREATE OR REPLACE PROCEDURE             "PS_REPEAT_CODE_TBL_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_REPEAT_CODE_TBL', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_REPEAT_CODE_TBL from PeopleSoft table PS_REPEAT_CODE_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_REPEAT_CODE_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_REPEAT_CODE_TBL';
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
    WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_REPEAT_CODE_TBL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_REPEAT_CODE_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_REPEAT_CODE_TBL';

   MERGE /*+ use_hash(S,T) */
        INTO  CSSTG_OWNER.PS_REPEAT_CODE_TBL T
        USING (SELECT /*+ full(S) */
                     NVL (TRIM (SETID), '-') SETID,
                      NVL (TRIM (REPEAT_SCHEME), '-') REPEAT_SCHEME,
                      TO_DATE (
                         TO_CHAR (
                            CASE
                               WHEN EFFDT < '01-JAN-1800' THEN NULL
                               ELSE EFFDT
                            END,
                            'MM/DD/YYYY HH24:MI:SS'),
                         'MM/DD/YYYY HH24:MI:SS')
                         EFFDT,
                      NVL (TRIM (REPEAT_CODE), '-') REPEAT_CODE,
                      NVL (TRIM (DESCR), '-') DESCR,
                      NVL (TRIM (DESCRSHORT), '-') DESCRSHORT,
                      NVL (TRIM (EARN_CREDIT), '-') EARN_CREDIT,
                      NVL (TRIM (UNITS_ATTEMPTED), '-') UNITS_ATTEMPTED,
                      NVL (TRIM (INCLUDE_IN_GPA), '-') INCLUDE_IN_GPA,
                      NVL (TRIM (PRINT_REPEAT_DESCR), '-') PRINT_REPEAT_DESCR,
                      NVL (TRIM (PRINT_REPEAT_DATE), '-') PRINT_REPEAT_DATE,
                      NVL (TRIM (DESCRFORMAL), '-') DESCRFORMAL,
                      NVL (TRIM (SAA_DISPLAY_OPTION), '-') SAA_DISPLAY_OPTION
                 FROM SYSADM.PS_REPEAT_CODE_TBL@SASOURCE S
                WHERE ORA_ROWSCN > (SELECT OLD_MAX_SCN
                                      FROM CSSTG_OWNER.UM_STAGE_JOBS
                                     WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL')) S
           ON (    T.SETID = S.SETID
               AND T.REPEAT_SCHEME = S.REPEAT_SCHEME
               AND T.EFFDT = S.EFFDT
               AND T.REPEAT_CODE = S.REPEAT_CODE
               AND T.SRC_SYS_ID = 'CS90')
   WHEN MATCHED
   THEN
      UPDATE SET
         T.DESCR = S.DESCR,
         T.DESCRSHORT = S.DESCRSHORT,
         T.EARN_CREDIT = S.EARN_CREDIT,
         T.UNITS_ATTEMPTED = S.UNITS_ATTEMPTED,
         T.INCLUDE_IN_GPA = S.INCLUDE_IN_GPA,
         T.PRINT_REPEAT_DESCR = S.PRINT_REPEAT_DESCR,
         T.PRINT_REPEAT_DATE = S.PRINT_REPEAT_DATE,
         T.DESCRFORMAL = S.DESCRFORMAL,
         T.SAA_DISPLAY_OPTION = S.SAA_DISPLAY_OPTION,
         T.DATA_ORIGIN = 'S',
         T.LASTUPD_EW_DTTM = SYSDATE,
         T.BATCH_SID = 1234
              WHERE    T.DESCR <> S.DESCR
                    OR T.DESCRSHORT <> S.DESCRSHORT
                    OR T.EARN_CREDIT <> S.EARN_CREDIT
                    OR T.UNITS_ATTEMPTED <> S.UNITS_ATTEMPTED
                    OR T.INCLUDE_IN_GPA <> S.INCLUDE_IN_GPA
                    OR T.PRINT_REPEAT_DESCR <> S.PRINT_REPEAT_DESCR
                    OR T.PRINT_REPEAT_DATE <> S.PRINT_REPEAT_DATE
                    OR T.DESCRFORMAL <> S.DESCRFORMAL
                    OR T.SAA_DISPLAY_OPTION <> S.SAA_DISPLAY_OPTION
                    OR T.DATA_ORIGIN = 'D'
   WHEN NOT MATCHED
   THEN
      INSERT     (T.SETID,
                  T.REPEAT_SCHEME,
                  T.EFFDT,
                  T.REPEAT_CODE,
                  T.SRC_SYS_ID,
                  T.DESCR,
                  T.DESCRSHORT,
                  T.EARN_CREDIT,
                  T.UNITS_ATTEMPTED,
                  T.INCLUDE_IN_GPA,
                  T.PRINT_REPEAT_DESCR,
                  T.PRINT_REPEAT_DATE,
                  T.DESCRFORMAL,
                  T.SAA_DISPLAY_OPTION,
                  T.LOAD_ERROR,
                  T.DATA_ORIGIN,
                  T.CREATED_EW_DTTM,
                  T.LASTUPD_EW_DTTM,
                  T.BATCH_SID)
          VALUES (S.SETID,
                  S.REPEAT_SCHEME,
                  S.EFFDT,
                  S.REPEAT_CODE,
                  'CS90',
                  S.DESCR,
                  S.DESCRSHORT,
                  S.EARN_CREDIT,
                  S.UNITS_ATTEMPTED,
                  S.INCLUDE_IN_GPA,
                  S.PRINT_REPEAT_DESCR,
                  S.PRINT_REPEAT_DATE,
                  S.DESCRFORMAL,
                  S.SAA_DISPLAY_OPTION,
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
         '# of PS_REPEAT_CODE_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_REPEAT_CODE_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_REPEAT_CODE_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_REPEAT_CODE_TBL';


   UPDATE CSSTG_OWNER.PS_REPEAT_CODE_TBL T
      SET T.DATA_ORIGIN = 'D', T.LASTUPD_EW_DTTM = SYSDATE
    WHERE     T.DATA_ORIGIN <> 'D'
          AND EXISTS
                 (SELECT 1
                    FROM (SELECT SETID,
                                 REPEAT_SCHEME,
                                 EFFDT,
                                 REPEAT_CODE
                            FROM CSSTG_OWNER.PS_REPEAT_CODE_TBL T2
                           WHERE (SELECT DELETE_FLG
                                    FROM CSSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL') =
                                    'Y'
                          MINUS
                          SELECT SETID,
                                 REPEAT_SCHEME,
                                 EFFDT,
                                 REPEAT_CODE
                            FROM SYSADM.PS_REPEAT_CODE_TBL@SASOURCE
                           WHERE (SELECT DELETE_FLG
                                    FROM CSSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL') =
                                    'Y') S
                   WHERE     T.SETID = S.SETID
                         AND T.REPEAT_SCHEME = S.REPEAT_SCHEME
                         AND T.EFFDT = S.EFFDT
                         AND T.REPEAT_CODE = S.REPEAT_CODE
                         AND T.SRC_SYS_ID = 'CS90');

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_REPEAT_CODE_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_REPEAT_CODE_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_REPEAT_CODE_TBL';

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
END PS_REPEAT_CODE_TBL_P;
/
