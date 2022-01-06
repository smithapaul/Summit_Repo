CREATE OR REPLACE PROCEDURE             "PS_STDNT_ATTR_TBL_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_STDNT_ATTR_TBL', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_STDNT_ATTR_TBL from PeopleSoft table PS_STDNT_ATTR_TBL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_STDNT_ATTR_TBL.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_STDNT_ATTR_TBL';
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
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_STDNT_ATTR_TBL@SASOURCE S)
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_STDNT_ATTR_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_STDNT_ATTR_TBL';

   MERGE /*+ use_hash(S,T) */
        INTO  CSSTG_OWNER.PS_STDNT_ATTR_TBL T
        USING (SELECT /*+ full(S) */
                     NVL (TRIM (INSTITUTION), '-') INSTITUTION,
                      NVL (TRIM (STDNT_ATTR), '-') STDNT_ATTR,
                      TO_DATE (
                         TO_CHAR (
                            CASE
                               WHEN EFFDT < '01-JAN-1800' THEN NULL
                               ELSE EFFDT
                            END,
                            'MM/DD/YYYY HH24:MI:SS'),
                         'MM/DD/YYYY HH24:MI:SS')
                         EFFDT,
                      NVL (TRIM (EFF_STATUS), '-') EFF_STATUS,
                      NVL (TRIM (DESCR), '-') DESCR,
                      NVL (TRIM (DESCRSHORT), '-') DESCRSHORT,
                      NVL (TRIM (ACAD_PROG), '-') ACAD_PROG,
                      NVL (TRIM (ACAD_CAREER), '-') ACAD_CAREER
                 FROM SYSADM.PS_STDNT_ATTR_TBL@SASOURCE S
                WHERE ORA_ROWSCN > (SELECT OLD_MAX_SCN
                                      FROM CSSTG_OWNER.UM_STAGE_JOBS
                                     WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL')) S
           ON (    T.INSTITUTION = S.INSTITUTION
               AND T.STDNT_ATTR = S.STDNT_ATTR
               AND T.EFFDT = S.EFFDT
               AND T.SRC_SYS_ID = 'CS90')
   WHEN MATCHED
   THEN
      UPDATE SET
         T.EFF_STATUS = S.EFF_STATUS,
         T.DESCR = S.DESCR,
         T.DESCRSHORT = S.DESCRSHORT,
         T.ACAD_PROG = S.ACAD_PROG,
         T.ACAD_CAREER = S.ACAD_CAREER,
         T.DATA_ORIGIN = 'S',
         T.LASTUPD_EW_DTTM = SYSDATE,
         T.BATCH_SID = 1234
              WHERE    T.EFF_STATUS <> S.EFF_STATUS
                    OR T.DESCR <> S.DESCR
                    OR T.DESCRSHORT <> S.DESCRSHORT
                    OR T.ACAD_PROG <> S.ACAD_PROG
                    OR T.ACAD_CAREER <> S.ACAD_CAREER
                    OR T.DATA_ORIGIN = 'D'
   WHEN NOT MATCHED
   THEN
      INSERT     (T.INSTITUTION,
                  T.STDNT_ATTR,
                  T.EFFDT,
                  T.SRC_SYS_ID,
                  T.EFF_STATUS,
                  T.DESCR,
                  T.DESCRSHORT,
                  T.ACAD_PROG,
                  T.ACAD_CAREER,
                  T.LOAD_ERROR,
                  T.DATA_ORIGIN,
                  T.CREATED_EW_DTTM,
                  T.LASTUPD_EW_DTTM,
                  T.BATCH_SID)
          VALUES (S.INSTITUTION,
                  S.STDNT_ATTR,
                  S.EFFDT,
                  'CS90',
                  S.EFF_STATUS,
                  S.DESCR,
                  S.DESCRSHORT,
                  S.ACAD_PROG,
                  S.ACAD_CAREER,
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
         '# of PS_STDNT_ATTR_TBL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STDNT_ATTR_TBL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ATTR_TBL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_STDNT_ATTR_TBL';

   UPDATE CSSTG_OWNER.PS_STDNT_ATTR_TBL T
      SET T.DATA_ORIGIN = 'D', T.LASTUPD_EW_DTTM = SYSDATE
    WHERE     T.DATA_ORIGIN <> 'D'
          AND EXISTS
                 (SELECT 1
                    FROM (SELECT INSTITUTION, STDNT_ATTR, EFFDT
                            FROM CSSTG_OWNER.PS_STDNT_ATTR_TBL T2
                           WHERE (SELECT DELETE_FLG
                                    FROM CSSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL') =
                                    'Y'
                          MINUS
                          SELECT INSTITUTION, STDNT_ATTR, EFFDT
                            FROM SYSADM.PS_STDNT_ATTR_TBL@SASOURCE
                           WHERE (SELECT DELETE_FLG
                                    FROM CSSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL') =
                                    'Y'-- AND EMPLID <>'00386824'
                         ) S
                   WHERE     T.INSTITUTION = S.INSTITUTION
                         AND T.STDNT_ATTR = S.STDNT_ATTR
                         AND T.EFFDT = S.EFFDT
                         AND T.SRC_SYS_ID = 'CS90');

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_STDNT_ATTR_TBL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_STDNT_ATTR_TBL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_STDNT_ATTR_TBL';

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
END PS_STDNT_ATTR_TBL_P;
/
