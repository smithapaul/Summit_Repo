CREATE OR REPLACE PROCEDURE             "PS_ACAD_STDNG_ACTN_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ACAD_STDNG_ACTN'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_ACAD_STDNG_ACTN', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_ACAD_STDNG_ACTN'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_ACAD_STDNG_ACTN from PeopleSoft table PS_ACAD_STDNG_ACTN.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_ACAD_STDNG_ACTN.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_ACAD_STDNG_ACTN';
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
    WHERE TABLE_NAME = 'PS_ACAD_STDNG_ACTN';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_ACAD_STDNG_ACTN@SASOURCE S)
    WHERE TABLE_NAME = 'PS_ACAD_STDNG_ACTN';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_ACAD_STDNG_ACTN';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_ACAD_STDNG_ACTN';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_STDNG_ACTN T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(STRM),'-') STRM,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(EFFSEQ,0) EFFSEQ,
nvl(trim(ACAD_STNDNG_ACTN),'-') ACAD_STNDNG_ACTN,
nvl(trim(OVERRIDE_MANUAL),'-') OVERRIDE_MANUAL,
nvl(trim(ACAD_PROG),'-') ACAD_PROG,
nvl(trim(ACAD_STNDNG_STAT),'-') ACAD_STNDNG_STAT,
to_date(to_char(case when ACTION_DT < '01-JAN-1800' then NULL else ACTION_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') ACTION_DT,
nvl(trim(OPRID),'-') OPRID
from SYSADM.PS_ACAD_STDNG_ACTN@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_STDNG_ACTN') ) S
   on (
T.EMPLID = S.EMPLID and
T.ACAD_CAREER = S.ACAD_CAREER and
T.INSTITUTION = S.INSTITUTION and
T.STRM = S.STRM and
T.EFFDT = S.EFFDT and
T.EFFSEQ = S.EFFSEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.ACAD_STNDNG_ACTN = S.ACAD_STNDNG_ACTN,
T.OVERRIDE_MANUAL = S.OVERRIDE_MANUAL,
T.ACAD_PROG = S.ACAD_PROG,
T.ACAD_STNDNG_STAT = S.ACAD_STNDNG_STAT,
T.ACTION_DT = S.ACTION_DT,
T.OPRID = S.OPRID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.ACAD_STNDNG_ACTN <> S.ACAD_STNDNG_ACTN or
T.OVERRIDE_MANUAL <> S.OVERRIDE_MANUAL or
T.ACAD_PROG <> S.ACAD_PROG or
T.ACAD_STNDNG_STAT <> S.ACAD_STNDNG_STAT or
nvl(trim(T.ACTION_DT),0) <> nvl(trim(S.ACTION_DT),0) or
T.OPRID <> S.OPRID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.ACAD_CAREER,
T.INSTITUTION,
T.STRM,
T.EFFDT,
T.EFFSEQ,
T.SRC_SYS_ID,
T.ACAD_STNDNG_ACTN,
T.OVERRIDE_MANUAL,
T.ACAD_PROG,
T.ACAD_STNDNG_STAT,
T.ACTION_DT,
T.OPRID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.ACAD_CAREER,
S.INSTITUTION,
S.STRM,
S.EFFDT,
S.EFFSEQ,
'CS90',
S.ACAD_STNDNG_ACTN,
S.OVERRIDE_MANUAL,
S.ACAD_PROG,
S.ACAD_STNDNG_STAT,
S.ACTION_DT,
S.OPRID,
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
         '# of PS_ACAD_STDNG_ACTN rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ACAD_STDNG_ACTN',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_ACAD_STDNG_ACTN';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_STDNG_ACTN';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_STDNG_ACTN';

update CSSTG_OWNER.PS_ACAD_STDNG_ACTN T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, INSTITUTION, STRM, EFFDT, EFFSEQ
   from CSSTG_OWNER.PS_ACAD_STDNG_ACTN T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_STDNG_ACTN') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, INSTITUTION, STRM, EFFDT, EFFSEQ
   from SYSADM.PS_ACAD_STDNG_ACTN@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_STDNG_ACTN') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
  AND T.ACAD_CAREER = S.ACAD_CAREER
  AND T.STRM = S.STRM
  AND T.INSTITUTION = S.INSTITUTION
  AND T.EFFDT = S.EFFDT
  AND T.EFFSEQ = S.EFFSEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_ACAD_STDNG_ACTN rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_ACAD_STDNG_ACTN',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_ACAD_STDNG_ACTN';

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
END PS_ACAD_STDNG_ACTN_P;
/
