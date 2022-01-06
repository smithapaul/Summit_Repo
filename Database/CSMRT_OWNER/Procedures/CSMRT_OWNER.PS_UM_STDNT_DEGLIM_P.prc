CREATE OR REPLACE PROCEDURE             "PS_UM_STDNT_DEGLIM_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_UM_STDNT_DEGLIM'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_UM_STDNT_DEGLIM', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_UM_STDNT_DEGLIM'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_UM_STDNT_DEGLIM from PeopleSoft table PS_UM_STDNT_DEGLIM.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_UM_STDNT_DEGLIM.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_UM_STDNT_DEGLIM';
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
    WHERE TABLE_NAME = 'PS_UM_STDNT_DEGLIM';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_UM_STDNT_DEGLIM@SASOURCE S)
    WHERE TABLE_NAME = 'PS_UM_STDNT_DEGLIM';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_UM_STDNT_DEGLIM';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_UM_STDNT_DEGLIM';

merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_STDNT_DEGLIM T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR,
nvl(trim(ACAD_PROG),'-') ACAD_PROG,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
nvl(trim(ACAD_SUB_PLAN),'-') ACAD_SUB_PLAN,
EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(UM_OVRRIDE_EXTENSN),'-') UM_OVRRIDE_EXTENSN,
nvl(trim(STRM),'-') STRM,
nvl(trim(STRM_1),'-') STRM_1,
nvl(YEARS,0) YEARS,
-- nvl(trim(COMMENTS_MSGS),'-') COMMENTS_MSGS
COMMENTS_MSGS
from SYSADM.PS_UM_STDNT_DEGLIM@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_DEGLIM')
  and EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8  ) S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.ACAD_CAREER = S.ACAD_CAREER and
T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and
T.ACAD_PROG = S.ACAD_PROG and
T.ACAD_PLAN = S.ACAD_PLAN and
T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.UM_OVRRIDE_EXTENSN = S.UM_OVRRIDE_EXTENSN,
T.STRM = S.STRM,
T.STRM_1 = S.STRM_1,
T.YEARS = S.YEARS,
T.COMMENTS_MSGS = S.COMMENTS_MSGS,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.UM_OVRRIDE_EXTENSN <> S.UM_OVRRIDE_EXTENSN or
T.STRM <> S.STRM or
T.STRM_1 <> S.STRM_1 or
T.YEARS <> S.YEARS or
T.COMMENTS_MSGS <> S.COMMENTS_MSGS or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.ACAD_CAREER,
T.STDNT_CAR_NBR,
T.ACAD_PROG,
T.ACAD_PLAN,
T.ACAD_SUB_PLAN,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.UM_OVRRIDE_EXTENSN,
T.STRM,
T.STRM_1,
T.YEARS,
T.COMMENTS_MSGS,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.INSTITUTION,
S.ACAD_CAREER,
S.STDNT_CAR_NBR,
S.ACAD_PROG,
S.ACAD_PLAN,
S.ACAD_SUB_PLAN,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.UM_OVRRIDE_EXTENSN,
S.STRM,
S.STRM_1,
S.YEARS,
S.COMMENTS_MSGS,
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
         '# of PS_UM_STDNT_DEGLIM rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_UM_STDNT_DEGLIM',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_UM_STDNT_DEGLIM';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_STDNT_DEGLIM';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_STDNT_DEGLIM';

update CSSTG_OWNER.PS_UM_STDNT_DEGLIM T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, ACAD_PLAN, ACAD_SUB_PLAN, EFFDT
   from CSSTG_OWNER.PS_UM_STDNT_DEGLIM T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_DEGLIM') = 'Y'
  minus
 select nvl(trim(EMPLID),'-') EMPLID,
        nvl(trim(INSTITUTION),'-') INSTITUTION,
        nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
        nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR,
        nvl(trim(ACAD_PROG),'-') ACAD_PROG,
        nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
        nvl(trim(ACAD_SUB_PLAN),'-') ACAD_SUB_PLAN,
        EFFDT
   from SYSADM.PS_UM_STDNT_DEGLIM@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_STDNT_DEGLIM') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
  AND T.INSTITUTION = S.INSTITUTION
  AND T.ACAD_CAREER = S.ACAD_CAREER
  AND T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
  AND T.ACAD_PROG = S.ACAD_PROG
  AND T.ACAD_PLAN = S.ACAD_PLAN
  AND T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN
  AND T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_UM_STDNT_DEGLIM rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_UM_STDNT_DEGLIM',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_UM_STDNT_DEGLIM';

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
END PS_UM_STDNT_DEGLIM_P;
/
