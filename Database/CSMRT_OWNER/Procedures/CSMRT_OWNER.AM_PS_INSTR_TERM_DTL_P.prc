DROP PROCEDURE CSMRT_OWNER.AM_PS_INSTR_TERM_DTL_P
/

--
-- AM_PS_INSTR_TERM_DTL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_INSTR_TERM_DTL_P" IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_INSTR_TERM_DTL from PeopleSoft table PS_INSTR_TERM_DTL.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_INSTR_TERM_DTL.sql
   --VXX    07/06/2021,            Kieu ,Srikanth - Added EMPLID or COMMON_ID additional filter logic 
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_INSTR_TERM_DTL';
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
    WHERE TABLE_NAME = 'PS_INSTR_TERM_DTL';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_INSTR_TERM_DTL@AMSOURCE S)
    WHERE TABLE_NAME = 'PS_INSTR_TERM_DTL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into AMSTG_OWNER.PS_INSTR_TERM_DTL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into AMSTG_OWNER.PS_INSTR_TERM_DTL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_INSTR_TERM_DTL T
using (select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(STRM),'-') STRM,
nvl(INST_TRM_DET_SEQ,0) INST_TRM_DET_SEQ,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(INSTR_ENTRY_TYPE),'-') INSTR_ENTRY_TYPE,
nvl(trim(CONCATENATED_KEYS),'-') CONCATENATED_KEYS,
nvl(EMPL_RCD,0) EMPL_RCD,
nvl(trim(ASSIGN_TYPE),'-') ASSIGN_TYPE,
nvl(WEEK_WORKLOAD_HRS,0) WEEK_WORKLOAD_HRS,
nvl(trim(LOAD_CALC_APPLY),'-') LOAD_CALC_APPLY,
nvl(trim(CRSE_ID),'-') CRSE_ID,
nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR,
nvl(CLASS_NBR,0) CLASS_NBR,
nvl(CLASS_MTG_NBR,0) CLASS_MTG_NBR,
nvl(ASSIGNMENT_PCT,0) ASSIGNMENT_PCT,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(trim(SCTN_COMBINED_ID),'-') SCTN_COMBINED_ID
from SYSADM.PS_INSTR_TERM_DTL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_INSTR_TERM_DTL') 
AND LENGTH(EMPLID) = 8 AND EMPLID BETWEEN '00000000' AND '99999999') S
   on (
T.EMPLID = S.EMPLID and
T.INSTITUTION = S.INSTITUTION and
T.STRM = S.STRM and
T.INST_TRM_DET_SEQ = S.INST_TRM_DET_SEQ and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DESCR = S.DESCR,
T.INSTR_ENTRY_TYPE = S.INSTR_ENTRY_TYPE,
T.CONCATENATED_KEYS = S.CONCATENATED_KEYS,
T.EMPL_RCD = S.EMPL_RCD,
T.ASSIGN_TYPE = S.ASSIGN_TYPE,
T.WEEK_WORKLOAD_HRS = S.WEEK_WORKLOAD_HRS,
T.LOAD_CALC_APPLY = S.LOAD_CALC_APPLY,
T.CRSE_ID = S.CRSE_ID,
T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR,
T.CLASS_NBR = S.CLASS_NBR,
T.CLASS_MTG_NBR = S.CLASS_MTG_NBR,
T.ASSIGNMENT_PCT = S.ASSIGNMENT_PCT,
T.SESSION_CODE = S.SESSION_CODE,
T.SCTN_COMBINED_ID = S.SCTN_COMBINED_ID,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DESCR <> S.DESCR or
T.INSTR_ENTRY_TYPE <> S.INSTR_ENTRY_TYPE or
T.CONCATENATED_KEYS <> S.CONCATENATED_KEYS or
T.EMPL_RCD <> S.EMPL_RCD or
T.ASSIGN_TYPE <> S.ASSIGN_TYPE or
T.WEEK_WORKLOAD_HRS <> S.WEEK_WORKLOAD_HRS or
T.LOAD_CALC_APPLY <> S.LOAD_CALC_APPLY or
T.CRSE_ID <> S.CRSE_ID or
T.CRSE_OFFER_NBR <> S.CRSE_OFFER_NBR or
T.CLASS_NBR <> S.CLASS_NBR or
T.CLASS_MTG_NBR <> S.CLASS_MTG_NBR or
T.ASSIGNMENT_PCT <> S.ASSIGNMENT_PCT or
T.SESSION_CODE <> S.SESSION_CODE or
T.SCTN_COMBINED_ID <> S.SCTN_COMBINED_ID or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.EMPLID,
T.INSTITUTION,
T.STRM,
T.INST_TRM_DET_SEQ,
T.SRC_SYS_ID,
T.DESCR,
T.INSTR_ENTRY_TYPE,
T.CONCATENATED_KEYS,
T.EMPL_RCD,
T.ASSIGN_TYPE,
T.WEEK_WORKLOAD_HRS,
T.LOAD_CALC_APPLY,
T.CRSE_ID,
T.CRSE_OFFER_NBR,
T.CLASS_NBR,
T.CLASS_MTG_NBR,
T.ASSIGNMENT_PCT,
T.SESSION_CODE,
T.SCTN_COMBINED_ID,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.EMPLID,
S.INSTITUTION,
S.STRM,
S.INST_TRM_DET_SEQ,
'CS90',
S.DESCR,
S.INSTR_ENTRY_TYPE,
S.CONCATENATED_KEYS,
S.EMPL_RCD,
S.ASSIGN_TYPE,
S.WEEK_WORKLOAD_HRS,
S.LOAD_CALC_APPLY,
S.CRSE_ID,
S.CRSE_OFFER_NBR,
S.CLASS_NBR,
S.CLASS_MTG_NBR,
S.ASSIGNMENT_PCT,
S.SESSION_CODE,
S.SCTN_COMBINED_ID,
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
         '# of PS_INSTR_TERM_DTL rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_INSTR_TERM_DTL',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_INSTR_TERM_DTL';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_INSTR_TERM_DTL';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on AMSTG_OWNER.PS_INSTR_TERM_DTL';

update AMSTG_OWNER.PS_INSTR_TERM_DTL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, STRM, INST_TRM_DET_SEQ
   from AMSTG_OWNER.PS_INSTR_TERM_DTL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_INSTR_TERM_DTL') = 'Y'
  minus
 select EMPLID, INSTITUTION, STRM, INST_TRM_DET_SEQ
   from SYSADM.PS_INSTR_TERM_DTL@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_INSTR_TERM_DTL') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
  AND T.INSTITUTION = S.INSTITUTION
  AND T.STRM = S.STRM
  AND T.INST_TRM_DET_SEQ = S.INST_TRM_DET_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;
   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_INSTR_TERM_DTL rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_INSTR_TERM_DTL',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_INSTR_TERM_DTL';

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
END AM_PS_INSTR_TERM_DTL_P;
/
