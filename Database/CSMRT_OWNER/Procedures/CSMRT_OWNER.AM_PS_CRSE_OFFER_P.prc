DROP PROCEDURE CSMRT_OWNER.AM_PS_CRSE_OFFER_P
/

--
-- AM_PS_CRSE_OFFER_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_CRSE_OFFER_P" IS

   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_CRSE_OFFER from PeopleSoft table PS_CRSE_OFFER.
   --
   -- V01  SMT-xxxx 07/10/2017,    Preethi Lodha
   --                              Converted from PS_CRSE_OFFER.sql
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'AM_PS_CRSE_OFFER';
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
    WHERE TABLE_NAME = 'PS_CRSE_OFFER';

   strSqlCommand := 'commit';
   COMMIT;

   strSqlCommand := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_CRSE_OFFER@AMSOURCE S)
    WHERE TABLE_NAME = 'PS_CRSE_OFFER';

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 := 'Merging data into AMSTG_OWNER.PS_CRSE_OFFER';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into AMSTG_OWNER.PS_CRSE_OFFER';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CRSE_OFFER T
using (select /*+ full(S) */
nvl(trim(CRSE_ID),'-') CRSE_ID,
--to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
to_date(to_char(case when EFFDT < '01-JAN-1800' then to_date('01-JAN-1800') else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,   -- Jan 2022 
nvl(CRSE_OFFER_NBR,0) CRSE_OFFER_NBR,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ACAD_GROUP),'-') ACAD_GROUP,
nvl(trim(SUBJECT),'-') SUBJECT,
nvl(trim(CATALOG_NBR),'-') CATALOG_NBR,
nvl(trim(COURSE_APPROVED),'-') COURSE_APPROVED,
nvl(trim(CAMPUS),'-') CAMPUS,
nvl(trim(SCHEDULE_PRINT),'-') SCHEDULE_PRINT,
nvl(trim(CATALOG_PRINT),'-') CATALOG_PRINT,
nvl(trim(SCHED_PRINT_INSTR),'-') SCHED_PRINT_INSTR,
nvl(trim(ACAD_ORG),'-') ACAD_ORG,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(SPLIT_OWNER),'-') SPLIT_OWNER,
nvl(trim(SCHED_TERM_ROLL),'-') SCHED_TERM_ROLL,
nvl(trim(RQRMNT_GROUP),'-') RQRMNT_GROUP,
nvl(trim(CIP_CODE),'-') CIP_CODE,
nvl(trim(HEGIS_CODE),'-') HEGIS_CODE,
nvl(trim(USE_BLIND_GRADING),'-') USE_BLIND_GRADING,
nvl(trim(RCV_FROM_ITEM_TYPE),'-') RCV_FROM_ITEM_TYPE,
nvl(trim(AP_BUS_UNIT),'-') AP_BUS_UNIT,
nvl(trim(AP_LEDGER),'-') AP_LEDGER,
nvl(trim(AP_ACCOUNT),'-') AP_ACCOUNT,
nvl(trim(AP_DEPTID),'-') AP_DEPTID,
nvl(trim(AP_PROJ_ID),'-') AP_PROJ_ID,
nvl(trim(AP_PRODUCT),'-') AP_PRODUCT,
nvl(trim(AP_FUND_CODE),'-') AP_FUND_CODE,
nvl(trim(AP_PROG_CODE),'-') AP_PROG_CODE,
nvl(trim(AP_CLASS_FLD),'-') AP_CLASS_FLD,
nvl(trim(AP_AFFILIATE),'-') AP_AFFILIATE,
nvl(trim(AP_OP_UNIT),'-') AP_OP_UNIT,
nvl(trim(AP_ALTACCT),'-') AP_ALTACCT,
nvl(trim(AP_BUD_REF),'-') AP_BUD_REF,
nvl(trim(AP_CF1),'-') AP_CF1,
nvl(trim(AP_CF2),'-') AP_CF2,
nvl(trim(AP_CF3),'-') AP_CF3,
nvl(trim(AP_AFF_INT1),'-') AP_AFF_INT1,
nvl(trim(AP_AFF_INT2),'-') AP_AFF_INT2,
nvl(trim(WRITEOFF_BUS_UNIT),'-') WRITEOFF_BUS_UNIT,
nvl(trim(WRITEOFF_LEDGER),'-') WRITEOFF_LEDGER,
nvl(trim(WRITEOFF_ACCOUNT),'-') WRITEOFF_ACCOUNT,
nvl(trim(WRITEOFF_DEPTID),'-') WRITEOFF_DEPTID,
nvl(trim(WRITEOFF_PROJ_ID),'-') WRITEOFF_PROJ_ID,
nvl(trim(WRITEOFF_PRODUCT),'-') WRITEOFF_PRODUCT,
nvl(trim(WRITEOFF_FUND_CODE),'-') WRITEOFF_FUND_CODE,
nvl(trim(WRITEOFF_PROG_CODE),'-') WRITEOFF_PROG_CODE,
nvl(trim(WRITEOFF_CLASS_FLD),'-') WRITEOFF_CLASS_FLD,
nvl(trim(WRITEOFF_AFFILIATE),'-') WRITEOFF_AFFILIATE,
nvl(trim(WRITEOFF_OP_UNIT),'-') WRITEOFF_OP_UNIT,
nvl(trim(WRITEOFF_ALTACCT),'-') WRITEOFF_ALTACCT,
nvl(trim(WRITEOFF_BUD_REF),'-') WRITEOFF_BUD_REF,
nvl(trim(WRITEOFF_CF1),'-') WRITEOFF_CF1,
nvl(trim(WRITEOFF_CF2),'-') WRITEOFF_CF2,
nvl(trim(WRITEOFF_CF3),'-') WRITEOFF_CF3,
nvl(trim(WRITEOFF_AFF_INT1),'-') WRITEOFF_AFF_INT1,
nvl(trim(WRITEOFF_AFF_INT2),'-') WRITEOFF_AFF_INT2,
nvl(trim(EXT_WRITEOFF),'-') EXT_WRITEOFF,
nvl(trim(GL_INTERFACE_REQ),'-') GL_INTERFACE_REQ,
nvl(trim(SEL_GROUP),'-') SEL_GROUP,
nvl(trim(SCHEDULE_COURSE),'-') SCHEDULE_COURSE,
nvl(trim(DYN_CLASS_DATA),'-') DYN_CLASS_DATA,
nvl(trim(OEE_IND),'-') OEE_IND,
nvl(trim(OEE_DYN_DATE_RULE),'-') OEE_DYN_DATE_RULE
from SYSADM.PS_CRSE_OFFER@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_OFFER') ) S
   on (
T.CRSE_ID = S.CRSE_ID and
T.EFFDT = S.EFFDT and
T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.INSTITUTION = S.INSTITUTION,
T.ACAD_GROUP = S.ACAD_GROUP,
T.SUBJECT = S.SUBJECT,
T.CATALOG_NBR = S.CATALOG_NBR,
T.COURSE_APPROVED = S.COURSE_APPROVED,
T.CAMPUS = S.CAMPUS,
T.SCHEDULE_PRINT = S.SCHEDULE_PRINT,
T.CATALOG_PRINT = S.CATALOG_PRINT,
T.SCHED_PRINT_INSTR = S.SCHED_PRINT_INSTR,
T.ACAD_ORG = S.ACAD_ORG,
T.ACAD_CAREER = S.ACAD_CAREER,
T.SPLIT_OWNER = S.SPLIT_OWNER,
T.SCHED_TERM_ROLL = S.SCHED_TERM_ROLL,
T.RQRMNT_GROUP = S.RQRMNT_GROUP,
T.CIP_CODE = S.CIP_CODE,
T.HEGIS_CODE = S.HEGIS_CODE,
T.USE_BLIND_GRADING = S.USE_BLIND_GRADING,
T.RCV_FROM_ITEM_TYPE = S.RCV_FROM_ITEM_TYPE,
T.AP_BUS_UNIT = S.AP_BUS_UNIT,
T.AP_LEDGER = S.AP_LEDGER,
T.AP_ACCOUNT = S.AP_ACCOUNT,
T.AP_DEPTID = S.AP_DEPTID,
T.AP_PROJ_ID = S.AP_PROJ_ID,
T.AP_PRODUCT = S.AP_PRODUCT,
T.AP_FUND_CODE = S.AP_FUND_CODE,
T.AP_PROG_CODE = S.AP_PROG_CODE,
T.AP_CLASS_FLD = S.AP_CLASS_FLD,
T.AP_AFFILIATE = S.AP_AFFILIATE,
T.AP_OP_UNIT = S.AP_OP_UNIT,
T.AP_ALTACCT = S.AP_ALTACCT,
T.AP_BUD_REF = S.AP_BUD_REF,
T.AP_CF1 = S.AP_CF1,
T.AP_CF2 = S.AP_CF2,
T.AP_CF3 = S.AP_CF3,
T.AP_AFF_INT1 = S.AP_AFF_INT1,
T.AP_AFF_INT2 = S.AP_AFF_INT2,
T.WRITEOFF_BUS_UNIT = S.WRITEOFF_BUS_UNIT,
T.WRITEOFF_LEDGER = S.WRITEOFF_LEDGER,
T.WRITEOFF_ACCOUNT = S.WRITEOFF_ACCOUNT,
T.WRITEOFF_DEPTID = S.WRITEOFF_DEPTID,
T.WRITEOFF_PROJ_ID = S.WRITEOFF_PROJ_ID,
T.WRITEOFF_PRODUCT = S.WRITEOFF_PRODUCT,
T.WRITEOFF_FUND_CODE = S.WRITEOFF_FUND_CODE,
T.WRITEOFF_PROG_CODE = S.WRITEOFF_PROG_CODE,
T.WRITEOFF_CLASS_FLD = S.WRITEOFF_CLASS_FLD,
T.WRITEOFF_AFFILIATE = S.WRITEOFF_AFFILIATE,
T.WRITEOFF_OP_UNIT = S.WRITEOFF_OP_UNIT,
T.WRITEOFF_ALTACCT = S.WRITEOFF_ALTACCT,
T.WRITEOFF_BUD_REF = S.WRITEOFF_BUD_REF,
T.WRITEOFF_CF1 = S.WRITEOFF_CF1,
T.WRITEOFF_CF2 = S.WRITEOFF_CF2,
T.WRITEOFF_CF3 = S.WRITEOFF_CF3,
T.WRITEOFF_AFF_INT1 = S.WRITEOFF_AFF_INT1,
T.WRITEOFF_AFF_INT2 = S.WRITEOFF_AFF_INT2,
T.EXT_WRITEOFF = S.EXT_WRITEOFF,
T.GL_INTERFACE_REQ = S.GL_INTERFACE_REQ,
T.SEL_GROUP = S.SEL_GROUP,
T.SCHEDULE_COURSE = S.SCHEDULE_COURSE,
T.DYN_CLASS_DATA = S.DYN_CLASS_DATA,
T.OEE_IND = S.OEE_IND,
T.OEE_DYN_DATE_RULE = S.OEE_DYN_DATE_RULE,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.INSTITUTION <> S.INSTITUTION or
T.ACAD_GROUP <> S.ACAD_GROUP or
T.SUBJECT <> S.SUBJECT or
T.CATALOG_NBR <> S.CATALOG_NBR or
T.COURSE_APPROVED <> S.COURSE_APPROVED or
T.CAMPUS <> S.CAMPUS or
T.SCHEDULE_PRINT <> S.SCHEDULE_PRINT or
T.CATALOG_PRINT <> S.CATALOG_PRINT or
T.SCHED_PRINT_INSTR <> S.SCHED_PRINT_INSTR or
T.ACAD_ORG <> S.ACAD_ORG or
T.ACAD_CAREER <> S.ACAD_CAREER or
T.SPLIT_OWNER <> S.SPLIT_OWNER or
T.SCHED_TERM_ROLL <> S.SCHED_TERM_ROLL or
T.RQRMNT_GROUP <> S.RQRMNT_GROUP or
T.CIP_CODE <> S.CIP_CODE or
T.HEGIS_CODE <> S.HEGIS_CODE or
T.USE_BLIND_GRADING <> S.USE_BLIND_GRADING or
T.RCV_FROM_ITEM_TYPE <> S.RCV_FROM_ITEM_TYPE or
T.AP_BUS_UNIT <> S.AP_BUS_UNIT or
T.AP_LEDGER <> S.AP_LEDGER or
T.AP_ACCOUNT <> S.AP_ACCOUNT or
T.AP_DEPTID <> S.AP_DEPTID or
T.AP_PROJ_ID <> S.AP_PROJ_ID or
T.AP_PRODUCT <> S.AP_PRODUCT or
T.AP_FUND_CODE <> S.AP_FUND_CODE or
T.AP_PROG_CODE <> S.AP_PROG_CODE or
T.AP_CLASS_FLD <> S.AP_CLASS_FLD or
T.AP_AFFILIATE <> S.AP_AFFILIATE or
T.AP_OP_UNIT <> S.AP_OP_UNIT or
T.AP_ALTACCT <> S.AP_ALTACCT or
T.AP_BUD_REF <> S.AP_BUD_REF or
T.AP_CF1 <> S.AP_CF1 or
T.AP_CF2 <> S.AP_CF2 or
T.AP_CF3 <> S.AP_CF3 or
T.AP_AFF_INT1 <> S.AP_AFF_INT1 or
T.AP_AFF_INT2 <> S.AP_AFF_INT2 or
T.WRITEOFF_BUS_UNIT <> S.WRITEOFF_BUS_UNIT or
T.WRITEOFF_LEDGER <> S.WRITEOFF_LEDGER or
T.WRITEOFF_ACCOUNT <> S.WRITEOFF_ACCOUNT or
T.WRITEOFF_DEPTID <> S.WRITEOFF_DEPTID or
T.WRITEOFF_PROJ_ID <> S.WRITEOFF_PROJ_ID or
T.WRITEOFF_PRODUCT <> S.WRITEOFF_PRODUCT or
T.WRITEOFF_FUND_CODE <> S.WRITEOFF_FUND_CODE or
T.WRITEOFF_PROG_CODE <> S.WRITEOFF_PROG_CODE or
T.WRITEOFF_CLASS_FLD <> S.WRITEOFF_CLASS_FLD or
T.WRITEOFF_AFFILIATE <> S.WRITEOFF_AFFILIATE or
T.WRITEOFF_OP_UNIT <> S.WRITEOFF_OP_UNIT or
T.WRITEOFF_ALTACCT <> S.WRITEOFF_ALTACCT or
T.WRITEOFF_BUD_REF <> S.WRITEOFF_BUD_REF or
T.WRITEOFF_CF1 <> S.WRITEOFF_CF1 or
T.WRITEOFF_CF2 <> S.WRITEOFF_CF2 or
T.WRITEOFF_CF3 <> S.WRITEOFF_CF3 or
T.WRITEOFF_AFF_INT1 <> S.WRITEOFF_AFF_INT1 or
T.WRITEOFF_AFF_INT2 <> S.WRITEOFF_AFF_INT2 or
T.EXT_WRITEOFF <> S.EXT_WRITEOFF or
T.GL_INTERFACE_REQ <> S.GL_INTERFACE_REQ or
T.SEL_GROUP <> S.SEL_GROUP or
T.SCHEDULE_COURSE <> S.SCHEDULE_COURSE or
T.DYN_CLASS_DATA <> S.DYN_CLASS_DATA or
T.OEE_IND <> S.OEE_IND or
T.OEE_DYN_DATE_RULE <> S.OEE_DYN_DATE_RULE or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.CRSE_ID,
T.EFFDT,
T.CRSE_OFFER_NBR,
T.SRC_SYS_ID,
T.INSTITUTION,
T.ACAD_GROUP,
T.SUBJECT,
T.CATALOG_NBR,
T.COURSE_APPROVED,
T.CAMPUS,
T.SCHEDULE_PRINT,
T.CATALOG_PRINT,
T.SCHED_PRINT_INSTR,
T.ACAD_ORG,
T.ACAD_CAREER,
T.SPLIT_OWNER,
T.SCHED_TERM_ROLL,
T.RQRMNT_GROUP,
T.CIP_CODE,
T.HEGIS_CODE,
T.USE_BLIND_GRADING,
T.RCV_FROM_ITEM_TYPE,
T.AP_BUS_UNIT,
T.AP_LEDGER,
T.AP_ACCOUNT,
T.AP_DEPTID,
T.AP_PROJ_ID,
T.AP_PRODUCT,
T.AP_FUND_CODE,
T.AP_PROG_CODE,
T.AP_CLASS_FLD,
T.AP_AFFILIATE,
T.AP_OP_UNIT,
T.AP_ALTACCT,
T.AP_BUD_REF,
T.AP_CF1,
T.AP_CF2,
T.AP_CF3,
T.AP_AFF_INT1,
T.AP_AFF_INT2,
T.WRITEOFF_BUS_UNIT,
T.WRITEOFF_LEDGER,
T.WRITEOFF_ACCOUNT,
T.WRITEOFF_DEPTID,
T.WRITEOFF_PROJ_ID,
T.WRITEOFF_PRODUCT,
T.WRITEOFF_FUND_CODE,
T.WRITEOFF_PROG_CODE,
T.WRITEOFF_CLASS_FLD,
T.WRITEOFF_AFFILIATE,
T.WRITEOFF_OP_UNIT,
T.WRITEOFF_ALTACCT,
T.WRITEOFF_BUD_REF,
T.WRITEOFF_CF1,
T.WRITEOFF_CF2,
T.WRITEOFF_CF3,
T.WRITEOFF_AFF_INT1,
T.WRITEOFF_AFF_INT2,
T.EXT_WRITEOFF,
T.GL_INTERFACE_REQ,
T.SEL_GROUP,
T.SCHEDULE_COURSE,
T.DYN_CLASS_DATA,
T.OEE_IND,
T.OEE_DYN_DATE_RULE,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.CRSE_ID,
S.EFFDT,
S.CRSE_OFFER_NBR,
'CS90',
S.INSTITUTION,
S.ACAD_GROUP,
S.SUBJECT,
S.CATALOG_NBR,
S.COURSE_APPROVED,
S.CAMPUS,
S.SCHEDULE_PRINT,
S.CATALOG_PRINT,
S.SCHED_PRINT_INSTR,
S.ACAD_ORG,
S.ACAD_CAREER,
S.SPLIT_OWNER,
S.SCHED_TERM_ROLL,
S.RQRMNT_GROUP,
S.CIP_CODE,
S.HEGIS_CODE,
S.USE_BLIND_GRADING,
S.RCV_FROM_ITEM_TYPE,
S.AP_BUS_UNIT,
S.AP_LEDGER,
S.AP_ACCOUNT,
S.AP_DEPTID,
S.AP_PROJ_ID,
S.AP_PRODUCT,
S.AP_FUND_CODE,
S.AP_PROG_CODE,
S.AP_CLASS_FLD,
S.AP_AFFILIATE,
S.AP_OP_UNIT,
S.AP_ALTACCT,
S.AP_BUD_REF,
S.AP_CF1,
S.AP_CF2,
S.AP_CF3,
S.AP_AFF_INT1,
S.AP_AFF_INT2,
S.WRITEOFF_BUS_UNIT,
S.WRITEOFF_LEDGER,
S.WRITEOFF_ACCOUNT,
S.WRITEOFF_DEPTID,
S.WRITEOFF_PROJ_ID,
S.WRITEOFF_PRODUCT,
S.WRITEOFF_FUND_CODE,
S.WRITEOFF_PROG_CODE,
S.WRITEOFF_CLASS_FLD,
S.WRITEOFF_AFFILIATE,
S.WRITEOFF_OP_UNIT,
S.WRITEOFF_ALTACCT,
S.WRITEOFF_BUD_REF,
S.WRITEOFF_CF1,
S.WRITEOFF_CF2,
S.WRITEOFF_CF3,
S.WRITEOFF_AFF_INT1,
S.WRITEOFF_AFF_INT2,
S.EXT_WRITEOFF,
S.GL_INTERFACE_REQ,
S.SEL_GROUP,
S.SCHEDULE_COURSE,
S.DYN_CLASS_DATA,
S.OEE_IND,
S.OEE_DYN_DATE_RULE,
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
         '# of PS_CRSE_OFFER rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CRSE_OFFER',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);

   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_CRSE_OFFER';

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CRSE_OFFER';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CRSE_OFFER';

update AMSTG_OWNER.PS_CRSE_OFFER T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CRSE_ID, EFFDT, CRSE_OFFER_NBR
   from AMSTG_OWNER.PS_CRSE_OFFER T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_OFFER') = 'Y'
  minus
 select CRSE_ID, EFFDT, CRSE_OFFER_NBR
   from SYSADM.PS_CRSE_OFFER@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CRSE_OFFER') = 'Y' 
   ) S
 where T.CRSE_ID = S.CRSE_ID   
  AND T.EFFDT = S.EFFDT
  AND T.CRSE_OFFER_NBR = S.CRSE_OFFER_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_CRSE_OFFER rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_CRSE_OFFER',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);

   strMessage01 := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

   UPDATE AMSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_CRSE_OFFER';

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

END AM_PS_CRSE_OFFER_P;
/
