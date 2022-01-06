CREATE OR REPLACE PROCEDURE             "UM_F_ADM_APPL_TRANSFER_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_TRANSFER
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_TRANSFER';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_TRANSFER');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER disable constraint PK_UM_F_ADM_APPL_TRANSFER';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER';
insert /*+ append parallel(8) enable_parallel_dml */ INTO UM_F_ADM_APPL_TRANSFER
  with X as (
select /*+ inline parallel(8) */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       A as (SELECT /*+ inline parallel(8) */
                    DISTINCT APPLCNT_SID,
                             ADMIT_TERM_SID,
                             SRC_SYS_ID,
                             INSTITUTION_CD
               FROM CSMRT_OWNER.UM_F_ADM_APPL_STAT)
   SELECT /*+ parallel(8) */
          A.APPLCNT_SID,
          A.ADMIT_TERM_SID,
          NVL (F.MODEL_NBR, 0) MODEL_NBR,
          NVL (F.TRNSFR_EQVLNCY_GRP, 0) TRNSFR_EQVLNCY_GRP,
          NVL (F.TRNSFR_EQVLNCY_SEQ, 0) TRNSFR_EQVLNCY_SEQ,
          A.SRC_SYS_ID,
          NVL (F.PERSON_ID, '-') PERSON_ID,
          A.INSTITUTION_CD,
          NVL (F.ACAD_CAR_CD, '-') ACAD_CAR_CD,
          NVL (TO_CHAR (F.ARTICULATION_TERM), '-') ARTICULATION_TERM,
          NVL (F.ACAD_PROG_TARGET_SID, 2147483646) ACAD_PROG_TARGET_SID,        -- Feb 2020
          NVL (F.ACAD_PLAN_TARGET_SID, 2147483646) ACAD_PLAN_TARGET_SID,        -- Feb 2020
          NVL (CLASS_SID, 2147483646) CLASS_SID,
          NVL (CRSE_SID, 2147483646) CRSE_SID,
          NVL (EXT_CRSE_SID, 2147483646) EXT_CRSE_SID,
          NVL (EXT_ORG_SID, 2147483646) EXT_ORG_SID,
          NVL (EXT_TERM_SID, 2147483646) EXT_TERM_SID,
          NVL (EXT_TERM_YEAR_SID, 2147483646) EXT_TERM_YEAR_SID,
          NVL (F.APPLY_AGREEMENT_FLG,'-') APPLY_AGREEMENT_FLG,      -- Feb 2020
          NVL (COURSE_LEVEL, '-') COURSE_LEVEL,
          NVL (CRSE_GRADE_INPUT, '-') CRSE_GRADE_INPUT,
          NVL (CRSE_GRADE_OFF, '-') CRSE_GRADE_OFF,
          NVL (
             CASE
                WHEN F.TRNSFR_SRC_TYPE = 'E'
                THEN
                   (SELECT A.EXT_COURSE_DESCR
                      FROM CSMRT_OWNER.UM_F_EXT_ACAD_CRSE A
                     WHERE     F.PERSON_SID = A.PERSON_SID
                           AND F.EXT_ORG_SID = A.EXT_ORG_SID
                           AND F.EXT_COURSE_NBR = A.EXT_COURSE_NBR
                           AND F.SRC_SYS_ID = A.SRC_SYS_ID
                           and F.PERSON_SID <> 2147483646)
                WHEN F.TRNSFR_SRC_TYPE = 'I'
                THEN
                   (SELECT C.DESCR
                      FROM CSMRT_OWNER.UM_D_CLASS C
                     WHERE F.CLASS_SID = C.CLASS_SID)
                ELSE
                   F.DESCR
             END,
             '-')
             DESCR,
          NVL (EARN_CREDIT_FLG, '-') EARN_CREDIT_FLG,
          EXT_COURSE_NBR,
          NVL (GRADE_CATEGORY, '-') GRADE_CATEGORY,
          NVL (GRADING_SCHEME, '-') GRADING_SCHEME,
          NVL (GRADING_SCHEME_SD, '-') GRADING_SCHEME_SD,
          NVL (GRADING_SCHEME_LD, '-') GRADING_SCHEME_LD,
          NVL (GRADING_BASIS, '-') GRADING_BASIS,
       nvl(X1.XLATSHORTNAME,'-') GRADING_BASIS_SD,
       nvl(X1.XLATLONGNAME,'-') GRADING_BASIS_LD,
          GRD_PTS_PER_UNIT,
          NVL (MODEL_STATUS_SCH, '-') MODEL_STATUS_SCH,
       nvl(X2.XLATSHORTNAME,'-') MODEL_STATUS_SCH_SD,
       nvl(X2.XLATLONGNAME,'-') MODEL_STATUS_SCH_LD,
          NVL (MODEL_STATUS_TERM, '-') MODEL_STATUS_TERM,
       nvl(X3.XLATSHORTNAME,'-') MODEL_STATUS_TERM_SD,
       nvl(X3.XLATLONGNAME,'-') MODEL_STATUS_TERM_LD,
          POST_DT_TERM,
          NVL (REJECT_REASON, '-') REJECT_REASON,
       nvl(X4.XLATSHORTNAME,'-') REJECT_REASON_SD,
       nvl(X4.XLATLONGNAME,'-') REJECT_REASON_LD,
          NVL (REPEAT_CODE, '-') REPEAT_CODE,
          NVL (
             CASE
                WHEN F.TRNSFR_SRC_TYPE = 'E'
                THEN
                   (SELECT A.SCHOOL_CRSE_NBR
                      FROM CSMRT_OWNER.UM_F_EXT_ACAD_CRSE A
                     WHERE     F.PERSON_SID = A.PERSON_SID
                           AND F.EXT_ORG_SID = A.EXT_ORG_SID
                           AND F.EXT_COURSE_NBR = A.EXT_COURSE_NBR
                           AND F.SRC_SYS_ID = A.SRC_SYS_ID
                           and F.PERSON_SID <> 2147483646)
                WHEN F.TRNSFR_SRC_TYPE = 'I'
                THEN
                   (SELECT C.CATALOG_NBR
                      FROM CSMRT_OWNER.UM_D_CLASS C
                     WHERE F.CLASS_SID = C.CLASS_SID)
                ELSE
                   F.SCHOOL_CRSE_NBR
             END,
             '-')
             SCHOOL_CRSE_NBR,
          NVL (
             CASE
                WHEN F.TRNSFR_SRC_TYPE = 'E'
                THEN
                   (SELECT A.SCHOOL_SUBJECT
                      FROM CSMRT_OWNER.UM_F_EXT_ACAD_CRSE A
                     WHERE     F.PERSON_SID = A.PERSON_SID
                           AND F.EXT_ORG_SID = A.EXT_ORG_SID
                           AND F.EXT_COURSE_NBR = A.EXT_COURSE_NBR
                           AND F.SRC_SYS_ID = A.SRC_SYS_ID
                           and F.PERSON_SID <> 2147483646)
                WHEN F.TRNSFR_SRC_TYPE = 'I'
                THEN
                   (SELECT C.SBJCT_CD
                      FROM CSMRT_OWNER.UM_D_CLASS C
                     WHERE F.CLASS_SID = C.CLASS_SID)
                ELSE
                   F.SCHOOL_SUBJECT
             END,
             '-')
             SCHOOL_SUBJECT,
          SRC_CLASS_NBR,
          NVL (
             CASE
                WHEN F.TRNSFR_SRC_TYPE = 'I'
                THEN
                   DECODE (F.INSTITUTION_CD,
                           'UMBOS', 'UMass Boston',
                           'UMDAR', 'UMass Dartmouth',
                           'UMLOW', 'UMass Lowell',
                           'UMass Other')
                WHEN F.EXT_ORG_SID = 2147483646
                THEN
                   F.SRC_ORG_NAME
                ELSE
                   (SELECT O.EXT_ORG_LD
                      FROM CSMRT_OWNER.PS_D_EXT_ORG O
                     WHERE F.EXT_ORG_SID = O.EXT_ORG_SID)
             END,
             '-')
             SRC_ORG_NAME,
          NVL (SRC_TERM, '-') SRC_TERM,
          NVL (TRNSFR_SRC_TYPE, '-') TRNSFR_SRC_TYPE,
       nvl(X5.XLATSHORTNAME,'-') TRNSFR_SRC_TYPE_SD,
       nvl(X5.XLATLONGNAME,'-') TRNSFR_SRC_TYPE_LD,
          NVL (TRNSFR_EQVLNCY, '-') TRNSFR_EQVLNCY,
          NVL (TRNSFR_EQVLNCY_CMP, '-') TRNSFR_EQVLNCY_CMP,
          NVL (TRNSFR_STAT, '-') TRNSFR_STAT,
       nvl(X6.XLATSHORTNAME,'-') TRNSFR_STAT_SD,
       nvl(X6.XLATLONGNAME,'-') TRNSFR_STAT_LD,
          NVL (UNITS_ATTEMPTED_FLG, '-') UNITS_ATTEMPTED_FLG,
          UNT_TAKEN,
          UNT_TRNSFR,
          NVL (VALID_ATTEMPT_FLG, '-') VALID_ATTEMPT_FLG,
          F.TRNSFR_GRADE_POINTS TRNSFR_GRADE_POINTS
     FROM A
          LEFT OUTER JOIN CSMRT_OWNER.UM_F_TRANSFER F
             ON A.APPLCNT_SID = F.PERSON_SID
            AND A.ADMIT_TERM_SID = F.ARTICULATION_TERM_SID
            AND A.SRC_SYS_ID = F.SRC_SYS_ID
  left outer join X X1
    on F.GRADING_BASIS = X1.FIELDVALUE
   and F.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'GRADING_BASIS'
   and X1.X_ORDER = 1
  left outer join X X2
    on F.MODEL_STATUS_SCH = X2.FIELDVALUE
   and F.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'MODEL_STATUS'
   and X2.X_ORDER = 1
  left outer join X X3
    on F.MODEL_STATUS_TERM = X3.FIELDVALUE
   and F.SRC_SYS_ID = X3.SRC_SYS_ID
   and X3.FIELDNAME = 'MODEL_STATUS'
   and X3.X_ORDER = 1
  left outer join X X4
    on F.REJECT_REASON = X4.FIELDVALUE
   and F.SRC_SYS_ID = X4.SRC_SYS_ID
   and X4.FIELDNAME = 'REJECT_REASON'
   and X4.X_ORDER = 1
  left outer join X X5
    on F.TRNSFR_SRC_TYPE = X5.FIELDVALUE
   and F.SRC_SYS_ID = X5.SRC_SYS_ID
   and X5.FIELDNAME = 'TRNSFR_SRC_TYPE'
   and X5.X_ORDER = 1
  left outer join X X6
    on F.TRNSFR_STAT = X6.FIELDVALUE
   and F.SRC_SYS_ID = X6.SRC_SYS_ID
   and X6.FIELDNAME = 'TRNSFR_STAT'
   and X6.X_ORDER = 1
 where A.ADMIT_TERM_SID <> 2147483646	-- Mar 2019
   and A.APPLCNT_SID <> 2147483646
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_TRANSFER rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_TRANSFER',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER enable constraint PK_UM_F_ADM_APPL_TRANSFER';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_TRANSFER');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_F_ADM_APPL_TRANSFER_P;
/
