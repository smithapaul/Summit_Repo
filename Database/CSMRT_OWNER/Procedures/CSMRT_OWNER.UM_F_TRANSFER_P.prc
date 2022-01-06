CREATE OR REPLACE PROCEDURE             "UM_F_TRANSFER_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_TRANSFER.
--
 --V01  SMT-xxxx 07/17/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_TRANSFER';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_TRANSFER';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_TRANSFER');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_TRANSFER disable constraint PK_UM_F_TRANSFER';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_TRANSFER';
insert /*+ append parallel(8) enable_parallel_dml */ into UM_F_TRANSFER
  WITH DTL AS
(
SELECT /*+ PARALLEL(8) INLINE */
        S.INSTITUTION,
        S.ACAD_CAREER,
        S.EMPLID,
        S.MODEL_NBR,
        T.ARTICULATION_TERM,
        D1.TRNSFR_EQVLNCY_GRP,
        D1.TRNSFR_EQVLNCY_SEQ,
        D1.SRC_SYS_ID,
        S.ACAD_PLAN,
        S.ACAD_PROG,
        S.APPLY_AGREEMENT_FL APPLY_AGREEMENT_FLG,   -- Feb 2020
        S.EXT_ORG_ID,
        S.INCLUDE_IN_GPA INCLUDE_IN_GPA_FLG_SCH,
        S.LS_SCHOOL_TYPE,
        S.LS_DATA_SOURCE,
        S.MODEL_STATUS MODEL_STATUS_SCH,
        S.SRC_CAREER,
        S.SRC_INSTITUTION,
        S.SRC_ORG_NAME,
        S.TRANSCRIPT_LEVEL,
        S.TRNSFR_SRC_TYPE,
        S.TRF_TAKEN_GPA TRF_TAKEN_GPA_SCH,
        S.TRF_TAKEN_NOGPA TRF_TAKEN_NOGPA_SCH,
        S.TRF_PASSED_GPA TRF_PASSED_GPA_SCH,
        S.TRF_PASSED_NOGPA TRF_PASSED_NOGPA_SCH,
        S.TRF_GRADE_POINTS TRF_GRADE_POINTS_SCH,
        S.TRF_GPA TRF_GPA_SCH,
        T.MODEL_STATUS MODEL_STATUS_TERM,
        T.OPRID,
--        TO_CHAR(T.POST_DT, 'YYYY-MM-DD HH24:MI:SS')  POST_DT,
        T.POST_DT  POST_DT,
        T.TRF_TAKEN_GPA TRF_TAKEN_GPA_TERM,
        T.TRF_TAKEN_NOGPA TRF_TAKEN_NOGPA,
        T.TRF_PASSED_GPA TRF_PASSED_GPA_TERM,
        T.TRF_PASSED_NOGPA TRF_PASSED_NOGPA_TERM,
        T.TRF_GRADE_POINTS TRF_GRADE_POINTS_TERM,
        T.TRF_GPA TRF_GPA_TERM,
        T.SSR_FAWI_TKN,
        T.SSR_FAWI_TKN_GPA,
        T.SSR_FAWI_TKN_NOGPA,
        T.SSR_FAWI_PSD,
        T.SSR_FAWI_PSD_GPA,
        T.SSR_FAWI_PSD_NOGPA,
        T.SSR_FAWI_GRADE_PTS,
        T.SSR_FAWI_GPA,
        D1.COURSE_LEVEL,
        DECODE(D1.CRSE_GRADE_INPUT,'-',NVL(D2.CRSE_GRADE_INPUT,'-'),D1.CRSE_GRADE_INPUT) CRSE_GRADE_INPUT,
        D1.CRSE_GRADE_OFF,
        DECODE(D1.CRSE_ID,'-',NVL(D2.CRSE_ID,'-'),D1.CRSE_ID) CRSE_ID,
        DECODE(D1.CRSE_OFFER_NBR,0,NVL(D2.CRSE_OFFER_NBR,0),D1.CRSE_OFFER_NBR) CRSE_OFFER_NBR,
        D1.DESCR,
        DECODE(D1.EARN_CREDIT,'-',NVL(D2.EARN_CREDIT,'-'),D1.EARN_CREDIT) EARN_CREDIT_FLG,
        DECODE(D1.EXT_COURSE_NBR,0,NVL(D2.EXT_COURSE_NBR,0),D1.EXT_COURSE_NBR) EXT_COURSE_NBR,
        D1.EXT_TERM,
        D1.GRADE_CATEGORY,
        DECODE(D1.GRADING_BASIS,'-',NVL(D2.GRADING_BASIS,'-'),D1.GRADING_BASIS) GRADING_BASIS,
        DECODE(D1.GRADING_SCHEME,'-',NVL(D2.GRADING_SCHEME,'-'),D1.GRADING_SCHEME) GRADING_SCHEME,
        D1.GRD_PTS_PER_UNIT,
        D1.REPEAT_CODE,
        D1.REJECT_REASON,
        D1.SCHOOL_CRSE_NBR,
        D1.SCHOOL_SUBJECT,
        DECODE(D1.SRC_CLASS_NBR,0,NVL(D2.SRC_CLASS_NBR,0),D1.SRC_CLASS_NBR) SRC_CLASS_NBR,
        DECODE(D1.SRC_TERM,'-',NVL(D2.SRC_TERM,'-'),D1.SRC_TERM) SRC_TERM,
        D1.TERM_YEAR,
        D1.TRNSFR_EQVLNCY,
        D1.TRNSFR_EQVLNCY_CMP,
        DECODE(D1.TRNSFR_STAT,'-',NVL(D2.TRNSFR_STAT,'-'),D1.TRNSFR_STAT) TRNSFR_STAT,
        D1.UNITS_ATTEMPTED UNITS_ATTEMPTED_FLG,
        D1.UNT_TAKEN,
        D1.UNT_TRNSFR,
        D1.VALID_ATTEMPT VALID_ATTEMPT_FLG
   FROM CSSTG_OWNER.PS_TRNS_CRSE_SCH S
   JOIN CSSTG_OWNER.PS_TRNS_CRSE_TERM T
     ON S.EMPLID = T.EMPLID
    AND S.ACAD_CAREER = T.ACAD_CAREER
    AND S.INSTITUTION = T.INSTITUTION
    AND S.MODEL_NBR = T.MODEL_NBR
    AND S.SRC_SYS_ID = T.SRC_SYS_ID
    AND S.DATA_ORIGIN <> 'D'
    AND T.DATA_ORIGIN <> 'D'
    AND S.MODEL_STATUS IN ('P','C')
    AND T.MODEL_STATUS IN ('P','C')
   JOIN CSSTG_OWNER.PS_TRNS_CRSE_DTL D1
     ON T.EMPLID = D1.EMPLID
    AND T.ACAD_CAREER = D1.ACAD_CAREER
    AND T.INSTITUTION = D1.INSTITUTION
    AND T.MODEL_NBR = D1.MODEL_NBR
    AND T.ARTICULATION_TERM = D1.ARTICULATION_TERM
    AND T.SRC_SYS_ID = D1.SRC_SYS_ID
    AND D1.DATA_ORIGIN <> 'D'
   LEFT OUTER JOIN CSSTG_OWNER.PS_TRNS_CRSE_DTL D2
     ON D1.EMPLID = D2.EMPLID
    AND D1.ACAD_CAREER = D2.ACAD_CAREER
    AND D1.INSTITUTION = D2.INSTITUTION
    AND D1.MODEL_NBR = D2.MODEL_NBR
    AND D1.ARTICULATION_TERM = D2.ARTICULATION_TERM
    AND D1.TRNSFR_EQVLNCY_GRP = D2.TRNSFR_EQVLNCY_GRP
    AND D2.TRNSFR_EQVLNCY_SEQ = 1
    AND D1.SRC_SYS_ID = D2.SRC_SYS_ID
    AND D2.DATA_ORIGIN <> 'D'
),
GRD_TBL AS (
SELECT /*+ PARALLEL(8) INLINE */
       SETID, GRADING_SCHEME, GRADING_BASIS, CRSE_GRADE_INPUT, SRC_SYS_ID,
       GRADE_POINTS,
       ROW_NUMBER() OVER (PARTITION BY SETID, GRADING_SCHEME, GRADING_BASIS, CRSE_GRADE_INPUT
                              ORDER BY EFFDT DESC) AS GRD_ORDER
  FROM CSSTG_OWNER.PS_GRADE_TBL
 WHERE DATA_ORIGIN <> 'D'
 ),
CLASS as (
SELECT /*+ inline parallel(8) */
       INSTITUTION_CD, TERM_CD, CLASS_NUM, SRC_SYS_ID, CLASS_SID,
       row_number() over (partition by INSTITUTION_CD, TERM_CD, CLASS_NUM, SRC_SYS_ID
                              order by SESSION_CD, CLASS_SECTION_CD) CLASS_ORDER
  FROM UM_D_CLASS L
 WHERE DATA_ORIGIN <> 'D'
),
SCH as (
SELECT SETID, GRADING_SCHEME, SRC_SYS_ID, DESCR, DESCRSHORT,
       row_number() over (partition by SETID, GRADING_SCHEME, SRC_SYS_ID order by SETID, GRADING_SCHEME, SRC_SYS_ID, EFFDT desc) SCH_ORDER
  FROM CSSTG_OWNER.PS_GRADESCHEME_TBL
where DATA_ORIGIN <> 'D'
)
SELECT /*+ PARALLEL(8) */
       DTL.INSTITUTION INSTITUTION_CD,
       DTL.ACAD_CAREER ACAD_CAR_CD,
       DTL.EMPLID PERSON_ID,
       DTL.MODEL_NBR,
       DTL.ARTICULATION_TERM,
       DTL.TRNSFR_EQVLNCY_GRP,
       DTL.TRNSFR_EQVLNCY_SEQ,
       DTL.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(T.TERM_SID,2147483646) ARTICULATION_TERM_SID,
       nvl(G.ACAD_PROG_SID,2147483646) ACAD_PROG_TARGET_SID,       -- Feb 2020
       nvl(PL.ACAD_PLAN_SID,2147483646) ACAD_PLAN_TARGET_SID,      -- Feb 2020
       nvl(L.CLASS_SID,2147483646) CLASS_SID,
       nvl(R.CRSE_SID,2147483646) CRSE_SID,
       NVL (X.EXT_CRSE_SID, 2147483646) EXT_CRSE_SID,
       NVL (ORG.EXT_ORG_SID, 2147483646) EXT_ORG_SID,
       NVL (XT.EXT_TERM_SID, 2147483646) EXT_TERM_SID,
       NVL (DTL.TERM_YEAR, 0) EXT_TERM_YEAR_SID,
       DTL.APPLY_AGREEMENT_FLG,             -- Feb 2020
       DTL.COURSE_LEVEL,
       DTL.CRSE_GRADE_INPUT,
       DTL.CRSE_GRADE_OFF,
       DTL.DESCR,
       DTL.EARN_CREDIT_FLG,
       DTL.EXT_COURSE_NBR,
       DTL.GRADE_CATEGORY,
       DTL.GRADING_SCHEME,
	   nvl(trim(SCH.DESCRSHORT), '-')  GRADING_SCHEME_SD,
       nvl(trim(SCH.DESCR), '-')  GRADING_SCHEME_LD,
       DTL.GRADING_BASIS,
       DTL.GRD_PTS_PER_UNIT,
       DTL.MODEL_STATUS_SCH,
       DTL.MODEL_STATUS_TERM,
       DTL.POST_DT,
       DTL.REJECT_REASON,
       DTL.REPEAT_CODE,
       DTL.SCHOOL_CRSE_NBR,
       DTL.SCHOOL_SUBJECT,
       SRC_CLASS_NBR,
       SRC_ORG_NAME,
       SRC_TERM,
       CASE WHEN(DTL.REPEAT_CODE IN ('DLTN', 'EAMN', 'ERPT', 'EXCA','EXCF','EXCL', 'ILGL', 'FXNR','VIOL') OR DTL.CRSE_GRADE_INPUT = 'T')
            THEN(0)
            ELSE(COALESCE(GRD_TBL.GRADE_POINTS, 0))
        END TRNSFR_GRADE_POINTS,
       DTL.TRNSFR_SRC_TYPE,
       DTL.TRNSFR_EQVLNCY,
       DTL.TRNSFR_EQVLNCY_CMP,
       DTL.TRNSFR_STAT,
       DTL.UNITS_ATTEMPTED_FLG,
       DTL.UNT_TAKEN,
       DTL.UNT_TRNSFR,
       DTL.VALID_ATTEMPT_FLG,
        'N' LOAD_ERROR,
        'S' DATA_ORIGIN,
        SYSDATE CREATED_EW_DTTM,
        SYSDATE LASTUPD_EW_DTTM,
        1234 BATCH_SID
  FROM DTL
  LEFT OUTER JOIN GRD_TBL
    ON DTL.INSTITUTION = GRD_TBL.SETID
   AND DTL.GRADING_SCHEME = GRD_TBL.GRADING_SCHEME
   AND DTL.GRADING_BASIS = GRD_TBL.GRADING_BASIS
   AND DTL.CRSE_GRADE_INPUT = GRD_TBL.CRSE_GRADE_INPUT
   AND GRD_TBL.GRD_ORDER = 1
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on DTL.INSTITUTION = I.INSTITUTION_CD
   and DTL.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on DTL.INSTITUTION = C.INSTITUTION_CD
   and DTL.ACAD_CAREER = C.ACAD_CAR_CD
   and DTL.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_ACAD_PROG G
    on DTL.INSTITUTION = G.INSTITUTION_CD
   and DTL.ACAD_PROG = G.ACAD_PROG_CD
   and DTL.SRC_SYS_ID = G.SRC_SYS_ID
   and G.EFFDT_ORDER = 1
   and G.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_ACAD_PLAN PL
    on DTL.INSTITUTION = PL.INSTITUTION_CD
   and DTL.ACAD_PLAN = PL.ACAD_PLAN_CD
   and DTL.SRC_SYS_ID = PL.SRC_SYS_ID
   and PL.EFFDT_ORDER = 1
   and PL.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on DTL.EMPLID = P.PERSON_ID
   and DTL.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_TERM T
    on DTL.INSTITUTION = T.INSTITUTION_CD
   and DTL.ACAD_CAREER = T.ACAD_CAR_CD
   and DTL.ARTICULATION_TERM = T.TERM_CD
   and DTL.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join CLASS L
    on DTL.INSTITUTION = L.INSTITUTION_CD
   and DTL.SRC_TERM = L.TERM_CD
   and DTL.SRC_CLASS_NBR = L.CLASS_NUM
   and DTL.SRC_SYS_ID = L.SRC_SYS_ID
   and L.CLASS_ORDER = 1
  left outer join CSMRT_OWNER.UM_D_CRSE R
    on DTL.CRSE_ID = R.CRSE_CD
   and DTL.CRSE_OFFER_NBR = R.CRSE_OFFER_NUM
   and DTL.SRC_SYS_ID = R.SRC_SYS_ID
   and R.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_EXT_CRSE X
    on DTL.EXT_ORG_ID = X.EXT_ORG_ID
   and DTL.SCHOOL_SUBJECT = X.SCHOOL_SUBJECT
   and DTL.SCHOOL_CRSE_NBR = X.SCHOOL_CRSE_NBR
   and DTL.SRC_SYS_ID = X.SRC_SYS_ID
   and X.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_EXT_ORG ORG
    on ORG.EXT_ORG_ID =
       CASE
            WHEN DTL.TRNSFR_SRC_TYPE = 'I' and DTL.INSTITUTION = 'UMBOS' then '00004236'
			WHEN DTL.TRNSFR_SRC_TYPE = 'I' and DTL.INSTITUTION = 'UMDAR' then '00004233'
			WHEN DTL.TRNSFR_SRC_TYPE = 'I' and DTL.INSTITUTION = 'UMLOW' then '00004237'
			ELSE DTL.EXT_ORG_ID
		END
   and DTL.SRC_SYS_ID = ORG.SRC_SYS_ID
   and ORG.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_EXT_TERM XT
    on XT.EXT_TERM_TYPE_ID = 'QTR'
   and DTL.EXT_TERM = XT.EXT_TERM_ID
   and DTL.SRC_SYS_ID = XT.SRC_SYS_ID
   and XT.DATA_ORIGIN <> 'D'
  left outer join SCH
    on DTL.INSTITUTION = SCH.SETID
   and DTL.GRADING_SCHEME = SCH.GRADING_SCHEME
   and DTL.SRC_SYS_ID = SCH.SRC_SYS_ID
   and SCH.SCH_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_TRANSFER rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_TRANSFER',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_TRANSFER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_TRANSFER enable constraint PK_UM_F_TRANSFER';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_TRANSFER');

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

END UM_F_TRANSFER_P;
/
