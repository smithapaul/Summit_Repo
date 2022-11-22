DROP PROCEDURE CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY_P
/

--
-- UM_R_FA_PERSON_RSDNCY_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_R_FA_PERSON_RSDNCY_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_R_FA_PERSON_RSDNCY
--V01 12/12/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_FA_PERSON_RSDNCY';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_R_FA_PERSON_RSDNCY');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY disable constraint PK_UM_R_FA_PERSON_RSDNCY';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY
WITH
AGG_TERM AS (
-- Use MIN term for institution
SELECT /*+ INLINE PARALLEL(8) */
  INSTITUTION_CD, '-' AID_YEAR,
  MIN(TERM_CD) AGG_TERM
FROM
  CSMRT_OWNER.UM_D_TERM_VW
WHERE CURRENT_TERM_FLG = 'Y'
GROUP BY INSTITUTION_CD, '-'
UNION
-- Use MAX term for institution, aid year
SELECT /*+ INLINE PARALLEL(8) */
   INSTITUTION_CD, AID_YEAR,
   MAX(TERM_CD) AGG_TERM
   FROM
   CSMRT_OWNER.UM_D_TERM_VW
WHERE TRIM(AID_YEAR) IS NOT NULL
GROUP BY INSTITUTION_CD, AID_YEAR
),
RES_TAB AS
(
-- Get rsdncy term code by joining to term dimension
SELECT /*+ INLINE PARALLEL(8) */
  R.SRC_SYS_ID,
  R.INSTITUTION_CD,
  R.ACAD_CAR_CD,
  EFF_TERM_CD,
  PERSON_ID,
  RSDNCY_SID,
  ADM_RSDNCY_SID,
  FA_FED_RSDNCY_SID,
  FA_ST_RSDNCY_SID,
  TUITION_RSDNCY_SID,
  RSDNCY_TERM_SID,
  D.TERM_CD RSDNCY_TERM_CD,
  ADM_EXCPT_SID,
  FA_FED_EXCPT_SID,
  FA_ST_EXCPT_SID,
  TUITION_EXCPT_SID,
  RSDNCY_DT,
  APPEAL_EFFDT,
  APPEAL_STATUS,
  APPEAL_STATUS_SD,
  APPEAL_STATUS_LD,
  APPEAL_COMMENTS
FROM
  CSMRT_OWNER.UM_R_PERSON_RSDNCY R,
  CSMRT_OWNER.UM_D_TERM_VW D
WHERE R.RSDNCY_TERM_SID = D.TERM_SID
),
TERM_DASH AS
(
-- Derive residency for rows on fa term that have term = -
SELECT /*+ INLINE PARALLEL(8) */
   F.INSTITUTION_CD,
   F.ACAD_CAR_CD,
   F.AID_YEAR,
   F.TERM_CD,
   F.PERSON_ID,
   F.SRC_SYS_ID,
   NVL(R.RSDNCY_TERM_CD, '-') RSDNCY_TERM_CD,
   F.INSTITUTION_SID,
   F.ACAD_CAR_SID,
   F.TERM_SID,
   F.PERSON_SID,
   NVL(R.RSDNCY_SID, 2147483646) RSDNCY_SID,
   NVL(R.ADM_RSDNCY_SID, 2147483646) ADM_RSDNCY_SID,
   NVL(R.FA_FED_RSDNCY_SID, 2147483646) FA_FED_RSDNCY_SID,
   NVL(R.FA_ST_RSDNCY_SID, 2147483646) FA_ST_RSDNCY_SID,
   NVL(R.TUITION_RSDNCY_SID, 2147483646) TUITION_RSDNCY_SID,
   NVL(R.RSDNCY_TERM_SID, 2147483646) RSDNCY_TERM_SID,
   NVL(R.ADM_EXCPT_SID, 2147483646) ADM_EXCPT_SID,
   NVL(R.FA_FED_EXCPT_SID, 2147483646) FA_FED_EXCPT_SID,
   NVL(R.FA_ST_EXCPT_SID, 2147483646) FA_ST_EXCPT_SID,
   NVL(R.TUITION_EXCPT_SID, 2147483646) TUITION_EXCPT_SID,
   R.RSDNCY_DT,
   R.APPEAL_EFFDT,
   NVL(R.APPEAL_STATUS, '-') APPEAL_STATUS,
   NVL(R.APPEAL_STATUS_SD, '-') APPEAL_STATUS_SD,
   NVL(R.APPEAL_STATUS_LD, '-') APPEAL_STATUS_LD,
   NVL(R.APPEAL_COMMENTS, '-') APPEAL_COMMENTS,
   ROW_NUMBER() OVER (PARTITION BY F.PERSON_ID, F.INSTITUTION_CD, F.AID_YEAR ORDER BY CASE WHEN NVL(R.RSDNCY_TERM_CD, '-') > T.AGG_TERM THEN
'0001' ELSE
                    NVL(R.RSDNCY_TERM_CD, '-') END DESC)  RSDNCY_TERM_ORDER
FROM
  CSMRT_OWNER.UM_F_FA_TERM F LEFT OUTER JOIN
    RES_TAB R
      ON F.INSTITUTION_CD = R.INSTITUTION_CD
     AND F.PERSON_ID = R.PERSON_ID
     AND F.SRC_SYS_ID = R.SRC_SYS_ID,
  AGG_TERM T
WHERE F.AID_YEAR = T.AID_YEAR
  AND F.INSTITUTION_CD = T.INSTITUTION_CD
  AND F.TERM_CD = '-'
)
SELECT /*+ INLINE PARALLEL(8) */
   INSTITUTION_CD,
   ACAD_CAR_CD,
   AID_YEAR,
   TERM_CD,
   PERSON_ID,
   SRC_SYS_ID,
   RSDNCY_TERM_CD,
   INSTITUTION_SID,
   ACAD_CAR_SID,
   TERM_SID,
   PERSON_SID,
   RSDNCY_SID,
   ADM_RSDNCY_SID,
   FA_FED_RSDNCY_SID,
   FA_ST_RSDNCY_SID,
   TUITION_RSDNCY_SID,
   RSDNCY_TERM_SID,
   ADM_EXCPT_SID,
   FA_FED_EXCPT_SID,
   FA_ST_EXCPT_SID,
   TUITION_EXCPT_SID,
   RSDNCY_DT,
   APPEAL_EFFDT,
   APPEAL_STATUS,
   APPEAL_STATUS_SD,
   APPEAL_STATUS_LD,
   APPEAL_COMMENTS,
   'N' LOAD_ERROR,
   'S' DATA_ORIGIN,
   SYSDATE CREATED_EW_DTTM,
   SYSDATE LASTUPD_EW_DTTM,
   123 BATCH_SID
FROM TERM_DASH
WHERE RSDNCY_TERM_ORDER = 1
UNION
SELECT /*+ INLINE PARALLEL(8) */
   F.INSTITUTION_CD,
   F.ACAD_CAR_CD,
   F.AID_YEAR,
   F.TERM_CD,
   F.PERSON_ID,
   F.SRC_SYS_ID,
   NVL(R.RSDNCY_TERM_CD, '-') RSDNCY_TERM_CD,
   F.INSTITUTION_SID,
   F.ACAD_CAR_SID,
   F.TERM_SID,
   F.PERSON_SID,
   NVL(R.RSDNCY_SID, 2147483646) RSDNCY_SID,
   NVL(R.ADM_RSDNCY_SID, 2147483646) ADM_RSDNCY_SID,
   NVL(R.FA_FED_RSDNCY_SID, 2147483646) FA_FED_RSDNCY_SID,
   NVL(R.FA_ST_RSDNCY_SID, 2147483646) FA_ST_RSDNCY_SID,
   NVL(R.TUITION_RSDNCY_SID, 2147483646) TUITION_RSDNCY_SID,
   NVL(R.RSDNCY_TERM_SID, 2147483646) RSDNCY_TERM_SID,
   NVL(R.ADM_EXCPT_SID, 2147483646) ADM_EXCPT_SID,
   NVL(R.FA_FED_EXCPT_SID, 2147483646) FA_FED_EXCPT_SID,
   NVL(R.FA_ST_EXCPT_SID, 2147483646) FA_ST_EXCPT_SID,
   NVL(R.TUITION_EXCPT_SID, 2147483646) TUITION_EXCPT_SID,
   R.RSDNCY_DT,
   R.APPEAL_EFFDT,
   NVL(R.APPEAL_STATUS, '-') APPEAL_STATUS,
   NVL(R.APPEAL_STATUS_SD, '-') APPEAL_STATUS_SD,
   NVL(R.APPEAL_STATUS_LD, '-') APPEAL_STATUS_LD,
   NVL(R.APPEAL_COMMENTS, '-') APPEAL_COMMENTS,
   'N' LOAD_ERROR,
   'S' DATA_ORIGIN,
   SYSDATE CREATED_EW_DTTM,
   SYSDATE LASTUPD_EW_DTTM,
   123 BATCH_SID
FROM
   CSMRT_OWNER.UM_F_FA_TERM F
    LEFT OUTER JOIN RES_TAB R
      ON F.PERSON_ID = R.PERSON_ID
     AND F.INSTITUTION_CD =  R.INSTITUTION_CD
     AND F.TERM_CD =  R.EFF_TERM_CD
     AND F.ACAD_CAR_CD = R.ACAD_CAR_CD
     AND F.SRC_SYS_ID =  R.SRC_SYS_ID
 WHERE F.TERM_CD <> '-';
strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_FA_PERSON_RSDNCY rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_FA_PERSON_RSDNCY',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_FA_PERSON_RSDNCY enable constraint PK_UM_R_FA_PERSON_RSDNCY';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_R_FA_PERSON_RSDNCY');

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

END UM_R_FA_PERSON_RSDNCY_P;
/
