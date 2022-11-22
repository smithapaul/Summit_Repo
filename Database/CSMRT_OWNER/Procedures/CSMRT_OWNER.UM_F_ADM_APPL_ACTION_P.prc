DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_ACTION_P
/

--
-- UM_F_ADM_APPL_ACTION_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ADM_APPL_ACTION_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_ACTION
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_ACTION';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_ACTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_ACTION';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_ACTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_ACTION');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_ACTION disable constraint PK_UM_F_ADM_APPL_ACTION';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_ACTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_ACTION';
INSERT /*+ append enable_parallel_dml parallel(8) */ INTO UM_F_ADM_APPL_ACTION
WITH ACN_LIST AS (
    SELECT /*+ INLINE PARALLEL(8) */ DISTINCT
      APPLCNT_SID,
      ADM_APPL_NBR,
      D.PROG_ACN_CD
    FROM UM_F_ADM_APPL_STAT F,
      PS_D_PROG_ACN D
    WHERE F.PROG_ACN_SID = D.PROG_ACN_SID
  	  and F.INSTITUTION_CD = D.SETID               -- SETID added to key for PS_D_PROG_ACN_NEW!!!!!!!!!!!!!!
),
RSN_LIST AS (
    SELECT /*+ INLINE PARALLEL(8) */ DISTINCT
      APPLCNT_SID,
      ADM_APPL_NBR,
      PROG_ACN_RSN_CD
    FROM
      UM_F_ADM_APPL_STAT F,
      PS_D_PROG_ACN_RSN D
    WHERE F.PROG_ACN_RSN_SID = d.PROG_ACN_RSN_SID
),
ACN_LIST1 AS (
    SELECT /*+ INLINE PARALLEL(8) */ DISTINCT
       F.APPLCNT_SID,
       F.ADM_APPL_NBR,
       ACN.PROG_ACN_CD,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT DESC) MAX_DT,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT) MIN_DT,
       FIRST_VALUE(RPAD(RSN.PROG_ACN_RSN_CD, 4)||RSN.PROG_ACN_RSN_LD) OVER
            (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT DESC, F.APPL_COUNT_ORDER,  DECODE(RSN.PROG_ACN_RSN_CD, '-', NULL, RSN.PROG_ACN_RSN_CD) ASC NULLS LAST ) MAX_RSN_CD,
       FIRST_VALUE(RPAD(RSN.PROG_ACN_RSN_CD, 4)||RSN.PROG_ACN_RSN_LD) OVER
            (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT, F.APPL_COUNT_ORDER DESC,  DECODE(RSN.PROG_ACN_RSN_CD, '-', NULL, RSN.PROG_ACN_RSN_CD) ASC NULLS LAST  ) MIN_RSN_CD
    FROM
      UM_F_ADM_APPL_STAT F,
      ACN_LIST L,
      PS_D_PROG_ACN ACN,
      PS_D_PROG_ACN_RSN RSN
    WHERE F.APPLCNT_SID = L.APPLCNT_SID
    AND F.ADM_APPL_NBR = L.ADM_APPL_NBR
    AND F.PROG_ACN_SID = ACN.PROG_ACN_SID
    and F.INSTITUTION_CD = ACN.SETID               -- SETID added to key for PS_D_PROG_ACN_NEW!!!!!!!!!!!!!!
    AND ACN.PROG_ACN_CD = L.PROG_ACN_CD
    AND F.PROG_ACN_RSN_SID = RSN.PROG_ACN_RSN_SID),
ACN_LIST2 AS (
    SELECT /*+ INLINE PARALLEL(8) */ DISTINCT
       F.APPLCNT_SID,
       F.ADM_APPL_NBR,
       ACN.PROG_ACN_CD,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT DESC) MAX_TERM_MAX_DT,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT) MAX_TERM_MIN_DT,
       FIRST_VALUE(RPAD(RSN.PROG_ACN_RSN_CD, 4)||RSN.PROG_ACN_RSN_LD) OVER
            (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT DESC, F.APPL_COUNT_ORDER,  DECODE(RSN.PROG_ACN_RSN_CD, '-', NULL, RSN.PROG_ACN_RSN_CD) ASC NULLS LAST ) MAX_TERM_MAX_RSN_CD,
       FIRST_VALUE(RPAD(RSN.PROG_ACN_RSN_CD, 4)||RSN.PROG_ACN_RSN_LD) OVER
            (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, ACN.PROG_ACN_CD ORDER BY F.ACTION_DT, F.APPL_COUNT_ORDER DESC,  DECODE(RSN.PROG_ACN_RSN_CD, '-', NULL, RSN.PROG_ACN_RSN_CD) ASC NULLS LAST  ) MAX_TERM_MIN_RSN_CD
    FROM
      UM_F_ADM_APPL_STAT F,
      ACN_LIST L,
      PS_D_PROG_ACN ACN,
      PS_D_PROG_ACN_RSN RSN
    WHERE F.APPLCNT_SID = L.APPLCNT_SID
    AND F.ADM_APPL_NBR = L.ADM_APPL_NBR
    AND F.PROG_ACN_SID = ACN.PROG_ACN_SID
    and F.INSTITUTION_CD = ACN.SETID               -- SETID added to key for PS_D_PROG_ACN_NEW!!!!!!!!!!!!!!
    AND ACN.PROG_ACN_CD = L.PROG_ACN_CD
    AND F.PROG_ACN_RSN_SID = RSN.PROG_ACN_RSN_SID
    AND F.MAX_TERM_FLG = 'Y' ),
RSN_LIST1 AS (
    SELECT /*+ INLINE PARALLEL(8) */ DISTINCT
       F.APPLCNT_SID,
       F.ADM_APPL_NBR,
       RSN.PROG_ACN_RSN_CD,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, RSN.PROG_ACN_RSN_CD ORDER BY F.ACTION_DT DESC) MAX_DT,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, RSN.PROG_ACN_RSN_CD ORDER BY F.ACTION_DT) MIN_DT,
       NULL MAX_RSN_CD,
       NULL MIN_RSN_CD
    FROM
      UM_F_ADM_APPL_STAT F,
      PS_D_PROG_ACN_RSN RSN,
      RSN_LIST L
    WHERE F.APPLCNT_SID = L.APPLCNT_SID
    AND F.ADM_APPL_NBR = L.ADM_APPL_NBR
    AND F.PROG_ACN_RSN_SID = RSN.PROG_ACN_RSN_SID
    AND RSN.PROG_ACN_RSN_CD = L.PROG_ACN_RSN_CD),
RSN_LIST2 AS (
    SELECT /*+ INLINE PARALLEL(8) */ DISTINCT
       F.APPLCNT_SID,
       F.ADM_APPL_NBR,
       RSN.PROG_ACN_RSN_CD,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, RSN.PROG_ACN_RSN_CD ORDER BY F.ACTION_DT DESC) MAX_TERM_MAX_DT,
       FIRST_VALUE(F.ACTION_DT) OVER (PARTITION BY F.APPLCNT_SID, F.ADM_APPL_NBR, RSN.PROG_ACN_RSN_CD ORDER BY F.ACTION_DT) MAX_TERM_MIN_DT,
       NULL MAX_TERM_MAX_RSN_CD,
       NULL MAX_TERM_MIN_RSN_CD
    FROM
      UM_F_ADM_APPL_STAT F,
      PS_D_PROG_ACN_RSN RSN,
      RSN_LIST L
    WHERE F.APPLCNT_SID = L.APPLCNT_SID
    AND F.ADM_APPL_NBR = L.ADM_APPL_NBR
    AND F.PROG_ACN_RSN_SID = RSN.PROG_ACN_RSN_SID
    AND RSN.PROG_ACN_RSN_CD = L.PROG_ACN_RSN_CD
    AND F.MAX_TERM_FLG = 'Y' )
SELECT /*+ INLINE PARALLEL(8) */
  PER.PERSON_ID,
  L1.ADM_APPL_NBR,
  'PROG_ACN_CD' ACN_RSN_KEY,
  L1.PROG_ACN_CD ACN_RSN_VAL,
  MAX_TERM_MAX_DT,
  MAX_TERM_MIN_DT,
  MAX_DT,
  MIN_DT,
  MAX_TERM_MAX_RSN_CD,
  MAX_TERM_MIN_RSN_CD,
  MAX_RSN_CD,
  MIN_RSN_CD,
  'S' DATA_ORIGIN,
  SYSDATE CREATED_EW_DTTM,
  SYSDATE LASTUPD_EW_DTTM,
  99999 BATCH_SID
FROM
  ACN_LIST L1 LEFT OUTER JOIN  ACN_LIST1 L2
    ON L1.APPLCNT_SID = L2.APPLCNT_SID
    AND L1.ADM_APPL_NBR = L2.ADM_APPL_NBR
    AND L1.PROG_ACN_CD = L2.PROG_ACN_CD
  LEFT OUTER JOIN  ACN_LIST2 L3
    ON L1.APPLCNT_SID = L3.APPLCNT_SID
    AND L1.ADM_APPL_NBR = L3.ADM_APPL_NBR
    AND L1.PROG_ACN_CD = L3.PROG_ACN_CD,
  PS_D_PERSON PER
WHERE L1.APPLCNT_SID = PER.PERSON_SID
UNION
SELECT /*+ INLINE PARALLEL(8) */
  PER.PERSON_ID,
  L1.ADM_APPL_NBR,
  'PROG_ACN_RSN_CD' ACN_RSN_KEY,
  L1.PROG_ACN_RSN_CD ACN_RSN_VAL,
  MAX_TERM_MAX_DT,
  MAX_TERM_MIN_DT,
  MAX_DT,
  MIN_DT,
  MAX_TERM_MAX_RSN_CD,
  MAX_TERM_MIN_RSN_CD,
  MAX_RSN_CD,
  MIN_RSN_CD,
  'S' DATA_ORIGIN,
  SYSDATE CREATED_EW_DTTM,
  SYSDATE LASTUPD_EW_DTTM,
  99999 BATCH_SID
FROM
  RSN_LIST L1 LEFT OUTER JOIN  RSN_LIST1 L2
    ON L1.APPLCNT_SID = L2.APPLCNT_SID
    AND L1.ADM_APPL_NBR = L2.ADM_APPL_NBR
    AND L1.PROG_ACN_RSN_CD = L2.PROG_ACN_RSN_CD
  LEFT OUTER JOIN  RSN_LIST2 L3
    ON L1.APPLCNT_SID = L3.APPLCNT_SID
    AND L1.ADM_APPL_NBR = L3.ADM_APPL_NBR
    AND L1.PROG_ACN_RSN_CD = L3.PROG_ACN_RSN_CD,
  PS_D_PERSON PER
WHERE L1.APPLCNT_SID = PER.PERSON_SID;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_ACTION rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_ACTION',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_ACTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_ACTION enable constraint PK_UM_F_ADM_APPL_ACTION';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_ACTION');

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

END UM_F_ADM_APPL_ACTION_P;
/
