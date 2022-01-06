CREATE OR REPLACE PROCEDURE             UM_S_SR_DATA_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads table CSMRT_OWNER.UM_S_SR_DATA.
--
-- V01  SMT-xxxx 05/28/2019,    James Doucette
--                              Converted from script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_S_SR_DATA';
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

strMessage01    := 'Deleting from table CSMRT_OWNER.UM_S_SR_DATA where RUN_DT = '  || TO_CHAR(trunc(SYSDATE));
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'delete CSMRT_OWNER.UM_S_SR_DATA where RUN_DT = trunc(SYSDATE)';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_S_SR_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_S_SR_DATA';
insert /*+ append */ into CSMRT_OWNER.UM_S_SR_DATA
with TERM as (
--select distinct INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, SRC_SYS_ID
select distinct INSTITUTION_CD, TERM_CD, SRC_SYS_ID
  from PS_D_SESSION
 where trunc(SYSDATE) between REGISTR_START_DT and (ADD_DROP_END_DT+14)
)
SELECT  /* parallel(16) */
        trunc(SYSDATE) RUN_DT,
        T577578.INSTITUTION_CD,
        T577578.ACAD_CAR_CD,
        T577578.TERM_CD,
        T572353.PERSON_ID,
        T771456.STDNT_CAR_NUM,
        T955761.ACAD_PROG_CD,
        T955758.ACAD_PLAN_CD,
        T955760.ACAD_SPLAN_CD,
        T577578.SRC_SYS_ID,
        T771448.ONLINE_CREDITS AS ONLINE_CREDITS,
        T771448.CE_ONLINE_CREDITS AS CE_ONLINE_CREDITS,         -- Nov 2020
        T771448.TOT_CREDITS AS TOT_CREDITS,
        T771448.DAY_CREDITS AS DAY_CREDITS,
        T771448.CE_CREDITS AS CE_CREDITS,
        T771448.DAY_ONLY_FLG AS DAY_ONLY_FLG,
        T771448.ONLINE_ONLY_FLG AS ONLINE_ONLY_FLG,
        T771448.ENROLL_FLG AS ENROLL_FLG,
        T771448.CE_ONLY_FLG AS CE_ONLY_FLG,
        T771448.AUDIT_ONLY_FLG AS AUDIT_ONLY_FLG,
        T771456.UNDUP_STDNT_CNT AS UNDUP_STDNT_CNT,
        T771448.UGRD_SECOND_DEGR_FLG AS UGRD_SECOND_DEGR_FLG,
        T771456.STACK_READMIT_FLG AS STACK_READMIT_FLG,
        T771456.STACK_BEGIN_FLG AS STACK_BEGIN_FLG,
        T771456.DEGREE_SEEKING_FLG AS DEGREE_SEEKING_FLG,
        T771456.STACK_CONTINUE_FLG AS STACK_CONTINUE_FLG,
        T771456.CERTIFICATE_ONLY_FLG AS CERTIFICATE_ONLY_FLG,
--        CASE WHEN T1046997.STDNT_ATTR_VAL_LD IS NULL THEN '-'
--             ELSE T1046997.STDNT_ATTR_VAL_LD
--         END AS STDNT_ATTR_VAL_LD,
        nvl(trim(T1046997.STDNT_ATTR_VAL_LD),'-') STDNT_ATTR_VAL_LD,    -- June 2019
--        CASE WHEN T1046996.STDNT_ATTR_VALUE IS NULL THEN '-'
--             ELSE T1046996.STDNT_ATTR_VALUE
--         END AS STDNT_ATTR_VALUE,
        nvl(trim(T1046996.STDNT_ATTR_VALUE),'-') STDNT_ATTR_VALUE,      -- June 2019
        T991524.ACAD_GRP_CD AS ACAD_GRP_CD,
        T963945.EDU_LVL_CD AS EDU_LVL_CD,
        T955758.ACAD_PLAN_TYPE_CD AS ACAD_PLAN_TYPE_CD,
        T777414.PROG_STAT_CD AS PROG_STAT_CD,
        T718019.ETHNIC_GRP_CD AS ETHNIC_GRP_FED_CD,
        T572353.GENDER_CD AS GENDER_CD,
        T718025.ETHNIC_GRP_CD AS ETHNIC_GRP_ST_CD,
        T993278.RSDNCY_ID AS RSDNCY_ID,
        T575864.ACAD_LVL_CD AS ACAD_LVL_CD,
        T721572.COUNTRY,                        -- June 2019
        T721572.CITIZENSHIP_STATUS,             -- June 2019
        T777421.PROG_ACN_CD,                    -- June 2019
        T777430.PROG_ACN_RSN_CD                 -- June 2019
   FROM CSMRT_OWNER.UM_D_ACAD_LVL_VW T575864,
        CSMRT_OWNER.UM_D_RSDNCY_VW T993278,
        CSMRT_OWNER.UM_D_ACAD_PROG_VW T955761,
        CSMRT_OWNER.UM_D_ACAD_PLAN_VW T955758,
        CSMRT_OWNER.UM_D_ACAD_SPLAN_VW T955760,
        CSMRT_OWNER.UM_D_ACAD_GRP_VW T991524,
        CSMRT_OWNER.UM_D_PROG_STAT_VW T777414,
        CSMRT_OWNER.UM_D_DEG_VW T963945,
--        CSMRT_OWNER.UM_D_ETHNIC_GRP_ST_VW T718025,
--        CSMRT_OWNER.UM_D_ETHNIC_GRP_FED_VW T718019,
        CSMRT_OWNER.UM_D_ETHNIC_GRP_VW T718025,             -- Dec 2017
        CSMRT_OWNER.UM_D_ETHNIC_GRP_VW T718019,             -- Dec 2017
        CSMRT_OWNER.UM_D_PERSON_CS_CITIZEN_USA_VW T721572,  -- June 2019
        CSMRT_OWNER.UM_D_PERSON_CS_VW T572353,
        CSMRT_OWNER.UM_D_TERM_VW T577578,
        CSMRT_OWNER.UM_D_PROG_ACN_RSN_VW T777430,           -- June 2019
        CSMRT_OWNER.UM_D_PROG_ACN_VW T777421,               -- June 2018
        TERM,
        CSMRT_OWNER.UM_F_STDNT_TERM_VW T771448,
        CSMRT_OWNER.UM_R_PERSON_RSDNCY_VW T992910,
        (CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT_VW T771456
         LEFT OUTER JOIN CSMRT_OWNER.UM_D_STDNT_ATTR_VAL_VW T1046997
           ON T771456.STDNT_CAR_NUM = T1046997.STDNT_CAR_NUM
          AND T771456.SRC_SYS_ID = T1046997.SRC_SYS_ID
          AND T771456.ACAD_CAR_SID = T1046997.ACAD_CAR_SID
          AND T771456.PERSON_SID = T1046997.PERSON_SID
          AND T1046997.ATTR_ORDER = 1
          AND CAST (SUBSTR (T1046997.STDNT_ATTR, -2, 2) AS VARCHAR (2)) = 'AT')
        LEFT OUTER JOIN CSMRT_OWNER.UM_D_STDNT_ATTR_VAL_VW T1046996
          ON T771456.STDNT_CAR_NUM = T1046996.STDNT_CAR_NUM
         AND T771456.SRC_SYS_ID = T1046996.SRC_SYS_ID
         AND T771456.ACAD_CAR_SID = T1046996.ACAD_CAR_SID
         AND T771456.PERSON_SID = T1046996.PERSON_SID
         AND T1046996.ATTR_ORDER = 1
         AND CAST (SUBSTR (T1046996.STDNT_ATTR, -2, 2) AS VARCHAR (2)) = 'AD'
  WHERE (    T575864.ACAD_LVL_SID = T771448.STRT_ACAD_LVL_SID
--         AND T992910.RSDNCY_SID = T993278.RSDNCY_SID          -- June 2017
         AND T992910.TUITION_RSDNCY_SID = T993278.RSDNCY_SID    -- June 2017
         AND T771456.PROG_STAT_SID = T777414.PROG_STAT_SID
         AND T771456.INSTITUTION_CD = T777421.SETID
         AND T771456.PROG_ACN_SID = T777421.PROG_ACN_SID
         AND T771456.PROG_ACN_RSN_SID = T777430.PROG_ACN_RSN_SID
         AND T572353.ETHNIC_GRP_ST_SID = T718025.ETHNIC_GRP_SID
         AND T572353.ETHNIC_GRP_FED_SID = T718019.ETHNIC_GRP_SID
         AND T572353.PERSON_SID = T771448.PERSON_SID
         AND T572353.PERSON_SID = T721572.PERSON_SID
         AND T577578.TERM_SID = T771448.TERM_SID
--         AND T577578.TERM_CD_DESC = '2710 (2017 Fall)'
         and T577578.INSTITUTION_CD = TERM.INSTITUTION_CD
--         and T577578.ACAD_CAR_CD = TERM.ACAD_CAR_CD
         and T577578.TERM_CD = TERM.TERM_CD
         and T577578.SRC_SYS_ID = TERM.SRC_SYS_ID
         AND T771456.ACAD_PROG_SID = T955761.ACAD_PROG_SID
         AND NVL (T771456.TERM_END_DT, TRUNC (SYSDATE)) BETWEEN T955761.EFFDT_START
                                                            AND T955761.EFFDT_END)
         AND T771456.ACAD_PLAN_SID = T955758.ACAD_PLAN_SID
         AND T955758.EFFDT_END >= NVL (T771456.TERM_END_DT, TRUNC (SYSDATE))
         AND NVL (T771456.TERM_END_DT, TRUNC (SYSDATE)) >= T955758.EFFDT_START
         AND T771456.ACAD_SPLAN_SID = T955760.ACAD_SPLAN_SID
         AND T955760.EFFDT_END >= NVL (T771456.TERM_END_DT, TRUNC (SYSDATE))
         AND NVL (T771456.TERM_END_DT, TRUNC (SYSDATE)) >= T955760.EFFDT_START
         AND T955758.DEG_SID = T963945.DEG_SID
         AND T955761.ACAD_GRP_SID = T991524.ACAD_GRP_SID
         AND T771448.ACAD_CAR_SID = T771456.ACAD_CAR_SID
         AND T771448.TERM_SID = T771456.TERM_SID
         AND T771448.INSTITUTION_SID = T771456.INSTITUTION_SID
         AND T771448.SRC_SYS_ID = T771456.SRC_SYS_ID
         AND T771448.PERSON_SID = T771456.PERSON_SID
         AND T771448.TERM_SID = T992910.EFF_TERM_SID
         AND T771448.SRC_SYS_ID = T992910.SRC_SYS_ID
         AND T771448.PERSON_SID = T992910.PERSON_SID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_S_SR_DATA rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_S_SR_DATA',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

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

END UM_S_SR_DATA_P;
/
