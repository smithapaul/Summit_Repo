CREATE OR REPLACE PROCEDURE             UM_S_RA_DATA_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- James Doucette
--
-- Loads table CSMRT_OWNER.UM_S_RA_DATA.
--
-- V01  SMT-xxxx 05/28/2019,    James Doucette
--                              Converted from script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_S_RA_DATA';
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

strMessage01    := 'Deleting from table CSMRT_OWNER.UM_S_RA_DATA where RUN_DT = '  || TO_CHAR(trunc(SYSDATE));
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'delete CSMRT_OWNER.UM_S_RA_DATA where RUN_DT = trunc(SYSDATE)';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_S_RA_DATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_S_RA_DATA';	
insert /*+ append */ into CSMRT_OWNER.UM_S_RA_DATA
with TERM as (
select INSTITUTION_CD, TERM_CD, SRC_SYS_ID,
       case when substr(term_cd,3,2) in ('20','40','50') 
            then max(nvl(ADD_DROP_END_DT,SESSION_BEGIN_DT)+120) 
            else max(nvl(ADD_DROP_END_DT,SESSION_BEGIN_DT)+14)
        end MAX_ADD_DROP_END_DT,
       min(SESSION_BEGIN_DT-365) MIN_SESSION_BEGIN_DT 
  from PS_D_SESSION
-- where substr(TERM_CD,3,2) in ('10','30')     -- July 2019  
--   and ACAD_CAR_CD = 'UGRD' 
 group by INSTITUTION_CD, TERM_CD, SRC_SYS_ID)
SELECT /* parallel(16) */ DISTINCT 
       trunc(SYSDATE) RUN_DT,
       T571889.INSTITUTION_CD,
       T574419.TERM_CD,
       T575314.ADM_APPL_NBR,
       T574419.SRC_SYS_ID,
       T577144.ACAD_CAR_CD,
       T575377.ADMIT_TYPE_GRP,
       T575377.ADMIT_TYPE_ID,
       T575651.APPL_CNTR_ID,    -- May 2018 
       T575498.ACAD_PROG_CD,
       T575539.ACAD_PLAN_CD,
       T575438.ACAD_SPLAN_CD,
       T575539.EDU_LVL_CTGRY,
       T576824.EDU_LVL_CD,
       T576426.PROG_STAT_CD,
       T576542.PROG_ACN_CD,
       T576440.PROG_ACN_RSN_CD,
       T575314.ACTION_DT,
       T575314.APPL_CNT,
       T575314.APPL_COMPLETE_CNT,
       T575314.ADMIT_CNT,
       T575314.DENY_CNT,
       T575314.WAIT_CNT,
       T575314.DEPOSIT_CNT,
       T575314.MATRIC_CNT,
      CASE
         WHEN     T575314.MATRIC_CNT = 1
              AND T575314.ENROLL_CNT = 0
              AND T571889.INSTITUTION_CD = 'UMLOW'
              AND T577144.ACAD_CAR_CD IN ('CSCE', 'GRAD') THEN
           CASE
             WHEN SUBSTR (T574419.TERM_CD, 3, 2) = '10' THEN
               T990963.PREV_ENROLL_CNT
             WHEN SUBSTR (T574419.TERM_CD, 3, 2) IN ('40', '50') THEN
               T990963.NEXT_ENROLL_CNT
             ELSE
               T575314.ENROLL_CNT
           END
         ELSE
           T575314.ENROLL_CNT
       END ENROLL_CNT,
       T572353.PERSON_ID,
       T572353.GENDER_CD,
       CAST (
         FLOOR (
             MONTHS_BETWEEN (TO_DATE ('2018-02-15', 'YYYY-MM-DD') - 1,
                             T572353.BIRTH_DT)
           / 12) AS INTEGER) AGE,
       T721572.COUNTRY,
       T721572.CITIZENSHIP_STATUS,
       T718019.ETHNIC_GRP_CD ETHNIC_GRP_FED_CD,
       T718025.ETHNIC_GRP_CD ETHNIC_GRP_ST_CD,
       T993278.RSDNCY_ID,
       T992902.RSDNCY_ID TUITION_RSDNCY_ID,
       T575631.ACAD_GRP_CD          -- June 2019 
  FROM CSMRT_OWNER.UM_D_RSDNCY_VW T992902             /* D_RSDNCY - Tuition */
                                         ,
       CSMRT_OWNER.UM_D_RSDNCY_VW T993278                       /* D_RSDNCY */
                                         ,
       CSMRT_OWNER.UM_D_ACAD_PROG_VW T575498                 /* D_ACAD_PROG */
                                            ,
       CSMRT_OWNER.UM_D_ETHNIC_GRP_VW T718025         /* D_ETHNIC_GRP_STATE */
                                             ,
       CSMRT_OWNER.UM_D_ETHNIC_GRP_VW T718019       /* D_ETHNIC_GRP_FEDERAL */
                                             ,
       CSMRT_OWNER.UM_D_PERSON_CS_CITIZEN_USA_VW T721572 /* D_PERSON_CS_CITIZEN_USA */
                                                        ,
       CSMRT_OWNER.UM_D_ACAD_SPLAN_VW T575438               /* D_ACAD_SPLAN */
                                             ,
       CSMRT_OWNER.UM_D_PROG_ACN_VW T576542       /* Admission - D_PROG_ACN */
                                           ,
       CSMRT_OWNER.UM_D_PROG_ACN_RSN_VW T576440 /* Admission - D_PROG_ACN_RSN */
                                               ,
       CSMRT_OWNER.UM_D_PROG_STAT_VW T576426     /* Admission - D_PROG_STAT */
                                            ,
       CSMRT_OWNER.UM_D_ADMIT_TYPE_VW T575377               /* D_ADMIT_TYPE */
                                             ,
       CSMRT_OWNER.UM_D_APPL_CNTR_VW T575651 /* D_APPL_CNTR */ 
                                             ,
       CSMRT_OWNER.UM_D_INSTITUTION_VW T571889             /* D_INSTITUTION */
                                              ,
       CSMRT_OWNER.UM_D_PERSON_CS_VW T572353                    /* D_PERSON */
                                            ,
       CSMRT_OWNER.UM_D_TERM_VW T574419                /* D_TERM_Admit_Term */
                                       ,
       TERM,
       CSMRT_OWNER.UM_F_ADM_APPL_STAT_VW T575314      /* F_ADM_APPL_STAT_VW */
                                                ,
       CSMRT_OWNER.UM_D_ACAD_PLAN_VW T575539                 /* D_ACAD_PLAN */
                                            ,
       CSMRT_OWNER.UM_D_ACAD_CAR_VW T577144                   /* D_ACAD_CAR */
                                           ,
       CSMRT_OWNER.UM_F_ADM_APPL_ENRL_VW T990963         /* F_ADM_APPL_ENRL */
                                                ,
       CSMRT_OWNER.UM_R_PERSON_RSDNCY_VW T992910         /* R_PERSON_RSDNCY */
                                                ,
       CSMRT_OWNER.UM_D_DEG_VW T576824,                         /* D_DEG */
       CSMRT_OWNER.UM_D_ACAD_GRP_VW T575631         -- June 2019
 WHERE (    T572353.ETHNIC_GRP_ST_SID = T718025.ETHNIC_GRP_SID
        AND T572353.ETHNIC_GRP_FED_SID = T718019.ETHNIC_GRP_SID
        AND T572353.PERSON_SID = T721572.PERSON_SID
        AND T575438.ACAD_SPLAN_SID = NVL (T575314.ACAD_SPLAN_SID, 2147483646)
        AND T575498.ACAD_GRP_SID = T575631.ACAD_GRP_SID
        AND T992902.RSDNCY_SID = T992910.TUITION_RSDNCY_SID
        AND T992910.RSDNCY_SID = T993278.RSDNCY_SID
        AND T575314.PROG_ACN_SID = T576542.PROG_ACN_SID
        AND T575314.PROG_ACN_RSN_SID = T576440.PROG_ACN_RSN_SID
        AND T575314.PROG_STAT_SID = T576426.PROG_STAT_SID
        AND T575314.ADMIT_TYPE_SID = T575377.ADMIT_TYPE_SID
        AND T575314.APPL_CNTR_SID = T575651.APPL_CNTR_SID
        AND T571889.INSTITUTION_SID = T575314.INSTITUTION_SID
        AND T572353.PERSON_SID = T575314.APPLCNT_SID
        AND T574419.TERM_SID = T575314.ADMIT_TERM_SID
        AND T575498.ACAD_PROG_SID = NVL (T575314.ACAD_PROG_SID, 2147483646)
        AND T575438.EFFDT_END >= NVL (T575314.TERM_END_DT, TRUNC (SYSDATE))
        AND T575498.EFFDT_END >= NVL (T575314.TERM_END_DT, TRUNC (SYSDATE))
        AND T575539.DEG_SID = T576824.DEG_SID
        AND T575539.ACAD_PLAN_SID = NVL (T575314.ACAD_PLAN_SID, 2147483646)
        AND T575314.ACAD_CAR_SID = T577144.ACAD_CAR_SID
        AND T575314.ADM_APPL_SID = T990963.ADM_APPL_SID
        AND T575314.ADMIT_TERM_SID = T992910.EFF_TERM_SID
        AND T575314.APPLCNT_SID = T992910.PERSON_SID
        AND T575314.SRC_SYS_ID = T992910.SRC_SYS_ID
        AND NVL (T575314.TERM_END_DT, TRUNC (SYSDATE)) >= T575438.EFFDT_START
        and T574419.INSTITUTION_CD = TERM.INSTITUTION_CD
        and T574419.TERM_CD = TERM.TERM_CD
        and T574419.SRC_SYS_ID = TERM.SRC_SYS_ID
        and trunc(SYSDATE) between TERM.MIN_SESSION_BEGIN_DT and TERM.MAX_ADD_DROP_END_DT
        AND T575314.APPL_COUNT_ORDER = 1
        AND T575314.MAX_TERM_FLG = 'Y'
        AND T718019.ETHNIC_GRP_FED_FLG = 'Y'
        AND T718025.ETHNIC_GRP_ST_FLG = 'Y'
        AND NVL (T575314.TERM_END_DT, TRUNC (SYSDATE)) >= T575498.EFFDT_START
        AND NVL (T575314.TERM_END_DT, TRUNC (SYSDATE)) BETWEEN T575539.EFFDT_START
                                                           AND T575539.EFFDT_END)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_S_RA_DATA rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_S_RA_DATA',
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

END UM_S_RA_DATA_P;
/
