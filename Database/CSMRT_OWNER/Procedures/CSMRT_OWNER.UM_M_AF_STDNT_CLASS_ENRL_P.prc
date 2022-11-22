DROP PROCEDURE CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL_P
/

--
-- UM_M_AF_STDNT_CLASS_ENRL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_M_AF_STDNT_CLASS_ENRL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Created                    -- Smitha Paul
--Date                       -- 3/24/2022
--Loads table                -- UM_M_AF_STDNT_CLASS_ENRL


------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_M_AF_STDNT_CLASS_ENRL';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_M_AF_STDNT_CLASS_ENRL');


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL';				
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL 
  --BDL Data
  SELECT DISTINCT
           /*+PARALLEL(8) inline*/
           INST.INSTITUTION_CD,
           TERM.TERM_CD,
           TERM.TERM_LD,
           TO_CHAR (TERM.AID_YEAR)
               AS FISCAL_YEAR,
           PERS.PERSON_ID,
           ACAD_CAR_1.ACAD_CAR_CD,
           ACAD_CAR_1.ACAD_CAR_LD,
           RSD.RSDNCY_LD,
           RSD.RSDNCY_ID,
           ST_TERM.TOT_CREDITS,
           ST_TERM.DAY_CREDITS,
           ST_TERM.CE_CREDITS,
           ST_TERM.ONLINE_CREDITS,
           ST_TERM.ONLINE_CREDITS - ST_TERM.CE_ONLINE_CREDITS
               DAY_ONLINE_CREDITS,
           ST_TERM.CE_ONLINE_CREDITS,
           ST_TERM.TOT_FTE,
           ST_TERM.DAY_FTE,
           ST_TERM.CE_FTE,
           CASE
               WHEN ST_TERM.TOT_CREDITS >= 12 THEN 1
               ELSE ST_TERM.TOT_CREDITS / 12
           END
               AS TOT_FFTE,
           CASE
               WHEN ST_TERM.DAY_CREDITS >= 12 THEN 1
               ELSE ST_TERM.DAY_CREDITS / 12
           END
               AS DAY_FFTE,
           CASE
               WHEN ST_TERM.CE_CREDITS >= 12 THEN 1
               ELSE ST_TERM.CE_CREDITS / 12
           END
               AS CE_FFTE,
           ACAD_PROG.ACAD_PROG_CD,
           ACAD_PROG.ACAD_PROG_LD,
           ACAD_GRP_1.ACAD_GRP_CD
               PROG_GRP_CD,
           ACAD_GRP_1.ACAD_GRP_LD
               PROG_GRP_LD,
           ACAD_ORD_1.ACAD_ORG_CD
               PROG_ORG_CD,
           ACAD_ORD_1.ACAD_ORG_LD
               PROG_ORG_LD,
           ACAD_PLAN.ACAD_PLAN_CD,
           ACAD_PLAN.ACAD_PLAN_LD,
           ACAD_ORG_2.ACAD_ORG_CD
               PLAN_ORG_CD,
           ACAD_ORG_2.ACAD_ORG_LD
               PLAN_ORG_LD,
           ACAD_OWN.PERCENT_OWNED / 100
               PLAN_PERCENT_OWNED,
           ACAD_STRUC.DEGREE_SEEKING_FLG,
           --CLASS ATTRS
           ROUND (
               ST_ENRL.TAKEN_UNIT * NVL ((ACAD_OWN.PERCENT_OWNED / 100), 1),
               3)
               TAKEN_UNIT,
           ROUND (
               CASE
                   WHEN ST_TERM.TOT_CREDITS >= 12
                   THEN
                         (  ST_ENRL.TAKEN_UNIT
                          * NVL ((ACAD_OWN.PERCENT_OWNED / 100), 1))
                       / ST_TERM.TOT_CREDITS
                   ELSE
                         (  ST_ENRL.TAKEN_UNIT
                          * NVL ((ACAD_OWN.PERCENT_OWNED / 100), 1))
                       / 12
               END,
               3)
               CLASS_FFTE,
           CLA.CLASS_NUM,
           CLA.CRSE_CD,
           CLA.SBJCT_CD,
           CLA.SBJCT_LD,
           CLA.CATALOG_NBR,
           CLA.DESCR
               CLASS_TITLE,
           ACAD_CAR.ACAD_CAR_CD
               CLASS_CAREER_CD,
           ACAD_CAR.ACAD_CAR_LD
               CLASS_CAREER_LD,
           ACAD_GRP.ACAD_GRP_CD
               CLASS_GRP_CD,
           ACAD_GRP.ACAD_GRP_LD
               CLASS_GRP_LD,
           ACAD_ORG.ACAD_ORG_CD
               CLASS_ORG_CD,
           ACAD_ORG.ACAD_ORG_LD
               CLASS_ORG_LD,
           INSTR_MODE.INSTRCTN_MODE_CD,
           INSTR_MODE.INSTRCTN_MODE_LD,
           CLA.CRSE_LVL,
           ROW_NUMBER ()
               OVER (PARTITION BY INST.INSTITUTION_CD,
                                  TERM.TERM_CD,
                                  PERS.PERSON_ID,
                                  ACAD_CAR_1.ACAD_CAR_CD
                     ORDER BY
                         INST.INSTITUTION_CD,
                         TERM.TERM_CD,
                         PERS.PERSON_ID,
                         ACAD_CAR_1.ACAD_CAR_CD,
                         CLA.CLASS_NUM)
               ROW_NUM,
               sysdate as insert_time
      FROM CSMRT_OWNER.UM_D_INSTRCTN_MODE_VW      INSTR_MODE /* SR Class - D_INSTRCTN_MODE */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_CAR_VW           ACAD_CAR /* SR Class - D_ACAD_CAR */
                                                         ,
           CSMRT_OWNER.UM_D_SESSION_VW            SES /* SR Class - D_SESSION */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_ORG_VW           ACAD_ORG /* SR Class - D_ACAD_ORG */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_GRP_VW           ACAD_GRP /* SR Class - D_ACAD_GRP */
                                                         ,
           CSMRT_OWNER.UM_D_CLASS_VW              CLA /* SR/FA Class - D_CLASS */
                                                         ,
           CSMRT_OWNER.UM_D_ENRLMT_STAT_VW        ENRL_STAT /* SR/FA Enrollment - D_ENRLMT_STAT */
                                                         ,
           CSMRT_OWNER.UM_D_RSDNCY_VW             RSD /* D_RSDNCY - Tuition */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_ORG_VW           ACAD_ORD_1 /* SR Structure - D_ACAD_ORG - Program */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_PROG_VW          ACAD_PROG /* SR Structure - D_ACAD_PROG */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_GRP_VW           ACAD_GRP_1 /* SR Structure - D_ACAD_GRP */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_ORG_VW           ACAD_ORG_2 /* SR Structure - D_ACAD_ORG - Plan */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_PLAN_VW          ACAD_PLAN /* SR Structure - D_ACAD_PLAN */
           LEFT OUTER JOIN CSMRT_OWNER.UM_R_ACAD_PLAN_OWNER_VW ACAD_OWN /* SR Structure - R_ACAD_PLAN_OWNER */
               ON     ACAD_PLAN.INSTITUTION_CD = ACAD_OWN.INSTITUTION_CD
                  AND ACAD_PLAN.ACAD_PLAN_CD = ACAD_OWN.ACAD_PLAN_CD
                  AND ACAD_PLAN.SRC_SYS_ID = ACAD_OWN.SRC_SYS_ID,
           CSMRT_OWNER.UM_D_INSTITUTION_VW        INST  /* D_INSTITUTION */
                                                         ,
           CSMRT_OWNER.UM_D_PERSON_CS_VW          PERS       /* D_PERSON */
                                                         ,
           CSMRT_OWNER.UM_D_ACAD_CAR_VW           ACAD_CAR_1     /* D_ACAD_CAR */
                                                         ,
           CSMRT_OWNER.UM_D_TERM_VW               TERM         /* D_TERM */
                                                         ,
           CSMRT_OWNER.UM_F_STDNT_TERM_VW         ST_TERM   /* F_STDNT_TERM */
                                                         ,
           CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT_VW  ACAD_STRUC /* F_STDNT_ACAD_STRUCT */
                                                         ,
           CSMRT_OWNER.UM_F_STDNT_ENRL_VW         ST_ENRL   /* F_STDNT_ENRL */
                                                         ,
           CSMRT_OWNER.UM_R_PERSON_RSDNCY_VW      PER_RSD /* R_PERSON_RSDNCY */
     WHERE     SES.SESSION_SID = CLA.SESSION_SID
           AND ACAD_ORG.ACAD_ORG_SID = CLA.ACAD_ORG_SID
           AND SES.ACAD_CAR_SID = ACAD_CAR.ACAD_CAR_SID
           AND ST_ENRL.CLASS_SID = CLA.CLASS_SID
           AND ST_ENRL.ENRLMT_STAT_SID = ENRL_STAT.ENRLMT_STAT_SID
           AND ACAD_PROG.ACAD_ORG_SID = ACAD_ORD_1.ACAD_ORG_SID
           AND INSTR_MODE.INSTRCTN_MODE_SID = CLA.INSTRCTN_MODE_SID
           AND ACAD_GRP.ACAD_GRP_SID = CLA.ACAD_GRP_SID
           AND INST.INSTITUTION_SID = ST_TERM.INSTITUTION_SID
           AND PERS.PERSON_SID = ST_TERM.PERSON_SID
           AND ACAD_CAR_1.ACAD_CAR_SID = ST_TERM.ACAD_CAR_SID
           AND TERM.TERM_SID = ST_TERM.TERM_SID
           AND ACAD_PROG.ACAD_GRP_SID = ACAD_GRP_1.ACAD_GRP_SID
           AND ACAD_STRUC.ACAD_PROG_SID = ACAD_PROG.ACAD_PROG_SID
           AND ACAD_ORG_2.ACAD_ORG_SID = NVL (ACAD_OWN.ACAD_ORG_SID, 2147483646)
           AND RSD.RSDNCY_SID = PER_RSD.TUITION_RSDNCY_SID
           AND ACAD_STRUC.ACAD_PLAN_SID = ACAD_PLAN.ACAD_PLAN_SID
           AND ACAD_PROG.EFFDT_END >=
               NVL (ACAD_STRUC.TERM_END_DT, TRUNC (SYSDATE))
           AND ST_TERM.ACAD_CAR_SID = ACAD_STRUC.ACAD_CAR_SID
           AND ST_TERM.TERM_SID = ACAD_STRUC.TERM_SID
           AND ST_TERM.INSTITUTION_SID = ACAD_STRUC.INSTITUTION_SID
           AND ST_TERM.SRC_SYS_ID = ACAD_STRUC.SRC_SYS_ID
           AND ST_TERM.PERSON_SID = ACAD_STRUC.PERSON_SID
           AND ST_TERM.TERM_SID = PER_RSD.EFF_TERM_SID
           AND ST_TERM.SRC_SYS_ID = PER_RSD.SRC_SYS_ID
           AND ST_TERM.PERSON_SID = PER_RSD.PERSON_SID
           AND ST_TERM.ACAD_CAR_SID = ST_ENRL.ACAD_CAR_SID
           AND ST_TERM.TERM_SID = ST_ENRL.TERM_SID
           AND ST_TERM.INSTITUTION_SID = ST_ENRL.INSTITUTION_SID
           AND ST_TERM.SRC_SYS_ID = ST_ENRL.SRC_SYS_ID
           AND ST_TERM.PERSON_SID = ST_ENRL.PERSON_SID
           AND TERM.TERM_CD >= '2810' --TERM.TERM_CD = '3010' --AND PERS.PERSON_ID = '00836270' --AND CLA.CLASS_NUM = '3250'
           AND ST_TERM.ENROLL_FLG = 'Y'
           AND ACAD_STRUC.UNDUP_STDNT_CNT = 1
           AND ENRL_STAT.ENRLMT_STAT_ID = 'E'
           AND 0 < ST_ENRL.TAKEN_UNIT
           AND NVL (ACAD_STRUC.TERM_END_DT, TRUNC (SYSDATE)) >=
               ACAD_PLAN.EFFDT_START
           AND NVL (ACAD_STRUC.TERM_END_DT, TRUNC (SYSDATE)) BETWEEN ACAD_PROG.EFFDT_START
                                                              AND ACAD_PLAN.EFFDT_END
    UNION ALL
    --AMH Data
    SELECT /*+PARALLEL(8) inline*/
           INSTITUTION_CD,
           TERM_CD,
           TERM_LD,
           TO_CHAR (AID_YEAR)                  AS FISCAL_YEAR,
           PERSON_ID,
           ACAD_CAR_CD,
           CASE
               WHEN CE_ONLY_FLG = 'Y'
               THEN
                   'Continuing Ed'
               WHEN ACAD_CAR_CD = 'GRAD'
               THEN
                   'Graduate'
               WHEN ACAD_CAR_CD = 'UGRD'
               THEN
                   'Undergraduate'
               WHEN ACAD_CAR_CD = 'ND' AND ACAD_PROG_CD = 'ND-GR'
               THEN
                   'Graduate'
               WHEN ACAD_CAR_CD = 'ND' AND ACAD_PROG_CD = 'ND-UG'
               THEN
                   'Undergraduate'
               WHEN ACAD_CAR_CD = 'ND' AND ACAD_PROG_CD = 'ND-CE'
               THEN
                   'Continuing Ed'
           END                                 ACAD_CAR_LD,
           RSDNCY_LD,
           RSDNCY_ID,
           TOT_CREDITS,
           NON_CE_CREDITS                      AS day_Credits,
           CE_CREDITS,
           ONLINE_CREDITS,
           CAST (NULL AS NUMBER)               DAY_ONLINE_CREDITS,
           CAST (NULL AS NUMBER)               CE_ONLINE_CREDITS,
           TOT_FTE,
           CAST (NULL AS NUMBER)               DAY_FTE,
           CAST (NULL AS NUMBER)               CE_FTE,
           ROUND (
               CASE WHEN TOT_CREDITS >= 12 THEN 1 ELSE TOT_CREDITS / 12 END,
               3)                              TOT_FFTE,
           CAST (NULL AS NUMBER)               day_ffte,
           CAST (NULL AS NUMBER)               ce_ffte,
           ACAD_PROG_CD,
           ACAD_PROG_LD,
           CAST (NULL AS VARCHAR2 (30))        PROG_GRP_CD,
           CAST (NULL AS VARCHAR2 (30))        PROG_GRP_LD,
           ACAD_ORG_CD                         AS PROG_ORG_CD,
           ACAD_ORG_LD                         AS PROG_ORG_LD,
           ACAD_PLAN_CD,
           ACAD_PLAN_LD,
           CAST (NULL AS VARCHAR2 (30))        PLAN_ORG_CD,
           CAST (NULL AS VARCHAR2 (30))        PLAN_ORG_LD,
           CAST (NULL AS NUMBER)               PLAN_PERCENT_OWNED,
           CAST (NULL AS VARCHAR2 (5))         DEGREE_SEEKING_FLG,
           ENRL.CREDIT_HOURS                   AS TAKEN_UNIT,
           ROUND (
               CASE
                   WHEN TOT_CREDITS >= 12
                   THEN
                       ENRL.CREDIT_HOURS / TOT_CREDITS
                   ELSE
                       ENRL.CREDIT_HOURS / 12
               END,
               3)                              CLASS_FFTE,
           ENRL.CLASS_NUMBER                   AS CLASS_NUM,
           CAST (NULL AS VARCHAR2 (10))        AS CRSE_CD,
           CAST (NULL AS VARCHAR2 (10))        AS SBJCT_CD,
           CLS.CLASS_CRSE_SUBJECT              AS SBJCT_LD,
           CLS.CLASS_CATALOG_NUMBER            AS CATALOG_NBR,
           CLS.CLASS_TITLE_LD                  AS CLASS_TITLE,
           CASE
               WHEN CLS.CLASS_BRIDGE_CAREER = 'Graduate' THEN 'GRAD'
               WHEN CLS.CLASS_BRIDGE_CAREER = 'Undergraduate' THEN 'UGRD'
           END                                 CLASS_CAREER_CD,
           CLS.CLASS_BRIDGE_CAREER             AS CLASS_CAREER_LD,
           CAST (NULL AS VARCHAR2 (30))        AS CLASS_GRP_CD,
           CAST (NULL AS VARCHAR2 (30))        AS CLASS_GRP_LD,
           CLS.CLASS_BRIDGE_DEPT               AS CLASS_ORG_CD,
           CLS.DEPT_LABEL                      AS CLASS_ORG_LD,
           CASE
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'Indep. Study/Research'
               THEN
                   'IS'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'In Person'
               THEN
                   'P'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'Fully Remote'
               THEN
                   'FR'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'Multimodal'
               THEN
                   'MM'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'Online'
               THEN
                   'O'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'Internet Based'
               THEN
                   'WW'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'Blended'
               THEN
                   'B'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'In-Person Plus Online'
               THEN
                   'PP'
               WHEN CLS.CLASS_INSTRUCTION_MODE_LD = 'Online Plus In Person'
               THEN
                   'OP'
           END                                 INSTRCTN_MODE_CD,
           CLS.CLASS_INSTRUCTION_MODE_LD       AS INSTRCTN_MODE_LD,
           CAST (NULL AS VARCHAR2 (30))        CRSE_LVL,
           ROW_NUMBER ()
               OVER (PARTITION BY STU.INSTITUTION_CD,
                                  STU.TERM_CD,
                                  STU.PERSON_ID,
                                  STU.ACAD_CAR_CD
                     ORDER BY
                         STU.INSTITUTION_CD,
                         STU.TERM_CD,
                         STU.PERSON_ID,
                         STU.ACAD_CAR_CD,
                         ENRL.CLASS_NUMBER)    ROW_NUM,
                          sysdate as insert_time
      FROM CSMRT_OWNER.UM_M_AF_STDNT_ENRL_AMH  STU,
           AMSTG_OWNER.AM_ENRL_VW              ENRL,
           AMSTG_OWNER.AM_CRSE_VW              CLS
     WHERE     STU.PERSON_ID = ENRL.STDNT_EMPLID
           AND STU.TERM_CD = ENRL.TIME_STRM
           AND ENRL.CLASS_NUMBER = CLS.CLASS_NUMBER
           AND ENRL.TIME_STRM = CLS.TIME_STRM;
           
           
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_M_AF_STDNT_CLASS_ENRL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_M_AF_STDNT_CLASS_ENRL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_CLASS_ENRL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_M_AF_STDNT_CLASS_ENRL');

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

END UM_M_AF_STDNT_CLASS_ENRL_P;
/
