DROP PROCEDURE CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT_P
/

--
-- UM_F_STDNT_ACAD_STRUCT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_STDNT_ACAD_STRUCT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_STDNT_ACAD_STRUCT
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_ACAD_STRUCT';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_ACAD_STRUCT');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT disable constraint PK_UM_F_STDNT_ACAD_STRUCT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_STDNT_ACAD_STRUCT
with PROG as (
select /*+ INLINE PARALLEL(8) */
       TERM_SID,
       PERSON_SID,
       STDNT_CAR_NUM,
       SRC_SYS_ID,
       INSTITUTION_CD,
       ACAD_CAR_CD,
       TERM_CD,
       PERSON_ID,
       ACAD_PROG_CD,
       EFFDT,
       EFFSEQ,
       ACTION_DT,
       ACAD_CAR_SID,
       ACAD_PROG_SID,
       ADMIT_TERM_SID,
       CAMPUS_SID,
       COMPL_TERM_SID,
       EXP_GRAD_TERM_SID,
       INSTITUTION_SID,
       REQ_TERM_SID,
       PROG_STAT_SID,
       PROG_ACN_SID,
       PROG_ACN_RSN_SID,
       STACK_BEGIN_TERM_SID,
       STACK_READMIT_TERM_SID,
       STACK_READMIT_EFFDT,         -- Added Dec 2015
       ADM_APPL_NBR,
       DATA_FROM_ADM_APPL_FLG,
       DEGR_CHKOUT_STAT,
       DEGR_CHKOUT_LAST,            -- Added Sept 2015
       DEGR_CHKOUT_LAST_EGT,        -- Added Dec 2015
       DEGR_CHKOUT_LAST_EGT_EFFDT,  -- Added Dec 2015
       DEGR_CHKOUT_LAST_EGT_ORDER,  -- Added Dec 2015
       MIN_PROG_STAT_CTGRY,
       PROG_CNT,
       max(EFFDT) over (partition by INSTITUTION_CD, ACAD_CAR_CD, STDNT_CAR_NUM, PERSON_ID, SRC_SYS_ID) MAX_EFFDT,
       max(TERM_CD) over (partition by INSTITUTION_CD, ACAD_CAR_CD, STDNT_CAR_NUM, PERSON_ID, SRC_SYS_ID) MAX_TERM_CD
  from UM_F_ACAD_PROG
 where TERM_SID <> 2147483646
   and PERSON_SID <> 2147483646),
RQ as (
select /*+ INLINE PARALLEL(8) */
       REQUIREMENT, EFFDT, RQ_LINE_KEY_NBR, SRC_SYS_ID,
       RQ_LINE_NBR,
       CONDITION_DATA,
       row_number() over (partition by REQUIREMENT, RQ_LINE_KEY_NBR, SRC_SYS_ID order by EFFDT desc) RQ_ORDER
  from CSSTG_OWNER.PS_RQ_LINE_TBL
 where DATA_ORIGIN <> 'D'
   and CONDITION_CODE = 'PL'
   and GPA_REQUIRED > 0),
RQ2 as (
select /*+ INLINE PARALLEL(8) */ distinct
       R.INSTITUTION INSTITUTION_CD, R.SAA_CAREER_RPT ACAD_CAR_CD, R.EMPLID PERSON_ID, RQ.CONDITION_DATA ACAD_PLAN_CD, R.SRC_SYS_ID,
       S.GPA_ACTUAL PLAN_GPA,
       trunc(R.SAA_RPT_DTTM_STAMP) PLAN_GPA_DT,
--       row_number() over (partition by R.INSTITUTION, R.SAA_CAREER_RPT, R.EMPLID, RQ.CONDITION_DATA, R.SRC_SYS_ID order by S.GPA_REQUIRED desc) RES_ORDER,    -- Mar 2017
       count(*) over (partition by R.INSTITUTION, R.SAA_CAREER_RPT, R.EMPLID, RQ.CONDITION_DATA, R.SRC_SYS_ID) RES_CNT      -- Mar 2017
  from CSSTG_OWNER.PS_SAA_ADB_REPORT R
  join CSSTG_OWNER.PS_SAA_ADB_RESULTS S
    on R.EMPLID = S.EMPLID
   and R.ANALYSIS_DB_SEQ = S.ANALYSIS_DB_SEQ
   and R.SAA_CAREER_RPT = S.SAA_CAREER_RPT
   and R.SRC_SYS_ID = S.SRC_SYS_ID
   and S.GPA_REQUIRED > 0
  join RQ
    on S.REQUIREMENT = RQ.REQUIREMENT
   and S.SRC_SYS_ID = RQ.SRC_SYS_ID
   and RQ.RQ_ORDER = 1
-- where R.TSCRPT_TYPE in ('DGPA'))        -- Mar 2017
 where R.TSCRPT_TYPE in ('DGPA','LGPA')),        -- Mar 2017
S2 as (
   SELECT /*+ INLINE PARALLEL(8) */
          G.TERM_SID,
          G.PERSON_SID,
          G.STDNT_CAR_NUM,
          L.ACAD_PLAN_SID,
          L.ACAD_SPLAN_SID,
          G.SRC_SYS_ID,
          G.INSTITUTION_CD,
          G.ACAD_CAR_CD,
          G.TERM_CD,
          G.PERSON_ID,
          G.ACAD_PROG_CD,
          L.ACAD_PLAN_CD,
          L.ACAD_SPLAN_CD,
          G.EFFDT,
          G.EFFSEQ,
          G.ACTION_DT,                       -- Name change. Was ACTION_DT_SID
          G.ACAD_CAR_SID,
          G.ACAD_PROG_SID,
          G.ADMIT_TERM_SID,
          G.CAMPUS_SID,
          G.COMPL_TERM_SID,
          G.EXP_GRAD_TERM_SID,
          G.INSTITUTION_SID,
          G.REQ_TERM_SID,
          G.PROG_STAT_SID,
          G.PROG_ACN_SID,
          G.PROG_ACN_RSN_SID,
          G.STACK_BEGIN_TERM_SID,
          G.STACK_READMIT_TERM_SID,
          L.CALC_TERM_LIMIT_SID,            -- Added Feb 2016
          L.NEW_TERM_LIMIT_SID,             -- Added Feb 2016
          L.PLAN_COMPL_TERM_SID,
          L.PLAN_REQ_TERM_SID,
          L.SPLAN_REQ_TERM_SID,
          G.ADM_APPL_NBR,
          L.CERTIFICATE_ONLY_FLG,
          L.D_RANK,
          L.D_RANK_PTYPE,
          L.D_RANK_SPLAN,                  -- Name change. Was D_RANK_SUB_PLAN
          G.DATA_FROM_ADM_APPL_FLG,
          G.DEGR_CHKOUT_STAT,
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGR_CHKOUT_STAT'
                     AND X.FIELDVALUE = G.DEGR_CHKOUT_STAT),
             ' ')
             DEGR_CHKOUT_STAT_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGR_CHKOUT_STAT'
                     AND X.FIELDVALUE = G.DEGR_CHKOUT_STAT),
             ' ')
             DEGR_CHKOUT_STAT_LD,
          G.DEGR_CHKOUT_LAST,        -- Added Sept 2015
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGR_CHKOUT_STAT'
                     AND X.FIELDVALUE = G.DEGR_CHKOUT_LAST),
             ' ')
             DEGR_CHKOUT_LAST_SD,
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGR_CHKOUT_STAT'
                     AND X.FIELDVALUE = G.DEGR_CHKOUT_LAST),
             ' ')
             DEGR_CHKOUT_LAST_LD,
          G.DEGR_CHKOUT_LAST_EGT,               -- Added Dec 2015
          NVL (
             (SELECT MIN (X.XLATSHORTNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGR_CHKOUT_STAT'
                     AND X.FIELDVALUE = G.DEGR_CHKOUT_LAST_EGT),
             ' ')
             DEGR_CHKOUT_LAST_EGT_SD,           -- Added Dec 2015
          NVL (
             (SELECT MIN (X.XLATLONGNAME)
                FROM UM_D_XLATITEM_VW X
               WHERE     X.FIELDNAME = 'DEGR_CHKOUT_STAT'
                     AND X.FIELDVALUE = G.DEGR_CHKOUT_LAST_EGT),
             ' ')
             DEGR_CHKOUT_LAST_EGT_LD,           -- Added Dec 2015
          G.DEGR_CHKOUT_LAST_EGT_EFFDT,         -- Added Dec 2015
          G.DEGR_CHKOUT_LAST_EGT_ORDER,         -- Added Dec 2015
          L.DEGREE_SEEKING_FLG,
          L.ED_LVL_RANK,                        -- Name change. Was EDLVL_RANK
          L.MAJOR_RANK,                         -- Added Sept 2015
          L.MINOR_RANK,                         -- Added Sept 2015
          L.SPLAN_RANK,                         -- Added Sept 2015
          L.PRIM_STACK_CAREER_RANK,             -- Added Sept 2015
          L.PRIM_STACK_STDNT_RANK,              -- Added Sept 2015
          G.MIN_PROG_STAT_CTGRY,                       -- New?????????????????
          L.NULL_PLAN_TYPE_FLG MISSING_PROG_PLAN_FLG,
          L.PROGRAM_CATGRY,                     -- Name change. Was PROGRAM_IND
          L.PLAN_ADVIS_STAT,
          L.PLAN_DECLARE_DT,
          RQ2.PLAN_GPA,                         -- Added Mar 2017
          RQ2.PLAN_GPA_DT,                      -- Added Mar 2017
          L.PLAN_SEQUENCE,
          L.PLAN_DEGR_CHKOUT_STAT,
          L.PLAN_STDNT_DEGR_CD,                 -- Name change. Was DEGREE_NBR???
          L.SPLAN_DECLARE_DT,
          CASE WHEN G.STACK_BEGIN_TERM_SID = G.TERM_SID
                AND G.STACK_READMIT_TERM_SID <> G.TERM_SID      -- Feb 2018
               THEN 'Y'
               ELSE 'N'
           END STACK_BEGIN_FLG,
          CASE WHEN G.STACK_BEGIN_TERM_SID <> G.TERM_SID
                AND G.STACK_READMIT_TERM_SID <> G.TERM_SID
               THEN 'Y'
               ELSE 'N'
           END STACK_CONTINUE_FLG,
          G.STACK_READMIT_EFFDT,            -- Added Dec 2015
          CASE WHEN G.STACK_READMIT_TERM_SID = G.TERM_SID
               THEN 'Y'
               ELSE 'N'
           END STACK_READMIT_FLG,
          min(decode(G.EFFDT,G.MAX_EFFDT,G.TERM_CD,'9999')) over (partition by G.INSTITUTION_CD, G.ACAD_CAR_CD, G.STDNT_CAR_NUM, G.PERSON_ID, G.SRC_SYS_ID) STACK_LAST_UPD_TERM_CD, -- Added APR 2015
          L.DEG_LIMIT_EFFDT,                -- Added Feb 2016
          L.NEW_TERM_LIMIT_CD,              -- Added Feb 2016
          L.CALC_TERM_LIMIT_CD,             -- Added Feb 2016
          L.DEG_LIMIT_UM_OVRRIDE_EXTENSN,   -- Added Feb 2016
          L.DEG_LIMIT_YEARS,                -- Added Feb 2016
          L.DEG_LIMIT_COMMENTS_MSGS,        -- Added Feb 2016
          L.UMDAR_ED_LVL,        -- Name change. Was UMDAR_EDUCATION_LEVEL_IND
          CASE
             WHEN L.UMDAR_ED_LVL in ('SPG','SPU','SPL') THEN 'Non-Degree'
             WHEN L.UMDAR_ED_LVL in ('DR') THEN 'Doctorate'
             WHEN L.UMDAR_ED_LVL in ('JD') THEN 'Juris Doc'
             WHEN L.UMDAR_ED_LVL in ('GCT','UCT') THEN 'Cert'
             WHEN L.UMDAR_ED_LVL in ('10') THEN 'Freshman'
             WHEN L.UMDAR_ED_LVL in ('20') THEN 'Sophomore'
             WHEN L.UMDAR_ED_LVL in ('30') THEN 'Junior'
             WHEN L.UMDAR_ED_LVL in ('40') THEN 'Senior'
             WHEN L.UMDAR_ED_LVL in ('GR') THEN 'Graduate'
             ELSE '-'
          END
             UMDAR_ED_LVL_SD,           -- Nov 2017
          CASE
             WHEN L.UMDAR_ED_LVL in ('SPG') THEN 'Non-Degree Graduate'
             WHEN L.UMDAR_ED_LVL in ('SPU') THEN 'Non-Degree Undergraduate'
             WHEN L.UMDAR_ED_LVL in ('SPL') THEN 'Non-Degree Law'
             WHEN L.UMDAR_ED_LVL in ('DR')  THEN 'Doctorate'
             WHEN L.UMDAR_ED_LVL in ('JD')  THEN 'Juris Doctorate'
             WHEN L.UMDAR_ED_LVL in ('GCT') THEN 'Graduate Certificate'
             WHEN L.UMDAR_ED_LVL in ('UCT') THEN 'Undergraduate Certificate'
             WHEN L.UMDAR_ED_LVL in ('10')  THEN 'Freshman'
             WHEN L.UMDAR_ED_LVL in ('20')  THEN 'Sophomore'
             WHEN L.UMDAR_ED_LVL in ('30')  THEN 'Junior'
             WHEN L.UMDAR_ED_LVL in ('40')  THEN 'Senior'
             WHEN L.UMDAR_ED_LVL in ('GR')  THEN 'Graduate'
             ELSE '-'
          END
             UMDAR_ED_LVL_LD,           -- Nov 2017
          G.PROG_CNT,                           -- Name change. Was PROG_COUNT
          L.PRIM_PROG_MAJOR_1_CNT,
          L.PRIM_PROG_MAJOR_2_CNT,
          L.PRIM_PROG_MINOR_1_CNT,
          L.PRIM_PROG_MINOR_2_CNT,
          L.PRIM_PROG_OTHER_PLAN_CNT,
          L.SEC_PROG_MAJOR_1_CNT,
          L.SEC_PROG_MAJOR_2_CNT,
          L.SEC_PROG_MINOR_1_CNT,
          L.SEC_PROG_MINOR_2_CNT,
          L.SEC_PROG_OTHER_PLAN_CNT,
          L.PP_SUB_PLAN_11_CNT,
          L.PP_SUB_PLAN_12_CNT,
          L.PP_SUB_PLAN_21_CNT,
          L.PP_SUB_PLAN_22_CNT,
          L.SP_SUB_PLAN_11_CNT,
          L.SP_SUB_PLAN_12_CNT,
          L.SP_SUB_PLAN_21_CNT,
          L.SP_SUB_PLAN_22_CNT,
          ROW_NUMBER() OVER (PARTITION BY L.TERM_SID, L.PERSON_SID, L.STDNT_CAR_NUM, L.SRC_SYS_ID
                                 ORDER BY PRIM_PROG_MAJOR_1_CNT desc, PP_SUB_PLAN_11_CNT desc,
                                          PRIM_PROG_MAJOR_2_CNT desc, PRIM_PROG_MINOR_1_CNT desc, PRIM_PROG_MINOR_2_CNT desc, PRIM_PROG_OTHER_PLAN_CNT desc,
                                          PP_SUB_PLAN_11_CNT desc, PP_SUB_PLAN_12_CNT desc, PP_SUB_PLAN_21_CNT desc, PP_SUB_PLAN_22_CNT desc) PRIM_PROG_MAJOR1_ORDER
     from PROG G
     join UM_F_ACAD_PLAN L
       on G.TERM_SID = L.TERM_SID
      and G.PERSON_SID = L.PERSON_SID
      and G.STDNT_CAR_NUM = L.STDNT_CAR_NUM
      and G.SRC_SYS_ID = L.SRC_SYS_ID
--      and (G.TERM_CD >= '1010' or G.MAX_TERM_CD = G.TERM_CD)
      and (G.TERM_CD >= '1010' or G.MAX_TERM_CD >= '2010' or G.MAX_TERM_CD = G.TERM_CD)  -- Feb 2018
     left outer join RQ2
       on G.INSTITUTION_CD = RQ2.INSTITUTION_CD
      and G.ACAD_CAR_CD = RQ2.ACAD_CAR_CD
      and G.PERSON_ID = RQ2.PERSON_ID
      and L.ACAD_PLAN_CD = RQ2.ACAD_PLAN_CD
      and G.SRC_SYS_ID = RQ2.SRC_SYS_ID
      and RQ2.RES_CNT = 1)
select /*+ INLINE PARALLEL(8) */
       S2.TERM_SID,
       S2.PERSON_SID,
       S2.STDNT_CAR_NUM,
       S2.ACAD_PLAN_SID,
       S2.ACAD_SPLAN_SID,
       S2.SRC_SYS_ID,
       S2.INSTITUTION_CD,
       S2.ACAD_CAR_CD,
       S2.TERM_CD,
       S2.PERSON_ID,
       S2.ACAD_PROG_CD,
       S2.ACAD_PLAN_CD,
       S2.ACAD_SPLAN_CD,
       S2.EFFDT,
       S2.EFFSEQ,
       S2.ACTION_DT,
       S2.ACAD_CAR_SID,
       S2.ACAD_PROG_SID,
       S2.ADMIT_TERM_SID,
       S2.CAMPUS_SID,
       S2.COMPL_TERM_SID,
       S2.EXP_GRAD_TERM_SID,
       S2.INSTITUTION_SID,
       S2.REQ_TERM_SID,
       S2.PROG_STAT_SID,
       S2.PROG_ACN_SID,
       S2.PROG_ACN_RSN_SID,
       S2.STACK_BEGIN_TERM_SID,
       S2.STACK_READMIT_TERM_SID,
       S2.CALC_TERM_LIMIT_SID,
       S2.NEW_TERM_LIMIT_SID,
       S2.PLAN_COMPL_TERM_SID,
       S2.PLAN_REQ_TERM_SID,
       S2.SPLAN_REQ_TERM_SID,
       S2.ADM_APPL_NBR,
       S2.CERTIFICATE_ONLY_FLG,
       S2.D_RANK,
       S2.D_RANK_PTYPE,
       S2.D_RANK_SPLAN,
       S2.DATA_FROM_ADM_APPL_FLG,
       S2.DEGR_CHKOUT_STAT,
       S2.DEGR_CHKOUT_STAT_SD,
       S2.DEGR_CHKOUT_STAT_LD,
       S2.DEGR_CHKOUT_LAST,
       S2.DEGR_CHKOUT_LAST_SD,
       S2.DEGR_CHKOUT_LAST_LD,
       S2.DEGR_CHKOUT_LAST_EGT,
       S2.DEGR_CHKOUT_LAST_EGT_SD,
       S2.DEGR_CHKOUT_LAST_EGT_LD,
       S2.DEGR_CHKOUT_LAST_EGT_EFFDT,
       S2.DEGR_CHKOUT_LAST_EGT_ORDER,
       decode(PLAN_STDNT_DEGR_CD, '-', 0, to_number(PLAN_STDNT_DEGR_CD)) DEGREE_NBR,   -- Added Oct 2017
       S2.DEGREE_SEEKING_FLG,
       S2.ED_LVL_RANK,
       S2.MAJOR_RANK,
       S2.MINOR_RANK,
       S2.SPLAN_RANK,
       S2.PRIM_STACK_CAREER_RANK,
       S2.PRIM_STACK_STDNT_RANK,
       S2.MIN_PROG_STAT_CTGRY,
       S2.MISSING_PROG_PLAN_FLG,
       S2.PROGRAM_CATGRY,
       S2.PLAN_ADVIS_STAT,
       S2.PLAN_DECLARE_DT,
       S2.PLAN_GPA,
       S2.PLAN_GPA_DT,
       S2.PLAN_SEQUENCE,
       S2.PLAN_DEGR_CHKOUT_STAT,
       S2.PLAN_STDNT_DEGR_CD,
       S2.SPLAN_DECLARE_DT,
       S2.STACK_BEGIN_FLG,
       S2.STACK_CONTINUE_FLG,
       S2.STACK_READMIT_EFFDT,
       S2.STACK_READMIT_FLG,
       S2.STACK_LAST_UPD_TERM_CD,
       nvl(T2.TERM_SID,2147483646) STACK_LAST_UPD_TERM_SID,     -- Added Oct 2017
       T1.TERM_BEGIN_DT,                                         -- Added Oct 2017
       T1.TERM_END_DT,                                           -- Added Oct 2017
       S2.DEG_LIMIT_EFFDT,
       S2.NEW_TERM_LIMIT_CD,
       S2.CALC_TERM_LIMIT_CD,
       S2.DEG_LIMIT_UM_OVRRIDE_EXTENSN,
       S2.DEG_LIMIT_YEARS,
       S2.DEG_LIMIT_COMMENTS_MSGS,
       S2.UMDAR_ED_LVL,
       S2.UMDAR_ED_LVL_SD,
       S2.UMDAR_ED_LVL_LD,
       S2.PROG_CNT,
       S2.PRIM_PROG_MAJOR_1_CNT,
       S2.PRIM_PROG_MAJOR_2_CNT,
       S2.PRIM_PROG_MINOR_1_CNT,
       S2.PRIM_PROG_MINOR_2_CNT,
       S2.PRIM_PROG_OTHER_PLAN_CNT,
       S2.SEC_PROG_MAJOR_1_CNT,
       S2.SEC_PROG_MAJOR_2_CNT,
       S2.SEC_PROG_MINOR_1_CNT,
       S2.SEC_PROG_MINOR_2_CNT,
       S2.SEC_PROG_OTHER_PLAN_CNT,
       S2.PP_SUB_PLAN_11_CNT,
       S2.PP_SUB_PLAN_12_CNT,
       S2.PP_SUB_PLAN_21_CNT,
       S2.PP_SUB_PLAN_22_CNT,
       S2.SP_SUB_PLAN_11_CNT,
       S2.SP_SUB_PLAN_12_CNT,
       S2.SP_SUB_PLAN_21_CNT,
       S2.SP_SUB_PLAN_22_CNT,
       case when PRIM_STACK_CAREER_RANK = 1
             and PRIM_STACK_STDNT_RANK = 1
             and MAJOR_RANK = 1
             and nvl(SPLAN_RANK, 0) <= 1
            then 1
            else 0
        end UNDUP_STDNT_CNT,                               -- Added Oct 2017
       S2.PRIM_PROG_MAJOR1_ORDER,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from S2
  join PS_D_TERM T1
    on S2.TERM_SID = T1.TERM_SID
  left outer join PS_D_TERM T2
    on S2.INSTITUTION_CD = T2.INSTITUTION_CD
   and S2.ACAD_CAR_CD = T2.ACAD_CAR_CD
   and S2.STACK_LAST_UPD_TERM_CD = T2.TERM_CD
   and S2.SRC_SYS_ID = T2.SRC_SYS_ID
;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ACAD_STRUCT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ACAD_STRUCT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

insert into CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT
   (TERM_SID, PERSON_SID, STDNT_CAR_NUM, ACAD_PLAN_SID, ACAD_SPLAN_SID,
    SRC_SYS_ID, INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID,
    ACAD_PROG_CD, ACAD_PLAN_CD, ACAD_SPLAN_CD, EFFDT, EFFSEQ,
    ACTION_DT, ACAD_CAR_SID, ACAD_PROG_SID, ADMIT_TERM_SID, CAMPUS_SID,
    COMPL_TERM_SID, EXP_GRAD_TERM_SID, INSTITUTION_SID, REQ_TERM_SID, PROG_STAT_SID,
    PROG_ACN_SID, PROG_ACN_RSN_SID, STACK_BEGIN_TERM_SID, STACK_READMIT_TERM_SID, CALC_TERM_LIMIT_SID,
    NEW_TERM_LIMIT_SID, PLAN_COMPL_TERM_SID, PLAN_REQ_TERM_SID, SPLAN_REQ_TERM_SID, ADM_APPL_NBR,
    CERTIFICATE_ONLY_FLG, D_RANK, D_RANK_PTYPE, D_RANK_SPLAN, DATA_FROM_ADM_APPL_FLG,
    DEGR_CHKOUT_STAT, DEGR_CHKOUT_STAT_SD, DEGR_CHKOUT_STAT_LD, DEGR_CHKOUT_LAST, DEGR_CHKOUT_LAST_SD,
    DEGR_CHKOUT_LAST_LD, DEGR_CHKOUT_LAST_EGT, DEGR_CHKOUT_LAST_EGT_SD, DEGR_CHKOUT_LAST_EGT_LD, DEGR_CHKOUT_LAST_EGT_EFFDT,
    DEGR_CHKOUT_LAST_EGT_ORDER, DEGREE_NBR, DEGREE_SEEKING_FLG, ED_LVL_RANK, MAJOR_RANK, MINOR_RANK,
    SPLAN_RANK, PRIM_STACK_CAREER_RANK, PRIM_STACK_STDNT_RANK, MIN_PROG_STAT_CTGRY, MISSING_PROG_PLAN_FLG,
    PROGRAM_CATGRY, PLAN_ADVIS_STAT, PLAN_DECLARE_DT, PLAN_GPA, PLAN_GPA_DT, PLAN_SEQUENCE, PLAN_DEGR_CHKOUT_STAT,
    PLAN_STDNT_DEGR_CD, STACK_BEGIN_FLG, STACK_CONTINUE_FLG, STACK_READMIT_FLG, STACK_LAST_UPD_TERM_CD, STACK_LAST_UPD_TERM_SID,
    TERM_BEGIN_DT, TERM_END_DT, CALC_TERM_LIMIT_CD, DEG_LIMIT_YEARS, UMDAR_ED_LVL, UMDAR_ED_LVL_SD, UMDAR_ED_LVL_LD,
    PROG_CNT, PRIM_PROG_MAJOR_1_CNT, PRIM_PROG_MAJOR_2_CNT, PRIM_PROG_MINOR_1_CNT, PRIM_PROG_MINOR_2_CNT,
    PRIM_PROG_OTHER_PLAN_CNT, SEC_PROG_MAJOR_1_CNT, SEC_PROG_MAJOR_2_CNT, SEC_PROG_MINOR_1_CNT, SEC_PROG_MINOR_2_CNT,
    SEC_PROG_OTHER_PLAN_CNT, PP_SUB_PLAN_11_CNT, PP_SUB_PLAN_12_CNT, PP_SUB_PLAN_21_CNT, PP_SUB_PLAN_22_CNT,
    SP_SUB_PLAN_11_CNT, SP_SUB_PLAN_12_CNT, SP_SUB_PLAN_21_CNT, SP_SUB_PLAN_22_CNT, UNDUP_STDNT_CNT, PRIM_PROG_MAJOR1_ORDER,
    LOAD_ERROR, DATA_ORIGIN, CREATED_EW_DTTM, LASTUPD_EW_DTTM, BATCH_SID)
 values
   (2147483646, 2147483646, 0, 2147483646, 2147483646,
    'CS90', '-', '-', '-', '-',
    '-', '-', '-', TO_DATE(NULL), 0,
    TO_DATE(NULL), 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, 2147483646,
    2147483646, 2147483646, 2147483646, 2147483646, '',
    '-', NULL, NULL, NULL, '',
    '', '', '', '', '',
    '', '', '', '', TO_DATE(NULL),
    NULL, 0, '', NULL, NULL, NULL,
    NULL, NULL, NULL, '', '',
    '', '', TO_DATE(NULL), NULL, TO_DATE(NULL), NULL, '',
    '', '', '', '', '', 2147483646,
    '', NULL, '', '', '',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, 0, 1,
    'N', 'S', SYSDATE, SYSDATE, 1234)
;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ACAD_STRUCT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ACAD_STRUCT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT enable constraint PK_UM_F_STDNT_ACAD_STRUCT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_ACAD_STRUCT');

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

END UM_F_STDNT_ACAD_STRUCT_P;
/
