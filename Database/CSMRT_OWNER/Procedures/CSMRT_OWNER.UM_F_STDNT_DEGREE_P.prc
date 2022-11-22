DROP PROCEDURE CSMRT_OWNER.UM_F_STDNT_DEGREE_P
/

--
-- UM_F_STDNT_DEGREE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_STDNT_DEGREE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_STDNT_DEGREE
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_DEGREE';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_DEGREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_DEGREE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_DEGREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_DEGREE');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_DEGREE disable constraint PK_UM_F_STDNT_DEGREE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_DEGREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_DEGREE';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_STDNT_DEGREE
with NAMES as (
select /*+ INLINE PARALLEL(8) */
       N.EMPLID, N.NAME_TYPE, N.EFFDT, N.SRC_SYS_ID,
       nvl(min(N.EFFDT - .00001) OVER (PARTITION BY N.EMPLID, N.NAME_TYPE, N.SRC_SYS_ID
                                           ORDER BY N.EFFDT
                                       ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),
       to_date(99991231235959, 'YYYYMMDDHH24MISS'))  EFF_END_DT,
       P.PERSON_SID,
       N.EFF_STATUS, N.NAME, N.LAST_NAME, N.FIRST_NAME, N.MIDDLE_NAME
--  from CSSTG_OWNER.PS_S_NAMES N
  from CSSTG_OWNER.PS_NAMES N
  join PS_D_PERSON P
    on N.EMPLID = P.PERSON_ID
   and N.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
 where N.DATA_ORIGIN <> 'D'
   and N.EFF_STATUS = 'A'
   and N.NAME_TYPE IN ('DEG', 'PRI')
),
D0 as (
select /*+ INLINE PARALLEL(8) */
       DEGR.PERSON_SID, DEGR.DEGREE_NBR,
       --DEGR.HONORS_NBR,
       DEGR.ACAD_PLAN_SID, DEGR.ACAD_SPLAN_SID, DEGR.SRC_SYS_ID,
       DEGR.DEG_SID, DEGR.INSTITUTION_SID, DEGR.ACAD_CAR_SID, DEGR.COMPL_TERM_SID, DEGR.CONF_DT, DEGR.HONORS_PREFIX_SID, DEGR.HONORS_SUFFIX_SID,
       decode(DEGR.GPA_DEGREE,0,NULL,DEGR.GPA_DEGREE) GPA_DEGREE,
       DEGR.CLASS_RANK_NBR, DEGR.CLASS_RANK_TOT, DEGR.ACAD_DEGR_STAT_SID, DEGR.DEGR_STAT_DT,
       --DEGR.HONORS_CODE, DEGR.HNRS_AWD_DT,
       DEGR.PLAN_SEQUENCE, DEGR.ACAD_PLAN_CAR_SID, DEGR.STDNT_CAR_NBR, DEGR.PLAN_DEGR_STATUS, DEGR.PLN_DEG_ST_DT, DEGR.PLAN_OVERRIDE_FLG,
       DEGR.PLAN_DIPLOMA_DESCR, DEGR.PLAN_TRNSCR_DESCR, DEGR.PLN_HONRS_PREF_SID, DEGR.PLN_HONRS_SUFF_SID,
       decode(DEGR.GPA_PLAN,0,NULL,DEGR.GPA_PLAN) GPA_PLAN,
       DEGR.PLN_CLASS_RANK_NBR, DEGR.PLN_CLASS_RANK_TOT,
       DEGR.SPLAN_SEQUENCE, DEGR.SPLAN_OVERRIDE_FLG, DEGR.SPLAN_DIPLOMA_DESC, DEGR.SPLAN_TRNSCR_DESCR, DEGR.SPLN_HNRS_PREF_SID, DEGR.SPLN_HNRS_SUFF_SID,
       DEGR.DEGREE_COUNT_AWD, DEGR.DEGREE_COUNT_RVK, DEGR.DEGREE_COUNT,
       NVL(NAMES.EFFDT, TO_DATE('1/1/1900', 'MM/DD/YYYY')) AS EFFDT,
       NVL(NAMES.EFF_END_DT, TO_DATE('12/31/9999', 'MM/DD/YYYY')) AS EFF_END_DT,
       NAMES.NAME_TYPE, NAMES.NAME, NAMES.FIRST_NAME, NAMES.MIDDLE_NAME, NAMES.LAST_NAME
  from PS_F_DEGREES DEGR
--  left outer join UM_D_PERSON_NAME_HIST NAMES	-- Mar 2017
--    on DEGR.PERSON_SID = NAMES.PERSON_SID
--   and DEGR.SRC_SYS_ID = NAMES.SRC_SYS_ID
--   and NAMES.NAME_TYPE IN ('DEG', 'PRI')
--   and NAMES.EFF_STATUS = 'A'              -- Dec 2016
--   and NAMES.DATA_ORIGIN <> 'D'
  left outer join NAMES
    on DEGR.PERSON_SID = NAMES.PERSON_SID
   and DEGR.SRC_SYS_ID = NAMES.SRC_SYS_ID
 where DEGR.DATA_ORIGIN <> 'D'
),
D as (
select /*+ INLINE PARALLEL(8) */
       DEGR.PERSON_SID, DEGR.DEGREE_NBR,
       --DEGR.HONORS_NBR,
       DEGR.ACAD_PLAN_SID, DEGR.ACAD_SPLAN_SID, DEGR.SRC_SYS_ID,
       DEGR.DEG_SID, DEGR.INSTITUTION_SID, DEGR.ACAD_CAR_SID, DEGR.COMPL_TERM_SID, DEGR.CONF_DT, DEGR.HONORS_PREFIX_SID, DEGR.HONORS_SUFFIX_SID,
       DEGR.GPA_DEGREE, DEGR.CLASS_RANK_NBR, DEGR.CLASS_RANK_TOT, DEGR.ACAD_DEGR_STAT_SID, DEGR.DEGR_STAT_DT,
       --DEGR.HONORS_CODE, DEGR.HNRS_AWD_DT,
       DEGR.PLAN_SEQUENCE, DEGR.ACAD_PLAN_CAR_SID, DEGR.STDNT_CAR_NBR, DEGR.PLAN_DEGR_STATUS, DEGR.PLN_DEG_ST_DT, DEGR.PLAN_OVERRIDE_FLG,
       DEGR.PLAN_DIPLOMA_DESCR, DEGR.PLAN_TRNSCR_DESCR, DEGR.PLN_HONRS_PREF_SID, DEGR.PLN_HONRS_SUFF_SID, DEGR.GPA_PLAN, DEGR.PLN_CLASS_RANK_NBR, DEGR.PLN_CLASS_RANK_TOT,
       DEGR.SPLAN_SEQUENCE, DEGR.SPLAN_OVERRIDE_FLG, DEGR.SPLAN_DIPLOMA_DESC, DEGR.SPLAN_TRNSCR_DESCR, DEGR.SPLN_HNRS_PREF_SID, DEGR.SPLN_HNRS_SUFF_SID,
       DEGR.DEGREE_COUNT_AWD, DEGR.DEGREE_COUNT_RVK, DEGR.DEGREE_COUNT,
       DEGR.EFFDT, DEGR.EFF_END_DT,
       DEGR.NAME_TYPE, DEGR.NAME, DEGR.FIRST_NAME, DEGR.MIDDLE_NAME, DEGR.LAST_NAME,
       ROW_NUMBER() OVER (PARTITION BY DEGR.PERSON_SID, DEGR.DEGREE_NBR, DEGR.ACAD_PLAN_SID, DEGR.ACAD_SPLAN_SID, DEGR.SRC_SYS_ID
                              ORDER BY DECODE(DEGR.NAME_TYPE, 'DEG', 0, 'PRI', 1, 9)) NAME_ORDER
  from D0 DEGR
-- where to_date(NVL(DEGR.CONF_DT_SID,19000101),'YYYYMMDD') between EFFDT and EFF_END_DT)
 where DEGR.CONF_DT between EFFDT and EFF_END_DT)
select /*+ INLINE PARALLEL(8) */
    F.PERSON_SID,
    F.INSTITUTION_SID,
    F.ACAD_CAR_SID,
    F.STDNT_CAR_NUM,
    F.TERM_SID,
    F.ACAD_PROG_SID,
    F.ACAD_PLAN_SID,
    F.ACAD_SPLAN_SID,
    NVL(D.DEGREE_NBR,0) DEGREE_NBR,
    NVL(H.HONORS_NUM,0) HONORS_NBR,
    F.SRC_SYS_ID,
    F.PERSON_ID,
    F.INSTITUTION_CD,
    F.ACAD_CAR_CD,
    F.TERM_CD,
    F.ACAD_PROG_CD,
    F.ACAD_PLAN_CD,
    F.ACAD_SPLAN_CD,
    NVL(D.DEG_SID,2147483646) DEG_SID,
    NVL(H.DEG_HONORS_SID,2147483646) DEG_HONORS_SID,
    decode(H.HONORS_AWD_DT,NULL,NULL,TO_CHAR(H.HONORS_AWD_DT,'YYYYMMDD')) HONORS_AWD_DT_SID,
    H.HONORS_AWD_DT,
    NVL(D.COMPL_TERM_SID,2147483646) COMPL_TERM_SID,
--    D.CONF_DT_SID,
    decode(D.CONF_DT,NULL,NULL,TO_NUMBER(TO_CHAR(D.CONF_DT,'YYYYMMDD'))) CONF_DT_SID,
--    decode(D.CONF_DT_SID,NULL,NULL,TO_DATE(D.CONF_DT_SID,'YYYYMMDD')) CONF_DT,
    D.CONF_DT,
    NVL(D.HONORS_PREFIX_SID,2147483646) HONORS_PREFIX_SID,
    NVL(D.HONORS_SUFFIX_SID,2147483646) HONORS_SUFFIX_SID,
    D.GPA_DEGREE,
    NVL(D.CLASS_RANK_NBR,0) CLASS_RANK_NBR,
    NVL(D.CLASS_RANK_TOT,0) CLASS_RANK_TOT,
    NVL(D.ACAD_DEGR_STAT_SID,2147483646) ACAD_DEGR_STAT_SID,
--    D.DEGR_STAT_DT_SID,
    decode(D.DEGR_STAT_DT,NULL,NULL,TO_NUMBER(TO_CHAR(D.DEGR_STAT_DT,'YYYYMMDD'))) DEGR_STAT_DT_SID,
--    decode(D.DEGR_STAT_DT_SID,NULL,NULL,TO_DATE(D.DEGR_STAT_DT_SID,'YYYYMMDD')) DEGR_STAT_DT,
    D.DEGR_STAT_DT,
    NVL(D.PLAN_SEQUENCE,0) PLAN_SEQUENCE,
    NVL(D.PLAN_DEGR_STATUS,'-') PLAN_DEGR_STATUS,
--    D.PLN_DEG_ST_DT_SID,
    decode(D.PLN_DEG_ST_DT,NULL,NULL,TO_NUMBER(TO_CHAR(D.PLN_DEG_ST_DT,'YYYYMMDD'))) PLN_DEG_ST_DT_SID,
--    decode(D.PLN_DEG_ST_DT_SID,NULL,NULL,TO_DATE(D.PLN_DEG_ST_DT_SID,'YYYYMMDD')) PLN_DEG_ST_DT,
    D.PLN_DEG_ST_DT,
    NVL(D.PLAN_OVERRIDE_FLG,'-') PLAN_OVERRIDE_FLG,
    NVL(D.PLAN_DIPLOMA_DESCR,'-') PLAN_DIPLOMA_DESCR,
    NVL(D.PLAN_TRNSCR_DESCR,'-') PLAN_TRNSCR_DESCR,
    NVL(D.PLN_HONRS_PREF_SID,2147483646) PLN_HONRS_PREF_SID,
    NVL(D.PLN_HONRS_SUFF_SID,2147483646) PLN_HONRS_SUFF_SID,
    D.GPA_PLAN,
    NVL(D.PLN_CLASS_RANK_NBR,0) PLN_CLASS_RANK_NBR,
    NVL(D.PLN_CLASS_RANK_TOT,0) PLN_CLASS_RANK_TOT,
    NVL(D.SPLAN_SEQUENCE,0) SPLAN_SEQUENCE,
    NVL(D.SPLAN_OVERRIDE_FLG,'-') SPLAN_OVERRIDE_FLG,
    NVL(D.SPLAN_DIPLOMA_DESC,'-') SPLAN_DIPLOMA_DESC,
    NVL(D.SPLAN_TRNSCR_DESCR,'-') SPLAN_TRNSCR_DESCR,
    D.NAME DEG_NAME,
    D.FIRST_NAME DEG_FIRST_NAME,
    D.MIDDLE_NAME DEG_MIDDLE_NAME,
    D.LAST_NAME DEG_LAST_NAME,
    NVL(D.SPLN_HNRS_PREF_SID,2147483646) SPLN_HNRS_PREF_SID,
    NVL(D.SPLN_HNRS_SUFF_SID,2147483646) SPLN_HNRS_SUFF_SID,
    NVL(DEGREE_COUNT_AWD,0) AWARD_CNT,
    NVL((CASE WHEN H.HONORS_NUM > 0 THEN 1 ELSE 0 END),0) HONORS_CNT,
    NVL(DEGREE_COUNT_RVK,0) REVOKE_CNT,
    ROW_NUMBER() OVER (PARTITION BY F.PERSON_SID, F.INSTITUTION_SID, F.ACAD_CAR_SID, F.STDNT_CAR_NUM, F.TERM_SID, F.SRC_SYS_ID
                           ORDER BY PRIM_PROG_MAJOR_1_CNT DESC, PP_SUB_PLAN_11_CNT DESC, PRIM_PROG_MAJOR_2_CNT DESC,
                                    PRIM_PROG_MINOR_1_CNT DESC, PRIM_PROG_MINOR_2_CNT DESC,
                                    PRIM_PROG_OTHER_PLAN_CNT DESC, PP_SUB_PLAN_11_CNT DESC,
                                    PP_SUB_PLAN_12_CNT DESC, PP_SUB_PLAN_21_CNT DESC, PP_SUB_PLAN_22_CNT DESC) PRIM_PROG_MAJOR1_ORDER
  from UM_F_STDNT_ACAD_STRUCT F
  left outer join D
    on F.PERSON_SID = D.PERSON_SID
   and to_number(decode(F.PLAN_STDNT_DEGR_CD,'-',NULL,F.PLAN_STDNT_DEGR_CD)) = D.DEGREE_NBR
   and F.ACAD_PLAN_SID = D.ACAD_PLAN_SID
   and F.ACAD_SPLAN_SID = D.ACAD_SPLAN_SID
   and F.SRC_SYS_ID = D.SRC_SYS_ID
   and D.NAME_ORDER = 1
  left outer join PS_R_DEG_HONORS H
    on D.PERSON_SID = H.PERSON_SID
   and D.DEGREE_NBR = H.STDNT_DEGR_CD
   and D.SRC_SYS_ID = H.SRC_SYS_ID
   and H.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_DEGREE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_DEGREE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_DEGREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_DEGREE enable constraint PK_UM_F_STDNT_DEGREE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_DEGREE');

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

END UM_F_STDNT_DEGREE_P;
/
