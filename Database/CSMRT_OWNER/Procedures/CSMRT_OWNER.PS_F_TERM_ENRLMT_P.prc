CREATE OR REPLACE PROCEDURE             "PS_F_TERM_ENRLMT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Loads table             -- PS_F_TERM_ENRLMT
-- PS_F_TERM_ENRLMT    -- UM_D_GRD_RSTR_TYPE ;PS_D_INSTITUTION ;PS_D_PERSON;PS_D_TERm;PS_D_ACAD_CAR;
                           -- PS_D_ACAD_GRP;PS_D_ACAD_LOAD;PS_D_ACAD_LVL;UM_D_ACAD_PROG;PS_D_ACAD_STNDNG;
-- V01 10/29/2018          -- srikanth ,pabbu converted to proc from sql
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_F_TERM_ENRLMT';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_F_TERM_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_F_TERM_ENRLMT');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_TERM_ENRLMT disable constraint PK_PS_F_TERM_ENRLMT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.PS_F_TERM_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_F_TERM_ENRLMT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_F_TERM_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_F_TERM_ENRLMT';				
insert /*+ append */ into PS_F_TERM_ENRLMT 
  with STND as (
select /*+ parallel(8) inline */
       EMPLID, ACAD_CAREER, INSTITUTION, STRM, SRC_SYS_ID, ACAD_STNDNG_ACTN,
       row_number() over (partition by EMPLID, ACAD_CAREER, INSTITUTION, STRM, SRC_SYS_ID 
                              order by EFFDT desc, EFFSEQ desc) STND_ORDER
  from CSSTG_OWNER.PS_ACAD_STDNG_ACTN 
 where DATA_ORIGIN <> 'D')
SELECT /*+ parallel(8) inline */
       F.INSTITUTION INSTITUTION_CD, 
       F.ACAD_CAREER ACAD_CAR_CD, 
       F.STRM TERM_CD, 
       F.EMPLID PERSON_ID, 
       F.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
       nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID, 
	   nvl(D.TERM_SID, 2147483646) TERM_SID,
       nvl(P.PERSON_SID, 2147483646) PERSON_SID,
       nvl(G.ACAD_GRP_SID, 2147483646) ACAD_GRP_ADVIS_SID, 
       nvl(L.ACAD_LOAD_SID,2147483646) ACAD_LOAD_APPR_SID, 
       nvl(L1.ACAD_LOAD_SID,2147483646) ACAD_LOAD_SID, 
       nvl(PA.ACAD_LVL_SID,2147483646) STRT_ACAD_LVL_SID, 
       nvl(PA1.ACAD_LVL_SID,2147483646) END_ACAD_LVL_SID, 
       nvl(PA2.ACAD_LVL_SID,2147483646) PRJTD_ACAD_LVL_SID, 
       nvl(AG.ACAD_PROG_SID,2147483646) PRI_ACAD_PROG_SID, 
       nvl(ACS.ACAD_STNDNG_SID,2147483646) ACAD_STNDNG_SID, 
       nvl(C1.ACAD_CAR_SID,2147483646) BILL_CAR_SID, 
       nvl(L2.ACAD_LOAD_SID,2147483646) FA_LOAD_SID, 
       ACAD_CAREER_FIRST ACAD_CAR_FIRST_FLG,   
       ACADEMIC_LOAD_DT ACAD_LOAD_DT, 
       ACAD_YEAR ACAD_YR_SID, 
       CLASS_RANK_NBR CLASS_RANK_NUM,
       CLASS_RANK_TOT,
       COUNTRY,
       ELIG_TO_ENROLL ELIG_TO_ENROLL_FLG, 
       ENRL_ON_TRANS_DT ENRL_ON_TRN_DT, 
       EXT_ORG_ID,
       FA_ELIGIBILITY FA_ELIG_FLG,
       FA_STATS_CALC_REQ FA_STATS_CALC_REQ_FLG,
       FA_STATS_CALC_DTTM, 
       FORM_OF_STUDY,
       FULLY_ENRL_DT, 
       FULLY_GRADED_DT, 
       LAST_DATE_ATTENDED LAST_ATTND_DT, 
       LOCK_IN_AMT,
       LOCK_IN_DT, 
       MAX_CRSE_COUNT MAX_CRSE_CNT,
       NSLDS_LOAN_YEAR, 
       OVRD_ACAD_LVL_PROJ OVRD_ACAD_LVL_PROJ_FLG, 
       OVRD_ACAD_LVL_ALL OVRD_ACAD_LVL_ALL_FLG, 
       OVRD_BILL_UNITS OVRD_BILL_UNITS_FLG, 
       OVRD_INIT_ADD_FEE OVRD_INIT_ADD_FEE_FLG,
       OVRD_INIT_ENR_FEE OVRD_INIT_ENR_FEE_FLG,
       OVRD_MAX_UNITS OVRD_MAX_UNITS_FLG, 
       OVRD_TUIT_GROUP,
       OVRD_WDRW_SCHED,
       PROJ_BILL_UNT PRJTD_BILL_UNIT, 
       PRO_RATA_ELIGIBLE PRO_RATA_ELIG_FLG,
       REFUND_PCT,
       REFUND_SCHEME,
       REG_CARD_DATE REG_CARD_DT, 
       REGISTERED REG_FLG,
       RESET_CUM_STATS RESET_CUM_STATS_FLG,
       SEL_GROUP,
       SSR_ACTIVATION_DT SSR_ACTV_DT, 
       STATS_ON_TRANS_DT STATS_ON_TRN_DT, 
       STDNT_CAR_NBR STDNT_CAR_NUM, 
       STUDY_AGREEMENT,
	   D.TERM_BEGIN_DT,
	   D.TERM_END_DT,
       TERM_TYPE,
       TUIT_CALC_REQ TUIT_CALC_REQ_FLG, 
       TUIT_CALC_DTTM, 
       UNTPRG_CHG_NSLC_DT, 
       UNIT_MULTIPLIER,
       WITHDRAW_DATE WDN_DT, 
       WITHDRAW_CODE, 
       WITHDRAW_REASON, 
       CUR_GPA CUR_GPA_PTS,
       CUM_GPA CUM_GPA_PTS,
       GRADE_POINTS GRADE_PTS,
       GRADE_POINTS_FA GRADE_PTS_FA,
       UNT_AUDIT AUDIT_UNIT, 
       UNT_OTHER OTH_UNIT,      
       UNT_TERM_TOT TOT_TERM_UNIT,
       UNT_TRNSFR XFER_UNIT, 
       MIN_TOTAL_UNIT MIN_TOT_UNIT,
       MAX_TOTAL_UNIT MAX_TOT_UNIT, 
       MAX_NOGPA_UNIT MAX_NON_GPA_UNIT, 
       MAX_AUDIT_UNIT MAX_AUDIT_UNIT, 
       MAX_WAIT_UNIT MAX_WAIT_UNIT, 
       CUR_RESIDENT_TERMS,
       TRF_RESIDENT_TERMS,
       CUM_RESIDENT_TERMS,
       TUITION_RES_TERMS,
       SSR_TRF_CUR_GPA,
       SSR_COMB_CUR_GPA,
       SSR_CUM_EN_GPA,
       SSR_TOT_EN_GRDPTS,
       SSR_TOT_EN_TKNGPA,
       SSR_CUM_TR_GPA, 
       SSR_TOT_TR_GRDPTS,
       SSR_TOT_TR_TKNGPA,
       UNT_TAKEN_PRGRSS TAKEN_PRGRS_UNIT, 
       UNT_PASSD_PRGRSS PASSD_PRGRS_UNIT, 
       UNT_TAKEN_GPA TAKEN_GPA_UNIT, 
       UNT_PASSD_GPA PASSD_GPA_UNIT, 
       UNT_TAKEN_NOGPA TAKEN_NON_GPA_UNIT, 
       UNT_PASSD_NOGPA PASSD_NON_GPA_UNIT, 
       UNT_INPROG_GPA PRGRS_GPA_UNIT, 
       UNT_INPROG_NOGPA PRGRS_NON_GPA_UNIT, 
       TC_UNITS_ADJUST,
       TOT_TAKEN_PRGRSS,
       TOT_PASSD_PRGRSS,
       TOT_TAKEN_GPA,
       TOT_PASSD_GPA,
       TOT_TAKEN_NOGPA,
       TOT_PASSD_NOGPA,
       TOT_INPROG_GPA,
       TOT_INPROG_NOGPA,
       TOT_AUDIT,
       TOT_TRNSFR,
       TOT_TEST_CREDIT,
       TOT_OTHER,
       TOT_CUMULATIVE,
       TOT_GRADE_POINTS,
       TOT_TAKEN_FA,
       TOT_PASSD_FA,
       TOT_TAKEN_FA_GPA,
       TOT_GRD_POINTS_FA,
       TRF_TAKEN_GPA, 
       TRF_TAKEN_NOGPA, 
       TRF_PASSED_GPA, 
       TRF_PASSED_NOGPA, 
       TRF_GRADE_POINTS, 
       UNT_TEST_CREDIT, 
       UNT_TAKEN_FA, 
       UNT_PASSD_FA, 
       UNT_TAKEN_FA_GPA,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID	   
  FROM CSSTG_OWNER.PS_STDNT_CAR_TERM F    -- NK -> EMPLID, ACAD_CAREER, INSTITUTION, STRM, SRC_SYS_ID 
  left outer join STND
    on F.EMPLID = STND.EMPLID
   and F.ACAD_CAREER = STND.ACAD_CAREER
   and F.INSTITUTION = STND.INSTITUTION
   and F.STRM = STND.STRM
   and F.SRC_SYS_ID = STND.SRC_SYS_ID
   and STND.STND_ORDER = 1
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
   on F.INSTITUTION = I.INSTITUTION_CD
   and F.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on F.ACAD_CAREER = C.ACAD_CAR_CD 
   and F.INSTITUTION = C.INSTITUTION_CD	
   and F.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on F.EMPLID = P.PERSON_ID  
   and F.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_TERM D 	
    on D.INSTITUTION_CD = F.INSTITUTION
   and D.ACAD_CAR_CD = F.ACAD_CAREER
   and D.TERM_CD = F.STRM
   and D.SRC_SYS_ID = F.SRC_SYS_ID 
   and D.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_ACAD_GRP G
    on G.INSTITUTION_CD = F.INSTITUTION
   and G.ACAD_GRP_CD = F.ACAD_GROUP_ADVIS
   and G.SRC_SYS_ID = F.SRC_SYS_ID 
   and G.EFFDT_ORDER = 1
   and G.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_ACAD_LOAD L
    on F.ACAD_LOAD_APPR = L.ACAD_LOAD_CD
   and F.SRC_SYS_ID = L.SRC_SYS_ID 
   and L.APPRVD_IND = 'N'
   and L.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_ACAD_LOAD L1
    on F.ACADEMIC_LOAD = L1.ACAD_LOAD_CD
   and F.SRC_SYS_ID = L1.SRC_SYS_ID 
   and L1.APPRVD_IND = 'N'
   and L1.DATA_ORIGIN <> 'D' 
 left outer join CSMRT_OWNER.PS_D_ACAD_LVL PA
    on F.ACAD_LEVEL_BOT = PA.ACAD_LVL_CD
   and F.SRC_SYS_ID = PA.SRC_SYS_ID 
   and PA.DATA_ORIGIN <> 'D' 
    left outer join CSMRT_OWNER.PS_D_ACAD_LVL PA1
    on F.ACAD_LEVEL_EOT = PA1.ACAD_LVL_CD
   and F.SRC_SYS_ID = PA1.SRC_SYS_ID 
   and PA1.DATA_ORIGIN <> 'D' 
   left outer join CSMRT_OWNER.PS_D_ACAD_LVL PA2
    on F.ACAD_LEVEL_PROJ = PA2.ACAD_LVL_CD
   and F.SRC_SYS_ID = PA2.SRC_SYS_ID 
   and PA2.DATA_ORIGIN <> 'D' 
   left outer join CSMRT_OWNER.UM_D_ACAD_PROG AG
    on AG.INSTITUTION_CD = F.INSTITUTION
   and AG.ACAD_PROG_CD = F.ACAD_PROG_PRIMARY
   and AG.SRC_SYS_ID = F.SRC_SYS_ID 
   and AG.EFFDT_ORDER=1
   and AG.DATA_ORIGIN <> 'D' 
     left outer join CSMRT_OWNER.PS_D_ACAD_STNDNG ACS
    on ACS.INSTITUTION_CD = F.INSTITUTION
   and ACS.ACAD_CAR_CD = F.ACAD_CAREER
   and ACS.SRC_SYS_ID = F.SRC_SYS_ID 
   and nvl(STND.ACAD_STNDNG_ACTN,'-')= ACS.ACAD_STNDNG_ACN_CD
   and ACS.DATA_ORIGIN <> 'D'
     LEFT OUTER JOIN CSMRT_OWNER.PS_D_ACAD_CAR C1
    on F.BILLING_CAREER = C1.ACAD_CAR_CD 
   and F.INSTITUTION = C1.INSTITUTION_CD	
   and F.SRC_SYS_ID = C1.SRC_SYS_ID
   and C1.DATA_ORIGIN <> 'D'
    left outer join CSMRT_OWNER.PS_D_ACAD_LOAD L2
    on F.FA_LOAD = L2.ACAD_LOAD_CD
   and F.SRC_SYS_ID = L2.SRC_SYS_ID 
   and L2.APPRVD_IND = 'N'
   and L2.DATA_ORIGIN <> 'D' 
  WHERE F.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_F_TERM_ENRLMT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_TERM_ENRLMT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );
strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_F_TERM_ENRLMT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_TERM_ENRLMT enable constraint PK_PS_F_TERM_ENRLMT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_F_TERM_ENRLMT');

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

END PS_F_TERM_ENRLMT_P;
/
