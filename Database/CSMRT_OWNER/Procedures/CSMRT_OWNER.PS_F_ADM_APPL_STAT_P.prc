CREATE OR REPLACE PROCEDURE             "PS_F_ADM_APPL_STAT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table PS_F_ADM_APPL_STAT.
--
 --V01  SMT-xxxx 07/11/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_F_ADM_APPL_STAT';
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

strMessage01    := 'Truncating table CSMRT_OWNER.PS_F_ADM_APPL_STAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_F_ADM_APPL_STAT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_F_ADM_APPL_STAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_F_ADM_APPL_STAT');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_ADM_APPL_STAT disable constraint PK_PS_F_ADM_APPL_STAT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_F_ADM_APPL_STAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_F_ADM_APPL_STAT';				
insert /*+ append parallel(16) enable_parallel_dml */ into CSMRT_OWNER.PS_F_ADM_APPL_STAT 
  with AUD as (
select /*+ parallel(16) inline */ 
       EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR, EFFDT, EFFSEQ, SRC_SYS_ID,
       AUDIT_OPRID, AUDIT_STAMP, AUDIT_ACTN,
       row_number() over (partition by EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR, APPL_PROG_NBR, EFFDT, EFFSEQ, SRC_SYS_ID
                              order by AUDIT_STAMP, AUDIT_ACTN) AUD_ORDER
  from CSSTG_OWNER.PS_AUDIT_APPROG_UM
 where DATA_ORIGIN <> 'D'), 
  S as (
SELECT /*+ parallel(16) inline */
       A.EMPLID PERSON_ID, A.ACAD_CAREER ACAD_CAR_CD, A.STDNT_CAR_NBR STU_CAR_NBR, A.ADM_APPL_NBR, A.APPL_PROG_NBR, A.EFFDT, A.EFFSEQ, A.SRC_SYS_ID,
       A.INSTITUTION INSTITUTION_CD, A.ACAD_PROG ACAD_PROG_CD, A.PROG_STATUS, A.PROG_ACTION, A.ACTION_DT ACTION_DT, A.PROG_REASON, A.ADMIT_TERM, A.EXP_GRAD_TERM,
       A.ACAD_LOAD_APPR ACAD_LOAD_CD, A.CAMPUS, A.ACAD_PROG_DUAL, A.JOINT_PROG_APPR JOINT_PROG_FLG, AUD.AUDIT_OPRID, 
       substr(trim(AUD.AUDIT_OPRID),-8,8) AUDIT_OPRID_LKP,   
       to_date(substr(AUD.AUDIT_STAMP,1,19),'YYYY-MM-DD HH24:MI:SS') AUDIT_DT, AUD.AUDIT_ACTN AUDIT_ACTION,
       B.ADM_APPL_CTR, B.ADMIT_TYPE, B.LAST_SCH_ATTEND, B.ADM_APPL_COMPLETE APPL_CMPLTN_FLG, B.ADM_APPL_DT APPL_DT, trunc(B.ADM_APPL_CMPLT_DT) APPL_CMPLTN_DT,
       B.GRADUATION_DT LST_SCHL_GRDDT, B.ADM_APPL_METHOD, B.ACADEMIC_LEVEL ACAD_LVL_CD, B.EXT_ADM_APPL_NBR, B.UM_RA_TA_INTEREST, B.NOTIFICATION_PLAN,
       B.FIN_AID_INTEREST, B.HOUSING_INTEREST, B.ADM_CREATION_DT APPL_CREATE_DT, B.ADM_CREATION_BY APPL_CREATION_BY, B.ADM_UPDATED_DT APPL_UPDATE_DT, B.ADM_UPDATED_BY APPL_UPDATED_BY,
       B.APPL_FEE_AMT, B.APPL_FEE_DT, B.APPL_FEE_PAID, B.APPL_FEE_STATUS, B.APPL_FEE_TYPE, B.APP_FEE_STATUS, B.WAIVE_AMT,
       B2.UM_ACAD_PROG1, B2.UM_ACAD_PLAN1, B2.UM_ACAD_SUB_PLAN1, B2.UM_MANUAL_COMPLETE APPL_MANUAL_CMPLTN_FLG, trunc(B2.UM_COMPLETED_DT) APPL_MANUAL_CMPLTN_DT, B2.UM_CA_FIRST_GEN, B2.UM_CA_TESTING_PLAN, 
       C.ACAD_PLAN ACAD_PLAN_CD, C.DECLARE_DT PLAN_DECLARE_DT, 
       D.ACAD_SUB_PLAN ACAD_SPLAN_CD, D.DECLARE_DT SPLAN_DECLARE_DT, 
       E.UM_BHE, E.UM_BHE_ENG, E.UM_BHE_SOCSCI, E.UM_BHE_SCI, E.UM_BHE_MATH, E.UM_BHE_ELT, E.UM_BHE_FRLG, E.UM_BHE_CMPLT,
       E.UM_BHE_EXP_VOCTEC, E.UM_BHE_EXP_ESL, E.UM_BHE_EXP_INTL, E.UM_BHE_PRECOLLEGE, E.UM_BHE_EXP_LD, E.UM_BHE_TRANS_CR, E.UM_BHE_TRANS_GPA,
       F.STDNT_CAR_NBR_SR STU_CAR_NBR_SR, F.EVALUATN_STATUS, F.EVALUATION_DT EVALUATN_DT, G.UM_TCA_COMPLETE, G.UM_TCA_CREDITS
  FROM CSSTG_OWNER.PS_ADM_APPL_PROG A 
  LEFT OUTER JOIN AUD
    ON A.EMPLID = AUD.EMPLID
   AND A.ACAD_CAREER = AUD.ACAD_CAREER
   AND A.STDNT_CAR_NBR = AUD.STDNT_CAR_NBR
   AND A.ADM_APPL_NBR = AUD.ADM_APPL_NBR
   AND A.APPL_PROG_NBR = AUD.APPL_PROG_NBR
   AND A.EFFDT = AUD.EFFDT
   AND A.EFFSEQ = AUD.EFFSEQ
   AND A.SRC_SYS_ID = AUD.SRC_SYS_ID
   AND AUD.AUD_ORDER = 1
  LEFT OUTER JOIN CSSTG_OWNER.PS_ADM_APPL_DATA B 
    ON A.EMPLID = B.EMPLID
   AND A.ACAD_CAREER = B.ACAD_CAREER
   AND A.STDNT_CAR_NBR = B.STDNT_CAR_NBR
   AND A.ADM_APPL_NBR = B.ADM_APPL_NBR
   AND A.SRC_SYS_ID = B.SRC_SYS_ID
   AND B.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_UM_ADM_APPLDATA B2 
    ON A.EMPLID = B2.EMPLID
   AND A.ACAD_CAREER = B2.ACAD_CAREER
   AND A.STDNT_CAR_NBR = B2.STDNT_CAR_NBR
   AND A.ADM_APPL_NBR = B2.ADM_APPL_NBR
   AND A.SRC_SYS_ID = B2.SRC_SYS_ID
   AND B2.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_ADM_APPL_PLAN C 
    ON A.EMPLID = C.EMPLID
   AND A.ACAD_CAREER = C.ACAD_CAREER
   AND A.STDNT_CAR_NBR = C.STDNT_CAR_NBR
   AND A.ADM_APPL_NBR = C.ADM_APPL_NBR
   AND A.APPL_PROG_NBR = C.APPL_PROG_NBR
   AND A.EFFDT = C.EFFDT
   AND A.EFFSEQ =  C.EFFSEQ
   AND A.SRC_SYS_ID = C.SRC_SYS_ID
   AND C.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_ADM_APPL_SBPLAN D 
    ON C.EMPLID = D.EMPLID
   AND C.ACAD_CAREER = D.ACAD_CAREER
   AND C.STDNT_CAR_NBR = D.STDNT_CAR_NBR
   AND C.ADM_APPL_NBR = D.ADM_APPL_NBR
   AND C.APPL_PROG_NBR = D.APPL_PROG_NBR
   AND C.EFFDT= D.EFFDT
   AND C.EFFSEQ = D.EFFSEQ
   AND C.ACAD_PLAN = D.ACAD_PLAN
   AND C.SRC_SYS_ID = D.SRC_SYS_ID
   AND D.ACAD_SUB_PLAN <> '-'
   AND D.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_UM_ADM_BHE E  
    ON A.EMPLID = E.EMPLID
   AND A.ACAD_CAREER = E.ACAD_CAREER
   AND A.STDNT_CAR_NBR = E.STDNT_CAR_NBR
   AND A.ADM_APPL_NBR = E.ADM_APPL_NBR
   AND A.APPL_PROG_NBR = E.APPL_PROG_NBR
   AND A.SRC_SYS_ID = E.SRC_SYS_ID
   AND E.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_ADM_APP_CAR_SEQ F 
    ON A.EMPLID = F.EMPLID
   AND A.ACAD_CAREER = F.ACAD_CAREER
   AND A.STDNT_CAR_NBR = F.STDNT_CAR_NBR
   AND A.ADM_APPL_NBR = F.ADM_APPL_NBR
   AND A.APPL_PROG_NBR = F.APPL_PROG_NBR
   AND A.SRC_SYS_ID = F.SRC_SYS_ID
   AND F.CREATE_PROG_STATUS = 'S'
   AND F.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSSTG_OWNER.PS_UM_TCA_REVIEW G  
    ON A.EMPLID = G.EMPLID
   AND A.ACAD_CAREER = G.ACAD_CAREER
   AND A.STDNT_CAR_NBR = G.STDNT_CAR_NBR
   AND A.ADM_APPL_NBR = G.ADM_APPL_NBR
   AND A.APPL_PROG_NBR = G.APPL_PROG_NBR
   AND A.SRC_SYS_ID = G.SRC_SYS_ID
   AND G.DATA_ORIGIN <> 'D'
 WHERE A.DATA_ORIGIN <> 'D'
)
select /*+ parallel(16) */
       S.ACAD_CAR_CD, S.STU_CAR_NBR, S.PERSON_ID, S.ADM_APPL_NBR, S.APPL_PROG_NBR, nvl(S.ACAD_PLAN_CD,'-') ACAD_PLAN_CD, nvl(S.ACAD_SPLAN_CD,'-') ACAD_SPLAN_CD, S.EFFDT, S.EFFSEQ, S.SRC_SYS_ID, 
       S.INSTITUTION_CD, S.ACAD_PROG_CD, 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(L.ACAD_LOAD_SID, 2147483646) ACAD_LOAD_SID, 
       nvl(V.ACAD_LVL_SID, 2147483646) ACAD_LVL_SID, 
       nvl(G.ACAD_PROG_SID, 2147483646) ACAD_PROG_SID, 
       nvl(PL.ACAD_PLAN_SID, 2147483646) ACAD_PLAN_SID, 
       nvl(SP.ACAD_SPLAN_SID, 2147483646) ACAD_SPLAN_SID, 
       nvl(G2.ACAD_PROG_SID, 2147483646) ACAD_PROG2_SID, 
       nvl(PL2.ACAD_PLAN_SID, 2147483646) ACAD_PLAN2_SID, 
       nvl(SP2.ACAD_SPLAN_SID, 2147483646) ACAD_SPLAN2_SID, 
       nvl(T.TERM_SID,2147483646) ADMIT_TERM_SID, 
       nvl(Y.ADMIT_TYPE_SID,2147483646) ADMIT_TYPE_SID, 
       nvl(P.PERSON_SID,2147483646) APPLCNT_SID, 
       nvl(AC.APPL_CNTR_SID,2147483646) APPL_CNTR_SID, 
       nvl(AM.APPL_MTHD_SID,2147483646) APPL_MTHD_SID, 
       nvl(OP.PERSON_SID,2147483646) AUDIT_OPRID_SID, 
       nvl(CM.CAMPUS_SID,2147483646) CAMPUS_SID, 
       nvl(DG.ACAD_PROG_SID, 2147483646) DUAL_ACAD_PROG_SID, 
       nvl(EV.EVAL_STAT_SID, 2147483646) EVAL_STAT_SID, 
       nvl(GT.TERM_SID, 2147483646) EXP_GRAD_TERM_SID, 
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
       nvl(E.EXT_ORG_SID, 2147483646) LST_SCHL_ATTND_SID, 
       nvl(PA.PROG_ACN_SID, 2147483646) PROG_ACN_SID, 
       nvl(AR.PROG_ACN_RSN_SID, 2147483646) PROG_ACN_RSN_SID, 
       nvl(PS.PROG_STAT_SID, 2147483646) PROG_STAT_SID, 
       ACTION_DT, 
       nvl(APPL_CMPLTN_DT,to_date('01-JAN-1900')) APPL_CMPLTN_DT, 
       nvl(APPL_CMPLTN_FLG,'-') APPL_CMPLTN_FLG, 
       APPL_CREATE_DT, 
       nvl(APPL_CREATION_BY,'-') APPL_CREATION_BY, 
       APPL_DT, 
       nvl(APPL_FEE_AMT,0) APPL_FEE_AMT, 
       APPL_FEE_DT, 
       nvl(APPL_FEE_PAID,0) APPL_FEE_PAID, 
       nvl(APPL_FEE_STATUS,'-') APPL_FEE_STATUS, 
       nvl(APPL_FEE_TYPE,'-') APPL_FEE_TYPE, 
       nvl(APP_FEE_STATUS,'-') APP_FEE_STATUS, 
       APPL_MANUAL_CMPLTN_DT, 
       nvl(APPL_MANUAL_CMPLTN_FLG,'-') APPL_MANUAL_CMPLTN_FLG, 
       APPL_UPDATE_DT, 
       nvl(APPL_UPDATED_BY,'-') APPL_UPDATED_BY, 
       nvl(AUDIT_ACTION,'-') AUDIT_ACTION, 
       AUDIT_DT, 
       nvl(AUDIT_OPRID,'-') AUDIT_OPRID, 
       nvl(EVALUATN_DT,to_date('01-JAN-1900')) EVALUATN_DT, 
       nvl(EXT_ADM_APPL_NBR,'-') EXT_ADM_APPL_NBR, 
       nvl(FIN_AID_INTEREST,'-') FIN_AID_INTEREST, 
       nvl(JOINT_PROG_FLG,'-') JOINT_PROG_FLG, 
       nvl(HOUSING_INTEREST,'-') HOUSING_INTEREST, 
       nvl(LST_SCHL_GRDDT,to_date('01-JAN-1900')) LST_SCHL_GRDDT, 
       nvl(NOTIFICATION_PLAN,'-') NOTIFICATION_PLAN, 
       PLAN_DECLARE_DT, 
       SPLAN_DECLARE_DT, 
       STU_CAR_NBR_SR, 
       nvl(UM_BHE,'-') UM_BHE, 
       nvl(UM_BHE_ENG,'-') UM_BHE_ENG, 
       nvl(UM_BHE_SOCSCI,'-') UM_BHE_SOCSCI, 
       nvl(UM_BHE_SCI,'-') UM_BHE_SCI, 
       nvl(UM_BHE_MATH,'-') UM_BHE_MATH, 
       nvl(UM_BHE_ELT,'-') UM_BHE_ELT, 
       nvl(UM_BHE_FRLG,'-') UM_BHE_FRLG, 
       nvl(UM_BHE_CMPLT,'-') UM_BHE_CMPLT, 
       nvl(UM_BHE_EXP_VOCTEC,'-') UM_BHE_EXP_VOCTEC, 
       nvl(UM_BHE_EXP_ESL,'-') UM_BHE_EXP_ESL, 
       nvl(UM_BHE_EXP_INTL,'-') UM_BHE_EXP_INTL, 
       nvl(UM_BHE_PRECOLLEGE,'-') UM_BHE_PRECOLLEGE, 
       nvl(UM_BHE_EXP_LD,'-') UM_BHE_EXP_LD, 
       nvl(UM_BHE_TRANS_CR,0) UM_BHE_TRANS_CR, 
       nvl(UM_BHE_TRANS_GPA,0) UM_BHE_TRANS_GPA, 
       nvl(UM_CA_FIRST_GEN,'-') UM_CA_FIRST_GEN, 
       nvl(UM_CA_TESTING_PLAN,'-') UM_CA_TESTING_PLAN, 
       nvl(UM_RA_TA_INTEREST,'-') UM_RA_TA_INTEREST, 
       nvl(UM_TCA_COMPLETE,'-') UM_TCA_COMPLETE, 
       nvl(UM_TCA_CREDITS,0) UM_TCA_CREDITS, 
       nvl(WAIVE_AMT,0) WAIVE_AMT, 
       'N' LOAD_ERROR, 'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM, 1234 BATCH_SID
  from S
  left outer join PS_D_ACAD_CAR C
    on S.INSTITUTION_CD = C.INSTITUTION_CD
   and S.ACAD_CAR_CD = C.ACAD_CAR_CD 
   and S.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_LOAD L
    on L.APPRVD_IND = 'Y' 
   and S.ACAD_LOAD_CD = L.ACAD_LOAD_CD
   and S.SRC_SYS_ID = L.SRC_SYS_ID
   and L.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_LVL V
    on S.ACAD_LVL_CD = V.ACAD_LVL_CD
   and S.SRC_SYS_ID = V.SRC_SYS_ID
   and V.DATA_ORIGIN <> 'D'
  left outer join UM_D_ACAD_PROG G
    on S.INSTITUTION_CD = G.INSTITUTION_CD
   and S.ACAD_PROG_CD = G.ACAD_PROG_CD
   and S.SRC_SYS_ID = G.SRC_SYS_ID
   and G.EFFDT_ORDER = 1
   and G.DATA_ORIGIN <> 'D'
  left outer join UM_D_ACAD_PLAN PL
    on S.INSTITUTION_CD = PL.INSTITUTION_CD
   and S.ACAD_PLAN_CD = PL.ACAD_PLAN_CD
   and S.SRC_SYS_ID = PL.SRC_SYS_ID
   and PL.EFFDT_ORDER = 1
   and PL.DATA_ORIGIN <> 'D'
  left outer join UM_D_ACAD_SPLAN SP
    on S.INSTITUTION_CD = SP.INSTITUTION_CD
   and S.ACAD_PLAN_CD = SP.ACAD_PLAN_CD
   and S.ACAD_SPLAN_CD = SP.ACAD_SPLAN_CD
   and S.SRC_SYS_ID = SP.SRC_SYS_ID
   and SP.EFFDT_ORDER = 1
   and SP.DATA_ORIGIN <> 'D'
  left outer join UM_D_ACAD_PROG G2
    on S.INSTITUTION_CD = G2.INSTITUTION_CD
   and S.UM_ACAD_PROG1 = G2.ACAD_PROG_CD
   and S.SRC_SYS_ID = G2.SRC_SYS_ID
   and G2.EFFDT_ORDER = 1
   and G2.DATA_ORIGIN <> 'D'
  left outer join UM_D_ACAD_PLAN PL2
    on S.INSTITUTION_CD = PL2.INSTITUTION_CD
   and S.UM_ACAD_PLAN1 = PL2.ACAD_PLAN_CD
   and S.SRC_SYS_ID = PL2.SRC_SYS_ID
   and PL2.EFFDT_ORDER = 1
   and PL2.DATA_ORIGIN <> 'D'
  left outer join UM_D_ACAD_SPLAN SP2
    on S.INSTITUTION_CD = SP2.INSTITUTION_CD
   and S.UM_ACAD_PLAN1 = SP2.ACAD_PLAN_CD
   and S.UM_ACAD_SUB_PLAN1 = SP2.ACAD_SPLAN_CD
   and S.SRC_SYS_ID = SP2.SRC_SYS_ID
   and SP2.EFFDT_ORDER = 1
   and SP2.DATA_ORIGIN <> 'D'
  left outer join PS_D_TERM T
    on S.INSTITUTION_CD = T.INSTITUTION_CD
   and S.ACAD_CAR_CD = T.ACAD_CAR_CD 
   and S.ADMIT_TERM = T.TERM_CD 
   and S.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join PS_D_ADMIT_TYPE Y
    on S.INSTITUTION_CD = Y.INSTITUTION_CD      -- Changing to INSTITUTION_CD with PS_D_ADMIT_TYPE_NEW!!! 
   and S.ADMIT_TYPE = Y.ADMIT_TYPE_ID 
   and S.SRC_SYS_ID = Y.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
  left outer join PS_D_PERSON P
    on S.PERSON_ID = P.PERSON_ID
   and S.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
  left outer join PS_D_APPL_CNTR AC
    on S.INSTITUTION_CD = AC.INSTITUTION_CD     -- Changing to INSTITUTION_CD with PS_D_APPL_CNTR_NEW!!! 
   and S.ADM_APPL_CTR = AC.APPL_CNTR_ID 
   and S.SRC_SYS_ID = AC.SRC_SYS_ID
   and AC.DATA_ORIGIN <> 'D'
  left outer join PS_D_APPL_MTHD AM
    on S.ADM_APPL_METHOD = AM.APPL_MTHD_ID
   and S.SRC_SYS_ID = AM.SRC_SYS_ID
   and AM.DATA_ORIGIN <> 'D'
  left outer join PS_D_PERSON OP
    on S.AUDIT_OPRID_LKP = OP.PERSON_ID
   and S.SRC_SYS_ID = OP.SRC_SYS_ID
   and OP.DATA_ORIGIN <> 'D'
  left outer join PS_D_CAMPUS CM
    on S.INSTITUTION_CD = CM.INSTITUTION_CD  
   and S.CAMPUS = CM.CAMPUS_CD 
   and S.SRC_SYS_ID = CM.SRC_SYS_ID
   and CM.DATA_ORIGIN <> 'D'
  left outer join UM_D_ACAD_PROG DG
    on S.INSTITUTION_CD = DG.INSTITUTION_CD
   and S.ACAD_PROG_DUAL = DG.ACAD_PROG_CD
   and S.SRC_SYS_ID = DG.SRC_SYS_ID
   and DG.EFFDT_ORDER = 1
   and DG.DATA_ORIGIN <> 'D'
  left outer join PS_D_EVAL_STATUS EV
    on S.INSTITUTION_CD = EV.INSTITUTION_CD  
   and S.EVALUATN_STATUS = EV.EVAL_STATUS_CD 
   and S.SRC_SYS_ID = EV.SRC_SYS_ID
   and EV.DATA_ORIGIN <> 'D'
  left outer join PS_D_TERM GT
    on S.INSTITUTION_CD = GT.INSTITUTION_CD
   and S.ACAD_CAR_CD = GT.ACAD_CAR_CD 
   and S.EXP_GRAD_TERM = GT.TERM_CD 
   and S.SRC_SYS_ID = GT.SRC_SYS_ID
   and GT.DATA_ORIGIN <> 'D'
  left outer join PS_D_INSTITUTION I
    on S.INSTITUTION_CD = I.INSTITUTION_CD  
   and S.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_EXT_ORG E
    on S.LAST_SCH_ATTEND = E.EXT_ORG_ID  
   and S.SRC_SYS_ID = E.SRC_SYS_ID
   and E.DATA_ORIGIN <> 'D'
  left outer join PS_D_PROG_ACN PA
    on S.INSTITUTION_CD = PA.SETID                -- SETID added to key for PS_D_PROG_ACN_NEW!!!!!!!!!!!!!!  
   and S.PROG_ACTION = PA.PROG_ACN_CD  
   and S.SRC_SYS_ID = PA.SRC_SYS_ID
   and PA.DATA_ORIGIN <> 'D'
  left outer join PS_D_PROG_ACN_RSN AR
    on S.INSTITUTION_CD = AR.SETID   
   and S.PROG_ACTION = AR.PROG_ACN_CD
   and S.PROG_REASON = AR.PROG_ACN_RSN_CD  
   and S.SRC_SYS_ID = AR.SRC_SYS_ID
   and AR.DATA_ORIGIN <> 'D'
  left outer join PS_D_PROG_STAT PS
    on S.PROG_STATUS = PS.PROG_STAT_CD  
   and S.SRC_SYS_ID = PS.SRC_SYS_ID
   and PS.DATA_ORIGIN <> 'D'
-- where not (nvl(G.ACAD_PROG_SID, 2147483646)   = 2147483646 and nvl(S.ACAD_PROG_CD,'-')  <> '-')    -- Sept 2019 
--   and not (nvl(PL.ACAD_PLAN_SID, 2147483646)  = 2147483646 and nvl(S.ACAD_PLAN_CD,'-')  <> '-')
--   and not (nvl(SP.ACAD_SPLAN_SID, 2147483646) = 2147483646 and nvl(S.ACAD_SPLAN_CD,'-') <> '-')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_F_ADM_APPL_STAT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_ADM_APPL_STAT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_F_ADM_APPL_STAT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_F_ADM_APPL_STAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_F_ADM_APPL_STAT enable constraint PK_PS_F_ADM_APPL_STAT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_F_ADM_APPL_STAT');

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

END PS_F_ADM_APPL_STAT_P;
/
