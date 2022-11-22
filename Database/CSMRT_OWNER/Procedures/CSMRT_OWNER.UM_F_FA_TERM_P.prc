DROP PROCEDURE CSMRT_OWNER.UM_F_FA_TERM_P
/

--
-- UM_F_FA_TERM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_TERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_TERM.
--
--V01   SMT-xxxx 08/02/2018,,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_TERM';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_TERM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_TERM');

----alter table UM_F_FA_TERM disable constraint PK_UM_F_FA_TERM;
--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_TERM disable constraint PK_UM_F_FA_TERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_TERM';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_TERM
  with X as (
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
FATRM_KEY AS (
SELECT /*+ PARALLEL(8) INLINE */
       EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, STRM TERM_CD, SRC_SYS_ID, EFFDT, EFFSEQ, EFF_STATUS, AID_YEAR,
       ROW_NUMBER() OVER (PARTITION BY EMPLID, INSTITUTION, STRM ORDER BY EFFDT DESC, EFFSEQ DESC) ROW_ORDER
  FROM CSSTG_OWNER.PS_STDNT_FA_TERM A
 WHERE DATA_ORIGIN <> 'D'),
  COMP as (
select /*+ PARALLEL(8) INLINE */
       EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, AID_YEAR, SRC_SYS_ID, EFFDT, EFFSEQ, TABLE_ID,
       PRIMARY_EFC,
       row_number() over (partition by EMPLID, INSTITUTION, AID_YEAR, SRC_SYS_ID
                              order by TABLE_ID, EFFDT desc, EFFSEQ desc) COMP_ORDER
  from CSSTG_OWNER.PS_ISIR_COMPUTED
 where DATA_ORIGIN <> 'D')
--TEMP as (                   -- Temp!!! 2,720,752
SELECT  /*+ PARALLEL(8) INLINE */
	STAID.INSTITUTION_CD,
	nvl(FATRM.ACAD_CAREER,'-') ACAD_CAR_CD,
	STAID.AID_YEAR,
	nvl(FATRM.STRM,'-') TERM_CD,
	STAID.PERSON_ID,
	STAID.SRC_SYS_ID,
	FATRM.EFFDT,
	FATRM.EFFSEQ,
	FATRM.EFF_STATUS,
	nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
	nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
	nvl(T.TERM_SID,2147483646) TERM_SID,
	nvl(P.PERSON_SID,2147483646) PERSON_SID,
	nvl(G.ACAD_PROG_SID,2147483646) ACAD_PROG_SID,
	nvl(PL.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID,
	nvl(SP.ACAD_SPLAN_SID, 2147483646) ACAD_SPLAN_SID,
	FATRM.ACAD_CAREER_CL,
	FATRM.ACAD_GROUP_ADVIS,
	FATRM.ACADEMIC_LOAD,
	X1.XLATLONGNAME ACAD_LOAD_LD,
	FATRM.ACADEMIC_LOAD_CL,
	FATRM.ACAD_LEVEL_BOT,
--	nvl(X2.XLATLONGNAME,'-')  ACAD_LEVEL_BOT_LD,      -- XLAT
	X2.XLATLONGNAME  ACAD_LEVEL_BOT_LD,      -- XLAT
	FATRM.ACAD_LEVEL_BOT_CL,
	X3.XLATLONGNAME ACAD_LEVEL_BOT_CL_LD,   -- XLAT
	FATRM.ACAD_LEVEL_EOT,
	X4.XLATLONGNAME ACAD_LEVEL_EOT_LD,      -- XLAT
	FATRM.ACAD_LEVEL_EOT_CL,
	X5.XLATLONGNAME ACAD_LEVEL_EOT_CL_LD,   -- XLAT
	FATRM.ACAD_LEVEL_PROJ,
	X6.XLATLONGNAME ACAD_LEVEL_PROJ_LD,     -- XLAT
	FATRM.ACAD_LEVEL_PROJ_CL,
	X7.XLATLONGNAME ACAD_LEVEL_PROJ_CL_LD,  -- XLAT
	FATRM.ACAD_LOAD_APPR,
	X8.XLATLONGNAME ACAD_LOAD_APPR_LD,      -- XLAT
	FATRM.ACAD_LOAD_APPR_CL,
	X9.XLATLONGNAME ACAD_LOAD_APPR_CL_LD,   -- XLAT
	FATRM.ACAD_PROG_PRIMARY,
	FATRM.ACAD_PROG_PRIM_CL,
	FATRM.ACAD_PLAN,
	FATRM.ACAD_PLAN_CL,
	FATRM.ACAD_STANDING,
	FATRM.ACAD_STANDING_CL,
	FATRM.ACAD_SUB_PLAN,
	FATRM.ACAD_SUB_PLAN_CL,
	FATRM.ACAD_YEAR,
	FATRM.ADMIT_TERM,
	FATRM.AID_YEAR_CL,
	FATRM.BILLING_CAREER,
	FATRM.BUDGET_REQUIRED,
	FATRM.CAMPUS,
	FATRM.CENSUS_DT,
	FATRM.COURSE_LD_PCT,
	FATRM.CUM_GPA,
	FATRM.CUM_GPA_CL,
	FATRM.CUM_RESIDENT_TERMS,
	FATRM.CUR_RESIDENT_TERMS,
	FATRM.DEGR_CONFER_DT,
	FATRM.DIR_LND_YR,
	X10.XLATLONGNAME DIR_LND_YR_LD,      -- XLAT
	FATRM.DIR_LND_YR_CL,
	X11.XLATLONGNAME DIR_LND_YR_CL_LD,   -- XLAT
	FATRM.ELIG_TO_ENROLL,
	FATRM.EXP_GRAD_DATE,
	FATRM.EXP_GRAD_DATE_CL,
	FATRM.EXP_GRAD_TERM,
	FATRM.EXP_GRAD_TERM_CL,
	FATRM.FA_EXP_DT_REBUILD,
	FATRM.FA_LOAD,
	X12.XLATLONGNAME FA_LOAD_LD,         -- XLAT
	FATRM.FA_LOAD_CL,
	X13.XLATLONGNAME FA_LOAD_CL_LD,      -- XLAT
	FATRM.FA_LOAD_CURRENT,
	FATRM.FA_NUMBER_OF_WEEKS,
	FATRM.FA_STANDING,
	FATRM.FA_STANDING_CL,
	FATRM.FA_STATS_CALC_REQ,
	FATRM.FA_UNIT_ANTIC,
	FATRM.FA_UNIT_COMPLETED,
	FATRM.FA_UNIT_CURRENT,
	FATRM.FA_UNIT_IN_PROG,
	FATRM.FORM_OF_STUDY,
	X14.XLATLONGNAME FORM_OF_STUDY_LD,       -- XLAT
	FATRM.FORM_OF_STUDY_CL,
	X15.XLATLONGNAME FORM_OF_STUDY_CL_LD,    -- XLAT
	FATRM.GPA_CALC_IND,
	FATRM.GPA_CL,
	FATRM.LAST_DATE_ATTENDED,
	FATRM.LS_GPA,
	FATRM.LOCK_OVRD_DATE,
	FATRM.LOCK_OVRD_OPRID,
	FATRM.NSLDS_LOAN_YEAR,
	X16.XLATLONGNAME NSLDS_LOAN_YEAR_LD,     -- XLAT
	FATRM.NSLDS_LOAN_YEAR_CL,
	X17.XLATLONGNAME NSLDS_LOAN_YEAR_CL_LD,  -- XLAT
	FATRM.OVRD_ACADEMIC_LOAD,
	FATRM.OVRD_ACAD_LOAD_AP,
	FATRM.OVRD_ACAD_LVL_ALL,
	FATRM.OVRD_ACAD_LVL_PROJ,
	FATRM.OVRD_ACAD_PROG_PRM,
	FATRM.OVRD_ACAD_PLAN,
	FATRM.OVRD_ACAD_STANDING,
	FATRM.OVRD_AID_YEAR,
	FATRM.OVRD_ASG_BOT,
	FATRM.OVRD_ASG_EOT,
	FATRM.OVRD_CAREER,
	FATRM.OVRD_CENSUSDT_LOCK,
	FATRM.OVRD_CUM_GPA,
	FATRM.OVRD_DIR_LND_YR,
	FATRM.OVRD_EXP_DT,
	FATRM.OVRD_FA_LOAD,
	FATRM.OVRD_FA_NBR_WEEKS,
	FATRM.OVRD_FA_STANDING,
	FATRM.OVRD_FA_UNITS,
	FATRM.OVRD_FORM_OF_STUDY,
	FATRM.OVRD_GPA,
	FATRM.OVRD_GRAD_DATE,
	FATRM.OVRD_GRAD_TERM,
	FATRM.OVRD_NSLDS_LOAN_YR,
	FATRM.OVRD_SUB_PLAN,
	FATRM.OVRD_SULA_UNIT,
	FATRM.OVRD_TOT_PASSD_FA,
	FATRM.OVRD_TOT_TAKEN_FA,
	FATRM.OVRD_UNT_PASSD_FA,
	FATRM.OVRD_UNT_TAKEN_FA,
	FATRM.OVRD_WOI,
	case when FATRM.ACAD_CAREER not in ('CENC','CSCE','UGRD') then NULL else PELL.PELL_AMT end PELL_ANNUAL_AMT,
	case when FATRM.ACAD_CAREER not in ('CENC','CSCE','UGRD') then NULL
		 when substr(FATRM.STRM,-2,2) in ('10','40','50') then round(PELL.PELL_AMT/2,0)
		 when substr(FATRM.STRM,-2,2) in ('30') then trunc(PELL.PELL_AMT/2,0) else NULL end PELL_TERM_AMT,
	FATRM.REFUND_CLASS,
	FATRM.REFUND_PCT,
	FATRM.REFUND_SCHEME,
	FATRM.REFUND_SETID,
	FATRM.REMOTE_UNT_FA,
	FATRM.RESET_CUM_STATS,
	FATRM.SEL_GROUP,
	FATRM.SFA_ASG_AC_LVL_BCL,
	X18.XLATLONGNAME SFA_ASG_AC_LVL_BCL_LD,  -- XLAT
	FATRM.SFA_ASG_AC_LVL_BOT,
	X19.XLATLONGNAME SFA_ASG_AC_LVL_BOT_LD,  -- XLAT
	FATRM.SFA_ASG_AC_LVL_ECL,
	X20.XLATLONGNAME SFA_ASG_AC_LVL_ECL_LD,  -- XLAT
	FATRM.SFA_ASG_AC_LVL_EOT,
	X21.XLATLONGNAME SFA_ASG_AC_LVL_EOT_LD,  -- XLAT
	FATRM.SFA_ASG_UNITS_BOT,
	FATRM.SFA_ASG_UNITS_EOT,
	FATRM.SFA_ASG_WI_MTH,
	FATRM.SFA_ASG_WI_MTH_OVR,
	FATRM.SFA_ASG_WI_MTH_CL,
	FATRM.SFA_ASG_WI_TRM,
	FATRM.SFA_ASG_WI_TRM_OVR,
	FATRM.SFA_ASG_WI_TRM_CL,
	FATRM.SFA_ASG_WI_TCR,
	FATRM.SFA_ASG_WI_TCR_OVR,
	FATRM.SFA_ASG_WI_TCR_CL,
	FATRM.SFA_ASG_WI_CUM_BOT,
	FATRM.SFA_ASG_WI_CUM_EOT,
	FATRM.SFA_ASG_WI_USED,
	FATRM.SFA_FA_NBR_WKS_CL,
	FATRM.SFA_SPEC_PROG_FLG,
	X22.XLATLONGNAME SFA_SPEC_PROG_FLG_LD,   -- XLAT
	FATRM.SFA_SULA_LOAD,
	FATRM.SFA_SULA_UNIT,
	FATRM.SFA_SULA_UNIT_CL,
	FATRM.SFA_WKS_OF_INST_CL,
	FATRM.STDNT_CAR_NBR,
	nvl(T.TERM_BEGIN_DT,trunc(SYSDATE)) TERM_BEGIN_DT,
	nvl(T.TERM_END_DT,trunc(SYSDATE)) TERM_END_DT,
	FATRM.TERM_SRC,
	X23.XLATSHORTNAME TERM_SRC_LD,    -- XLAT
	FATRM.TOT_PASSD_FA,
	FATRM.TOT_PASSD_FA_CL,
	FATRM.TOT_TAKEN_FA,
	FATRM.TOT_TAKEN_FA_CL,
	FATRM.TOT_TERM_UNT_FA,
	FATRM.TOT_TERM_UNT_FA_CL,
	FATRM.TRF_RESIDENT_TERMS,
	FATRM.TUIT_CALC_DTTM,
	FATRM.TUIT_CALC_REQ,
	FATRM.UNT_PASSD_FA,
	FATRM.UNT_PASSD_FA_CL,
	FATRM.UNT_TAKEN_FA,
	FATRM.UNT_TAKEN_FA_CL,
	FATRM.UNT_TAKEN_FA_NOPIT,
	FATRM.WEEKS_OF_INSTRUCT,
	FATRM.WITHDRAW_CODE,
	FATRM.WITHDRAW_DATE,
	FATRM.WITHDRAW_REASON,
	'N',
	'S',
	sysdate,
	sysdate,
	1234
	FROM CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR STAID
	LEFT OUTER JOIN FATRM_KEY
	  ON STAID.PERSON_ID = FATRM_KEY.PERSON_ID
	 AND STAID.INSTITUTION_CD = FATRM_KEY.INSTITUTION_CD
	 AND STAID.AID_YEAR = FATRM_KEY.AID_YEAR
	 AND FATRM_KEY.ROW_ORDER = 1
	 AND FATRM_KEY.EFF_STATUS = 'A'
	LEFT OUTER JOIN CSSTG_OWNER.PS_STDNT_FA_TERM FATRM
	  ON FATRM_KEY.PERSON_ID = FATRM.EMPLID
	 AND FATRM_KEY.INSTITUTION_CD = FATRM.INSTITUTION
	 AND FATRM_KEY.TERM_CD = FATRM.STRM
	 AND FATRM_KEY.EFFDT = FATRM.EFFDT
	 AND FATRM_KEY.EFFSEQ = FATRM.EFFSEQ
	 AND FATRM.DATA_ORIGIN <> 'D'
	left outer join COMP
	  on STAID.PERSON_ID = COMP.PERSON_ID
	 and STAID.INSTITUTION_CD = COMP.INSTITUTION_CD
	 and STAID.AID_YEAR = COMP.AID_YEAR
	 and STAID.SRC_SYS_ID = COMP.SRC_SYS_ID
	 and COMP.COMP_ORDER = 1
	left outer join CSSTG_OWNER.UM_PELL_PAY PELL
	  on STAID.AID_YEAR = PELL.AID_YEAR
	 and PELL.FA_LOAD = FATRM.FA_LOAD
	 and COMP.PRIMARY_EFC between PELL.EFC_MIN and PELL.EFC_MAX
	left outer join PS_D_INSTITUTION I
	  on STAID.INSTITUTION_CD = I.INSTITUTION_CD
	 and STAID.SRC_SYS_ID = I.SRC_SYS_ID
	 and I.DATA_ORIGIN <> 'D'
	left outer join PS_D_ACAD_CAR C
	  on STAID.INSTITUTION_CD = C.INSTITUTION_CD
	 and FATRM.ACAD_CAREER = C.ACAD_CAR_CD
	 and STAID.SRC_SYS_ID = C.SRC_SYS_ID
	 and C.DATA_ORIGIN <> 'D'
	left outer join PS_D_TERM T
	  on STAID.INSTITUTION_CD = T.INSTITUTION_CD
	 and FATRM.ACAD_CAREER = T.ACAD_CAR_CD
	 and FATRM.STRM = T.TERM_CD
	 and STAID.SRC_SYS_ID = T.SRC_SYS_ID
	 and T.DATA_ORIGIN <> 'D'
	left outer join PS_D_PERSON P
	  ON STAID.PERSON_ID = P.PERSON_ID
	 AND STAID.SRC_SYS_ID = P.SRC_SYS_ID
	left outer join UM_D_ACAD_PROG G
	  on FATRM.INSTITUTION = G.INSTITUTION_CD
	 and FATRM.ACAD_PROG_PRIMARY = G.ACAD_PROG_CD
	 and STAID.SRC_SYS_ID = G.SRC_SYS_ID
	 and G.EFFDT_ORDER = 1
	left outer join UM_D_ACAD_PLAN PL
	  on FATRM.INSTITUTION = PL.INSTITUTION_CD
	 and FATRM.ACAD_PLAN = PL.ACAD_PLAN_CD
	 and FATRM.SRC_SYS_ID = PL.SRC_SYS_ID
	 and PL.EFFDT_ORDER = 1
	left outer join UM_D_ACAD_SPLAN SP
	  on FATRM.INSTITUTION = SP.INSTITUTION_CD
	 and FATRM.ACAD_PLAN = SP.ACAD_PLAN_CD
	 and FATRM.ACAD_SUB_PLAN = SP.ACAD_SPLAN_CD
	 and FATRM.SRC_SYS_ID = SP.SRC_SYS_ID
	 and SP.EFFDT_ORDER = 1
	 and SP.DATA_ORIGIN <> 'D'
	left outer join X X1
	  on X1.FIELDNAME = 'ACADEMIC_LOAD'
	 and X1.FIELDVALUE = FATRM.ACADEMIC_LOAD
	 and X1.X_ORDER = 1
	left outer join X X2
	  on X2.FIELDNAME = 'ACADEMIC_LEVEL'
	 and X2.FIELDVALUE = FATRM.ACAD_LEVEL_BOT
	 and X2.X_ORDER = 1
	left outer join X X3
	  on X3.FIELDNAME = 'ACADEMIC_LEVEL'
	 and X3.FIELDVALUE = FATRM.ACAD_LEVEL_BOT_CL
	 and X3.X_ORDER = 1
	left outer join X X4
	  on X4.FIELDNAME = 'ACADEMIC_LEVEL'
	 and X4.FIELDVALUE = FATRM.ACAD_LEVEL_EOT
	 and X4.X_ORDER = 1
	left outer join X X5
	  on X5.FIELDNAME = 'ACADEMIC_LEVEL'
	 and X5.FIELDVALUE = FATRM.ACAD_LEVEL_EOT_CL
	 and X5.X_ORDER = 1
	left outer join X X6
	  on X6.FIELDNAME = 'ACAD_LEVEL_PROJ'
	 and X6.FIELDVALUE = FATRM.ACAD_LEVEL_PROJ
	 and X6.X_ORDER = 1
	left outer join X X7
	  on X7.FIELDNAME = 'ACADEMIC_LEVEL'
	 and X7.FIELDVALUE = FATRM.ACAD_LEVEL_PROJ_CL
	 and X7.X_ORDER = 1
	left outer join X X8
	  on X8.FIELDNAME = 'ACAD_LOAD_APPR'
	 and X8.FIELDVALUE = FATRM.ACAD_LOAD_APPR
	 and X8.X_ORDER = 1
	left outer join X X9
	  on X9.FIELDNAME = 'ACAD_LOAD_APPR_CL'
	 and X9.FIELDVALUE = FATRM.ACAD_LOAD_APPR_CL
	 and X9.X_ORDER = 1
	left outer join X X10
	  on X10.FIELDNAME = 'DIR_LND_YR'
	 and X10.FIELDVALUE = FATRM.DIR_LND_YR
	 and X10.X_ORDER = 1
	left outer join X X11
	  on X11.FIELDNAME = 'DIR_LND_YR_CL'
	 and X11.FIELDVALUE = FATRM.DIR_LND_YR_CL
	 and X11.X_ORDER = 1
	left outer join X X12
	  on X12.FIELDNAME = 'FA_LOAD'
	 and X12.FIELDVALUE = FATRM.FA_LOAD
	 and X12.X_ORDER = 1
	left outer join X X13
	  on X13.FIELDNAME = 'FA_LOAD_CL'
	 and X13.FIELDVALUE = FATRM.FA_LOAD_CL
	 and X13.X_ORDER = 1
	left outer join X X14
	  on X14.FIELDNAME = 'FORM_OF_STUDY'
	 and X14.FIELDVALUE = FATRM.FORM_OF_STUDY
	 and X14.X_ORDER = 1
	left outer join X X15
	  on X15.FIELDNAME = 'FORM_OF_STUDY_CL'
	 and X15.FIELDVALUE = FATRM.FORM_OF_STUDY_CL
	 and X15.X_ORDER = 1
	left outer join X X16
	  on X16.FIELDNAME = 'NSLDS_LOAN_YEAR'
	 and X16.FIELDVALUE = FATRM.NSLDS_LOAN_YEAR
	 and X16.X_ORDER = 1
	left outer join X X17
	  on X17.FIELDNAME = 'NSLDS_LOAN_YEAR_CL'
	 and X17.FIELDVALUE = FATRM.NSLDS_LOAN_YEAR_CL
	 and X17.X_ORDER = 1
	left outer join X X18
	  on X18.FIELDNAME = 'SFA_ASG_AC_LVL_BCL'
	 and X18.FIELDVALUE = FATRM.SFA_ASG_AC_LVL_BCL
	 and X18.X_ORDER = 1
	left outer join X X19
	  on X19.FIELDNAME = 'SFA_ASG_AC_LVL_BOT'
	 and X19.FIELDVALUE = FATRM.SFA_ASG_AC_LVL_BOT
	 and X19.X_ORDER = 1
	left outer join X X20
	  on X20.FIELDNAME = 'SFA_ASG_AC_LVL_ECL'
	 and X20.FIELDVALUE = FATRM.SFA_ASG_AC_LVL_ECL
	 and X20.X_ORDER = 1
	left outer join X X21
	  on X21.FIELDNAME = 'SFA_ASG_AC_LVL_EOT'
	 and X21.FIELDVALUE = FATRM.SFA_ASG_AC_LVL_EOT
	 and X21.X_ORDER = 1
	left outer join X X22
	  on X22.FIELDNAME = 'SFA_SPEC_PROG_FLG'
	 and X22.FIELDVALUE = FATRM.SFA_SPEC_PROG_FLG
     and X22.X_ORDER = 1
	left outer join X X23
	  on X23.FIELDNAME = 'TERM_SRC'
	 and X23.FIELDVALUE = FATRM.TERM_SRC
     and X23.X_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_TERM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_TERM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_TERM',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_TERM enable constraint PK_UM_F_FA_TERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_TERM');

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

END UM_F_FA_TERM_P;
/
