DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_BDGT_P
/

--
-- UM_F_FA_STDNT_BDGT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_BDGT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_STDNT_BDGT.
--
--V01   SMT-xxxx 08/07/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_BDGT';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_BDGT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_BDGT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_BDGT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_BDGT');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_BDGT disable constraint PK_UM_F_FA_STDNT_BDGT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_BDGT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_BDGT';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_BDGT
  with X as (
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
   BIT as (
SELECT /*+ PARALLEL(8) INLINE */ distinct
       EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EFFDT, EFFSEQ, SRC_SYS_ID
  FROM CSSTG_OWNER.PS_STDNT_BUDGET_IT
 WHERE DATA_ORIGIN <> 'D'),
TERM_BDGT_ITEM AS (
SELECT /*+ PARALLEL(8) INLINE */
       A.EMPLID PERSON_ID, A.INSTITUTION INSTITUTION_CD, A.AID_YEAR, A.ACAD_CAREER ACAD_CAR_CD, A.STRM TERM_CD, A.SRC_SYS_ID,
       A.EFFDT, A.EFFSEQ,
       A.ACAD_LEVEL_BOT, A.ACAD_LEVEL_PROJ, A.ACADEMIC_LOAD,
       A.ACAD_LOAD_APPR, A.ACAD_PROG_PRIMARY, A.ACAD_PLAN, A.ACAD_SUB_PLAN, A.ACAD_PLAN_TYPE,
       A.BUDGET_GROUP_CODE, A.DEPNDNCY_STAT, A.DESCRSHORT, A.FA_LOAD,
       A.FA_LOAD_CURRENT, A.FA_TERM_EFFDT, A.FA_TERM_EFFSEQ,
       A.FA_UNIT_ANTIC, A.FA_UNIT_COMPLETED, A.FA_UNIT_IN_PROG, A.FA_UNIT_CURRENT, A.FIN_AID_FED_RES, A.FIN_AID_ST_RES, A.FIN_AID_FED_EXCPT, A.FIN_AID_ST_EXCPT, A.FORM_OF_STUDY,
       A.HOUSING_TYPE, A.MARITAL_STAT, A.NSLDS_LOAN_YEAR,
       A.NUMBER_IN_FAMILY, A.OPRID, A.POSTAL, A.PRORATE_BUDGET, A.RESIDENCY, A.STATE_RESIDENCE, A.APP_STATE_RESIDENC,
       A.TERM_TYPE, A.FED_TERM_COA, A.INST_TERM_COA, A.PELL_TERM_COA, A.SFA_PELTRM_COA_LHT,
       C.FA_TERM_WEEKS,
       C.AWARD_PERIOD,
       RANK() OVER (PARTITION BY A.EMPLID, A.INSTITUTION, A.ACAD_CAREER, A.STRM, A.AID_YEAR ORDER BY A.EFFDT DESC, A.EFFSEQ DESC) BDGT_ORDER
  FROM CSSTG_OWNER.PS_STDNT_TERM_BDGT A,     -- NK -> EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EFFDT, EFFSEQ, SRC_SYS_ID
       BIT B,                                -- NK -> EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, EFFDT, EFFSEQ, BGT_ITEM_CATEGORY, SRC_SYS_ID
       CSSTG_OWNER.PS_STDNT_CR_TERM C        -- NK -> EMPLID, INSTITUTION, AID_YEAR, ACAD_CAREER, STRM, SRC_SYS_ID
 WHERE A.EMPLID = B.EMPLID
   AND A.INSTITUTION = B.INSTITUTION
   AND A.AID_YEAR = B.AID_YEAR
   AND A.ACAD_CAREER = B.ACAD_CAREER
   AND A.STRM = B.STRM
   AND A.EFFDT = B.EFFDT
   AND A.EFFSEQ = B.EFFSEQ
   AND A.SRC_SYS_ID = B.SRC_SYS_ID
   AND A.EMPLID = C.EMPLID
   AND A.INSTITUTION = C.INSTITUTION
   AND A.AID_YEAR = C.AID_YEAR
   AND A.ACAD_CAREER = C.ACAD_CAREER
   AND A.STRM = C.STRM
   AND A.SRC_SYS_ID = C.SRC_SYS_ID
   AND A.DATA_ORIGIN <> 'D'
   AND C.DATA_ORIGIN <> 'D'
)
SELECT /*+ PARALLEL(8) INLINE */
       STAID.INSTITUTION_CD,
       NVL(TERM_BDGT_ITEM.ACAD_CAR_CD, '-') ACAD_CAR_CD,
       STAID.AID_YEAR,
       NVL(TERM_BDGT_ITEM.TERM_CD, '-') TERM_CD,
       STAID.PERSON_ID,
       STAID.SRC_SYS_ID,
       TERM_BDGT_ITEM.EFFDT,
       TERM_BDGT_ITEM.EFFSEQ,
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
       nvl(T.TERM_SID,2147483646) TERM_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       nvl(G.ACAD_PROG_SID,2147483646) ACAD_PROG_SID,
	   nvl(PL.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID,
	   nvl(SP.ACAD_SPLAN_SID, 2147483646) ACAD_SPLAN_SID,
	   nvl(FR.RSDNCY_SID, 2147483646) FIN_AID_FED_RES_SID,
	   nvl(SR.RSDNCY_SID, 2147483646) FIN_AID_ST_RES_SID,
       nvl(FE.RSDNCY_EXCPT_SID, 2147483646) FIN_AID_FED_EXCPT_SID,
       nvl(SE.RSDNCY_EXCPT_SID, 2147483646) FIN_AID_ST_EXCPT_SID,
       nvl(R.RSDNCY_SID, 2147483646) RESIDENCY_SID,
       TERM_BDGT_ITEM.ACAD_LEVEL_BOT,
       X1.XLATLONGNAME    ACAD_LEVEL_BOT_LD,       -- XLAT
       TERM_BDGT_ITEM.ACAD_LEVEL_PROJ,
       X2.XLATLONGNAME  ACAD_LEVEL_PROJ_LD,      -- XLAT
       TERM_BDGT_ITEM.ACADEMIC_LOAD,
       X3.XLATLONGNAME  ACADEMIC_LOAD_LD,        -- XLAT
       TERM_BDGT_ITEM.ACAD_LOAD_APPR,
       TERM_BDGT_ITEM.ACAD_PROG_PRIMARY,
       TERM_BDGT_ITEM.ACAD_PLAN,
       TERM_BDGT_ITEM.ACAD_SUB_PLAN,
       TERM_BDGT_ITEM.ACAD_PLAN_TYPE,
       TERM_BDGT_ITEM.AWARD_PERIOD,
       TERM_BDGT_ITEM.BUDGET_GROUP_CODE,
       GRP_TBL.DESCR BUDGET_GROUP_CODE_LD,
       TERM_BDGT_ITEM.DEPNDNCY_STAT,
       X4.XLATLONGNAME  DEPNDNCY_STAT_LD,        -- XLAT
       TERM_BDGT_ITEM.DESCRSHORT,
       TERM_BDGT_ITEM.FA_LOAD,
       X5.XLATLONGNAME  FA_LOAD_LD,              -- XLAT
       TERM_BDGT_ITEM.FA_LOAD_CURRENT,
       TERM_BDGT_ITEM.FA_TERM_EFFDT,
       TERM_BDGT_ITEM.FA_TERM_EFFSEQ,
       TERM_BDGT_ITEM.FA_TERM_WEEKS,
       TERM_BDGT_ITEM.FA_UNIT_ANTIC,
       TERM_BDGT_ITEM.FA_UNIT_COMPLETED,
       TERM_BDGT_ITEM.FA_UNIT_IN_PROG,
       TERM_BDGT_ITEM.FA_UNIT_CURRENT,
       TERM_BDGT_ITEM.FIN_AID_FED_RES,
       TERM_BDGT_ITEM.FIN_AID_ST_RES,
       TERM_BDGT_ITEM.FIN_AID_FED_EXCPT,
       TERM_BDGT_ITEM.FIN_AID_ST_EXCPT,
       TERM_BDGT_ITEM.FORM_OF_STUDY,
       X6.XLATLONGNAME  FORM_OF_STUDY_LD,        -- XLAT
       TERM_BDGT_ITEM.HOUSING_TYPE,
       X7.XLATLONGNAME  HOUSING_TYPE_LD,         -- XLAT
       TERM_BDGT_ITEM.MARITAL_STAT,
       X8.XLATLONGNAME  MARITAL_STAT_LD,         -- XLAT
       TERM_BDGT_ITEM.NSLDS_LOAN_YEAR,
       X9.XLATLONGNAME  NSLDS_LOAN_YEAR_LD,      -- XLAT
       TERM_BDGT_ITEM.NUMBER_IN_FAMILY,
       TERM_BDGT_ITEM.OPRID,
       TERM_BDGT_ITEM.POSTAL,
       TERM_BDGT_ITEM.PRORATE_BUDGET,
       TERM_BDGT_ITEM.RESIDENCY,
       TERM_BDGT_ITEM.STATE_RESIDENCE,
       TERM_BDGT_ITEM.APP_STATE_RESIDENC,
       nvl(T.TERM_BEGIN_DT,trunc(SYSDATE)) TERM_BEGIN_DT,
       nvl(T.TERM_END_DT,trunc(SYSDATE)) TERM_END_DT,
       CASE WHEN(TERM_BDGT_ITEM.PERSON_ID IS NULL) THEN ('N') ELSE('Y') END TERM_BUDGET_FLAG,
       TERM_BDGT_ITEM.TERM_TYPE,
       X10.XLATLONGNAME  TERM_TYPE_LD,            -- XLAT
       TERM_BDGT_ITEM.FED_TERM_COA,
       TERM_BDGT_ITEM.INST_TERM_COA,
       TERM_BDGT_ITEM.PELL_TERM_COA,
       TERM_BDGT_ITEM.SFA_PELTRM_COA_LHT,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  FROM CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR STAID
  LEFT OUTER JOIN TERM_BDGT_ITEM
    ON STAID.PERSON_ID = TERM_BDGT_ITEM.PERSON_ID
   AND STAID.INSTITUTION_CD = TERM_BDGT_ITEM.INSTITUTION_CD
   AND STAID.AID_YEAR = TERM_BDGT_ITEM.AID_YEAR
   AND STAID.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   AND TERM_BDGT_ITEM.BDGT_ORDER = 1
  LEFT OUTER JOIN CSSTG_OWNER.PS_BUDGET_GRP_TBL GRP_TBL
    ON STAID.INSTITUTION_CD = GRP_TBL.INSTITUTION
   AND STAID.AID_YEAR = GRP_TBL.AID_YEAR
   AND STAID.SRC_SYS_ID = GRP_TBL.SRC_SYS_ID
   AND TERM_BDGT_ITEM.ACAD_CAR_CD = GRP_TBL.ACAD_CAREER
   AND TERM_BDGT_ITEM.TERM_CD = GRP_TBL.STRM
   AND TERM_BDGT_ITEM.BUDGET_GROUP_CODE = GRP_TBL.BUDGET_GROUP_CODE
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
    on STAID.INSTITUTION_CD = I.INSTITUTION_CD
   and STAID.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on STAID.INSTITUTION_CD = C.INSTITUTION_CD
   and TERM_BDGT_ITEM.ACAD_CAR_CD = C.ACAD_CAR_CD
   and STAID.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_TERM T
    on STAID.INSTITUTION_CD = T.INSTITUTION_CD
   and TERM_BDGT_ITEM.ACAD_CAR_CD = T.ACAD_CAR_CD
   and TERM_BDGT_ITEM.TERM_CD = T.TERM_CD
   and STAID.SRC_SYS_ID = T.SRC_SYS_ID
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_PERSON P
    ON STAID.PERSON_ID = P.PERSON_ID
   AND STAID.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join CSMRT_OWNER.UM_D_ACAD_PROG G
    on TERM_BDGT_ITEM.INSTITUTION_CD = G.INSTITUTION_CD
   and TERM_BDGT_ITEM.ACAD_PROG_PRIMARY = G.ACAD_PROG_CD
   and TERM_BDGT_ITEM.SRC_SYS_ID = G.SRC_SYS_ID
   and G.EFFDT_ORDER = 1
  left outer join CSMRT_OWNER.UM_D_ACAD_PLAN PL
	on TERM_BDGT_ITEM.INSTITUTION_CD = PL.INSTITUTION_CD
   and TERM_BDGT_ITEM.ACAD_PLAN = PL.ACAD_PLAN_CD
   and TERM_BDGT_ITEM.SRC_SYS_ID = PL.SRC_SYS_ID
   and PL.EFFDT_ORDER = 1
  left outer join CSMRT_OWNER.UM_D_ACAD_SPLAN SP
    on TERM_BDGT_ITEM.INSTITUTION_CD = SP.INSTITUTION_CD
   and TERM_BDGT_ITEM.ACAD_PLAN = SP.ACAD_PLAN_CD
   and TERM_BDGT_ITEM.ACAD_SUB_PLAN = SP.ACAD_SPLAN_CD
   and TERM_BDGT_ITEM.SRC_SYS_ID = SP.SRC_SYS_ID
   and SP.EFFDT_ORDER = 1
   and SP.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_RSDNCY FR
    on TERM_BDGT_ITEM.FIN_AID_FED_RES = FR.RSDNCY_ID
   and TERM_BDGT_ITEM.SRC_SYS_ID = FR.SRC_SYS_ID
   and FR.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_RSDNCY SR
    on TERM_BDGT_ITEM.FIN_AID_ST_RES = SR.RSDNCY_ID
   and TERM_BDGT_ITEM.SRC_SYS_ID = SR.SRC_SYS_ID
   and SR.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_RSDNCY_EXCPT FE
    on TERM_BDGT_ITEM.FIN_AID_FED_EXCPT = FE.RSDNCY_EXCPTN
   and TERM_BDGT_ITEM.SRC_SYS_ID = FE.SRC_SYS_ID
   and FE.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_RSDNCY_EXCPT SE
    on TERM_BDGT_ITEM.FIN_AID_ST_EXCPT = SE.RSDNCY_EXCPTN
   and TERM_BDGT_ITEM.SRC_SYS_ID = SE.SRC_SYS_ID
   and SE.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_RSDNCY R
    on TERM_BDGT_ITEM.RESIDENCY = R.RSDNCY_ID
   and TERM_BDGT_ITEM.SRC_SYS_ID = R.SRC_SYS_ID
   and R.DATA_ORIGIN <> 'D'
  left outer join X X1
    on X1.FIELDNAME = 'ACADEMIC_LEVEL'
   and X1.FIELDVALUE = TERM_BDGT_ITEM.ACAD_LEVEL_BOT
   and X1.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X1.X_ORDER = 1
  left outer join X X2
    on X2.FIELDNAME = 'ACAD_LEVEL_PROJ'
   and X2.FIELDVALUE = TERM_BDGT_ITEM.ACAD_LEVEL_PROJ
   and X2.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X2.X_ORDER = 1
  left outer join X X3
    on X3.FIELDNAME = 'ACADEMIC_LOAD'
   and X3.FIELDVALUE = TERM_BDGT_ITEM.ACADEMIC_LOAD
   and X3.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X3.X_ORDER = 1
  left outer join X X4
    on X4.FIELDNAME = 'DEPNDNCY_STAT'
   and X4.FIELDVALUE = TERM_BDGT_ITEM.DEPNDNCY_STAT
   and X4.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X4.X_ORDER = 1
  left outer join X X5
    on X5.FIELDNAME = 'FA_LOAD'
   and X5.FIELDVALUE = TERM_BDGT_ITEM.FA_LOAD
   and X5.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X5.X_ORDER = 1
  left outer join X X6
    on X6.FIELDNAME = 'FORM_OF_STUDY'
   and X6.FIELDVALUE = TERM_BDGT_ITEM.FORM_OF_STUDY
   and X6.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X6.X_ORDER = 1
  left outer join X X7
    on X7.FIELDNAME = 'HOUSING_TYPE'
   and X7.FIELDVALUE = TERM_BDGT_ITEM.HOUSING_TYPE
   and X7.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X7.X_ORDER = 1
  left outer join X X8
    on X8.FIELDNAME = 'MARITAL_STAT'
   and X8.FIELDVALUE = TERM_BDGT_ITEM.MARITAL_STAT
   and X8.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X8.X_ORDER = 1
  left outer join X X9
    on X9.FIELDNAME = 'NSLDS_LOAN_YEAR'
   and X9.FIELDVALUE = TERM_BDGT_ITEM.NSLDS_LOAN_YEAR
   and X9.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X9.X_ORDER = 1
  left outer join X X10
    on X10.FIELDNAME = 'TERM_TYPE'
   and X10.FIELDVALUE = TERM_BDGT_ITEM.TERM_TYPE
   and X10.SRC_SYS_ID = TERM_BDGT_ITEM.SRC_SYS_ID
   and X10.X_ORDER = 1
   and T.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_BDGT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_BDGT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_BDGT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_BDGT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_BDGT enable constraint PK_UM_F_FA_STDNT_BDGT';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_BDGT');

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

END UM_F_FA_STDNT_BDGT_P;
/
