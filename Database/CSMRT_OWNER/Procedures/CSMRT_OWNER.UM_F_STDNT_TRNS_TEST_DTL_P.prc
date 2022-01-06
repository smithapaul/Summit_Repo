CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_TRNS_TEST_DTL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_STDNT_TRNS_TEST_DTL from PeopleSoft table UM_F_STDNT_TRNS_TEST_DTL.
--
 --V01  SMT-xxxx 06/18/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_TRNS_TEST_DTL';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_TRNS_TEST_DTL', TRUE);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL disable constraint PK_UM_F_STDNT_TRNS_TEST_DTL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL';
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL
  with X as (
select /*+ inline parallel(8) */
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       GRP as (
select /*+ inline parallel(8) */
       EMPLID, ACAD_CAREER, INSTITUTION, MODEL_NBR, ARTICULATION_TERM, TRNSFR_EQVLNCY_GRP, TRNSFR_EQVLNCY_SEQ, SRC_SYS_ID,
       TEST_DT, TEST_ID, TEST_COMPONENT, DESCR, LS_DATA_SOURCE, SCORE, PERCENTILE, TRNSFR_EQVLNCY_CMP, TST_EQVLNCY
  from CSSTG_OWNER.PS_TRNS_TEST_DTL
 where DATA_ORIGIN <> 'D'
   and TRNSFR_EQVLNCY_SEQ = 1),
       TST as (
select /*+ inline parallel(8) */
       TEST_ID, SRC_SYS_ID,
       DESCR,
       row_number() over (partition by TEST_ID, SRC_SYS_ID
                              order by EFFDT desc) TST_ORDER
  from CSSTG_OWNER.PS_SA_TEST_TBL
 where DATA_ORIGIN <> 'D'),
       SCH as (
select /*+ inline parallel(8) */
       SETID, GRADING_SCHEME, SRC_SYS_ID, DESCR, DESCRSHORT,
       row_number() over (partition by SETID, GRADING_SCHEME, SRC_SYS_ID order by SETID, GRADING_SCHEME, SRC_SYS_ID, EFFDT desc) SCH_ORDER
  from CSSTG_OWNER.PS_GRADESCHEME_TBL
 where DATA_ORIGIN <> 'D'),
       CMP as (
select /*+ inline parallel(8) */
       TEST_ID, TEST_COMPONENT, SRC_SYS_ID,
       DESCR,
       row_number() over (partition by TEST_ID, TEST_COMPONENT, SRC_SYS_ID
                              order by EFFDT desc) CMP_ORDER
  from CSSTG_OWNER.PS_SA_TCMP_REL_TBL
 where DATA_ORIGIN <> 'D'),
       DES as (
select /*+ inline parallel(8) */
       RQMNT_DESIGNTN, SRC_SYS_ID,
       DESCR,
       row_number() over (partition by RQMNT_DESIGNTN, SRC_SYS_ID
                              order by EFFDT desc) DES_ORDER
  from CSSTG_OWNER.PS_RQMNT_DESIG_TBL
 where DATA_ORIGIN <> 'D')
select /*+ parallel(8) */
       T.TERM_SID ARTICULATION_TERM_SID,
       P.PERSON_SID,
       S.MODEL_NBR,
       S.TRNSFR_EQVLNCY_GRP,
       S.TRNSFR_EQVLNCY_SEQ,
       S.SRC_SYS_ID,
       S.INSTITUTION,
       S.ACAD_CAREER,
       S.ARTICULATION_TERM,
       S.EMPLID,
       T.INSTITUTION_SID,
       T.ACAD_CAR_SID,
       nvl(C.CRSE_SID,2147483646) CRSE_SID,
       nvl(O.PERSON_SID,2147483646) OPRID_SID,     -- Feb 2020
       S.CRSE_GRADE_OFF,
       GRP.DESCR,               -- Feb 2020
       S.EARN_CREDIT,
       S.FREEZE_REC_FL,
       S.GRADE_CATEGORY,
       S.GRADING_SCHEME,
       nvl(SCH.DESCRSHORT,'-') GRADING_SCHEME_SD,
       nvl(SCH.DESCR,'-') GRADING_SCHEME_LD,
       S.GRADING_BASIS,
       nvl(X1.XLATSHORTNAME,'-') GRADING_BASIS_SD,
       nvl(X1.XLATLONGNAME,'-') GRADING_BASIS_LD,
       S.GRD_PTS_PER_UNIT,
       S.INCLUDE_IN_GPA,
       S.INPUT_CHG_FL,
       GRP.LS_DATA_SOURCE,      -- Feb 2020
       S.OVRD_RSN,
       S.OVRD_TRCR_FL,
       GRP.PERCENTILE,          -- Feb 2020
       S.REJECT_REASON,
       S.REPEAT_CODE,
       S.RQMNT_DESIGNTN,
       nvl(DES.RQMNT_DESIGNTN,'-') RQMNT_DESIGNTN_LD,
       GRP.SCORE,               -- Feb 2020
       S.SSR_FAWI_INCL,
       GRP.TEST_DT,             -- Feb 2020
       GRP.TST_EQVLNCY,         -- Feb 2020
       GRP.TEST_ID,             -- Feb 2020
       nvl(TST.DESCR,'-') TEST_ID_LD,
       GRP.TEST_COMPONENT,      -- Feb 2020
       nvl(CMP.DESCR,'-') TEST_COMPONENT_LD,
       GRP.TRNSFR_EQVLNCY_CMP,    -- Feb 2020
       S.TRNSFR_STAT,
       nvl(X2.XLATSHORTNAME,'-') TRNSFR_STAT_SD,
       nvl(X2.XLATLONGNAME,'-') TRNSFR_STAT_LD,
       S.UNITS_ATTEMPTED,
       S.UNT_TRNSFR,
       S.VALID_ATTEMPT,
       S.COMMENTS,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  from CSSTG_OWNER.PS_TRNS_TEST_TERM TT
  join CSSTG_OWNER.PS_TRNS_TEST_DTL S
    on TT.EMPLID = S.EMPLID
   and TT.ACAD_CAREER = S.ACAD_CAREER
   and TT.INSTITUTION = S.INSTITUTION
   and TT.MODEL_NBR = S.MODEL_NBR
   and TT.ARTICULATION_TERM = S.ARTICULATION_TERM
   and TT.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
  join GRP
    on GRP.EMPLID = S.EMPLID
   and GRP.ACAD_CAREER = S.ACAD_CAREER
   and GRP.INSTITUTION = S.INSTITUTION
   and GRP.MODEL_NBR = S.MODEL_NBR
   and GRP.ARTICULATION_TERM = S.ARTICULATION_TERM
   and GRP.TRNSFR_EQVLNCY_GRP = S.TRNSFR_EQVLNCY_GRP
   and GRP.SRC_SYS_ID = S.SRC_SYS_ID
  join CSMRT_OWNER.PS_D_TERM T
    on S.INSTITUTION = T.INSTITUTION_CD
   and S.ACAD_CAREER = T.ACAD_CAR_CD
   and S.ARTICULATION_TERM = T.TERM_CD
   and S.SRC_SYS_ID = T.SRC_SYS_ID
  join PS_D_PERSON P
    on S.EMPLID = P.PERSON_ID
   and S.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join PS_D_PERSON O
    on substr(TT.OPRID,4) = O.PERSON_ID
   and TT.SRC_SYS_ID = O.SRC_SYS_ID
   and O.DATA_ORIGIN <> 'D'
  left outer join UM_D_CRSE C
    on S.CRSE_ID = C.CRSE_CD
   and S.CRSE_OFFER_NBR = C.CRSE_OFFER_NUM
   and S.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join TST
    on GRP.TEST_ID = TST.TEST_ID
   and GRP.SRC_SYS_ID = TST.SRC_SYS_ID
   and TST.TST_ORDER = 1
  left outer join SCH
    on S.INSTITUTION = SCH.SETID
   and S.GRADING_SCHEME = SCH.GRADING_SCHEME
   and S.SRC_SYS_ID = SCH.SRC_SYS_ID
   and SCH.SCH_ORDER = 1
  left outer join CMP
    on GRP.TEST_ID = CMP.TEST_ID
   and GRP.TEST_COMPONENT = CMP.TEST_COMPONENT
   and GRP.SRC_SYS_ID = CMP.SRC_SYS_ID
   and CMP.CMP_ORDER = 1
  left outer join DES
    on S.RQMNT_DESIGNTN = DES.RQMNT_DESIGNTN
   and S.SRC_SYS_ID = DES.SRC_SYS_ID
   and DES.DES_ORDER = 1
  left outer join X X1
    on S.GRADING_BASIS = X1.FIELDVALUE
   and S.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'GRADING_BASIS'
   and X1.X_ORDER = 1
  left outer join X X2
    on S.TRNSFR_STAT = X2.FIELDVALUE
   and S.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'TRNSFR_STAT'
   and X2.X_ORDER = 1
 where TT.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_TRNS_TEST_DTL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_TRNS_TEST_DTL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_TRNS_TEST_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_TRNS_TEST_DTL enable constraint PK_UM_F_STDNT_TRNS_TEST_DTL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_TRNS_TEST_DTL');

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

END UM_F_STDNT_TRNS_TEST_DTL_P;
/
