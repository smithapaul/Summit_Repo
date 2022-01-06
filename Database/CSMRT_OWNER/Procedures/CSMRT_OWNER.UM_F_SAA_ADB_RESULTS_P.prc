CREATE OR REPLACE PROCEDURE             "UM_F_SAA_ADB_RESULTS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_SAA_ADB_RESULTS from PeopleSoft table UM_F_SAA_ADB_RESULTS.
--
 --V01  SMT-xxxx 06/19/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SAA_ADB_RESULTS';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_RESULTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SAA_ADB_RESULTS', TRUE);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_RESULTS disable constraint PK_UM_F_SAA_ADB_RESULTS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SAA_ADB_RESULTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_SAA_ADB_RESULTS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SAA_ADB_RESULTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SAA_ADB_RESULTS';				
insert /*+ append */ into UM_F_SAA_ADB_RESULTS
with XL as (select /*+ materialize */
                   FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
              from UM_D_XLATITEM
             where SRC_SYS_ID = 'CS90'),
 RT as (
select distinct EMPLID, ANALYSIS_DB_SEQ, SAA_CAREER_RPT, SRC_SYS_ID, INSTITUTION, SAA_RPT_DTTM_STAMP  
  from CSSTG_OWNER.PS_SAA_ADB_REPORT REP
),  
TERM as (
select INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, EFF_START_DT, EFF_END_DT, TERM_SID 
  from UM_D_TERM_VW
 where TERM_CD between '1010' and '9000'
   and substr(TERM_CD,-2,2) not in ('50','90')
   and EFF_START_DT < '01-JAN-9000'
),
 DT as (
select /*+ parallel(8) inline */
       distinct RT.EMPLID PERSON_ID, RT.ANALYSIS_DB_SEQ, RT.SAA_CAREER_RPT ACAD_CAR_CD, RT.SRC_SYS_ID, 
                RT.INSTITUTION INSTITUTION_CD, TERM.TERM_CD, TERM.TERM_SID
  from RT 
  left outer join TERM
    on RT.INSTITUTION = TERM.INSTITUTION_CD
   and RT.SAA_CAREER_RPT = TERM.ACAD_CAR_CD
   and trunc(RT.SAA_RPT_DTTM_STAMP) between TERM.EFF_START_DT and TERM.EFF_END_DT
),
 ST as (
select /*+ parallel(8) inline */ 
       distinct INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, PERSON_ID, TERM_SID
  from UM_F_STDNT_TERM ST
 where TERM_ACTV_FLG = 'Y'
),
 MT as (
select /*+ parallel(8) inline */
       DT.PERSON_ID, DT.ANALYSIS_DB_SEQ, DT.ACAD_CAR_CD, DT.SRC_SYS_ID, 
       DT.INSTITUTION_CD, DT.TERM_CD||DT.TERM_SID DT_TERM, min(ST.TERM_CD||ST.TERM_SID) MIN_ST_TERM  
  from DT
  left outer join ST 
    on DT.INSTITUTION_CD = ST.INSTITUTION_CD
   and DT.ACAD_CAR_CD = ST.ACAD_CAR_CD
   and DT.TERM_CD <= ST.TERM_CD
   and DT.PERSON_ID = ST.PERSON_ID
 group by DT.PERSON_ID, DT.ANALYSIS_DB_SEQ, DT.ACAD_CAR_CD, DT.SRC_SYS_ID,
          DT.INSTITUTION_CD, DT.TERM_CD||DT.TERM_SID
),
MT2 as (
select /*+ parallel(8) inline */
       PERSON_ID, ANALYSIS_DB_SEQ, ACAD_CAR_CD, SRC_SYS_ID, 
       coalesce(to_number(trim(substr(MIN_ST_TERM,5,9))), to_number(trim(substr(DT_TERM,5,9)))) TERM_SID  
  from MT
)
select /*+ parallel(8) inline */ 
REP.EMPLID,
REP.ANALYSIS_DB_SEQ,
REP.SAA_CAREER_RPT,
RES.SAA_ENTRY_SEQ,
--CRSA.SAA_COURSE_SEQ,
REP.SRC_SYS_ID,
REP.INSTITUTION,
I.INSTITUTION_SID,
C.ACAD_CAR_SID,
nvl(MT2.TERM_SID,2147483646) TERM_SID,
nvl(P.PERSON_SID,2147483646) PERSON_SID,
nvl(RG.RQRMNT_GROUP_SID,2147483646) RQRMNT_GROUP_SID,
nvl(RQ.REQUIREMENT_SID,2147483646) REQUIREMENT_SID,
nvl(RL.RQRMNT_LINE_SID,2147483646) RQRMNT_LINE_SID,
nvl(PR.ACAD_PROG_SID,2147483646) ACAD_PROG_SID,
nvl(PL.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID,
nvl(SP.ACAD_SPLAN_SID,2147483646) ACAD_SPLAN_SID,
REP.RPT_DATE,
REP.RPT_TYPE,
nvl(X1.XLATSHORTNAME,'-') RPT_TYPE_SD, 
nvl(X1.XLATLONGNAME,'-') RPT_TYPE_LD,
REP.SAA_RPT_IDENTIFIER,     -- New dim PS_SAA_IDENT_TBL? 
REP.TSCRPT_TYPE,            -- New dim PS_TRANSCRIPT_TYPE? 
REP.SAA_RPT_DTTM_STAMP,
RES.ENTRY_R_TYPE,
RES.ITEM_R_STATUS,
nvl(X3.XLATSHORTNAME,'-') ITEM_R_STATUS_SD, 
nvl(X3.XLATLONGNAME,'-') ITEM_R_STATUS_LD,
RES.RQ_DATE,
RES.RQRMNT_LIST_SEQ,
RES.REQ_LINE_TYPE,
nvl(X2.XLATSHORTNAME,'-') REQ_LINE_TYPE_SD, 
nvl(X2.XLATLONGNAME,'-') REQ_LINE_TYPE_LD,
RES.UNITS_REQUIRED,
RES.SAA_UNITS_USED, 
RES.UNITS_NEEDED,
RES.CRSES_REQUIRED,
RES.SAA_CRSES_USED,
RES.CRSES_NEEDED,
RES.SAA_CRSE_COUNT,
RES.GPA_REQUIRED,
RES.GPA_ACTUAL,
SYSDATE CREATED_EW_DTTM
  from CSSTG_OWNER.PS_SAA_ADB_REPORT REP
  join MT2
    on REP.EMPLID = MT2.PERSON_ID
   and REP.ANALYSIS_DB_SEQ = MT2.ANALYSIS_DB_SEQ
   and REP.SAA_CAREER_RPT = MT2.ACAD_CAR_CD
   and REP.SRC_SYS_ID = MT2.SRC_SYS_ID
  join CSSTG_OWNER.PS_SAA_ADB_RESULTS RES
    on REP.EMPLID = RES.EMPLID
   and REP.ANALYSIS_DB_SEQ = RES.ANALYSIS_DB_SEQ
   and REP.SAA_CAREER_RPT = RES.SAA_CAREER_RPT
  join PS_D_INSTITUTION I
    on REP.INSTITUTION = I.INSTITUTION_CD
   and REP.SRC_SYS_ID = I.SRC_SYS_ID
  join PS_D_ACAD_CAR C
    on REP.INSTITUTION = C.INSTITUTION_CD
   and REP.SAA_CAREER_RPT = C.ACAD_CAR_CD
   and REP.SRC_SYS_ID = C.SRC_SYS_ID
  left outer join PS_D_PERSON P
    on REP.EMPLID = P.PERSON_ID
   and REP.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join UM_D_RQ_GRP RG
    on RES.RQRMNT_GROUP = RG.RQRMNT_GROUP
   and RES.SRC_SYS_ID = RG.SRC_SYS_ID
  left outer join UM_D_RQ RQ
    on RES.REQUIREMENT = RQ.REQUIREMENT
   and RES.SRC_SYS_ID = RQ.SRC_SYS_ID
  left outer join UM_D_RQ_LINE RL
    on RES.REQUIREMENT = RL.REQUIREMENT
   and RES.RQ_LINE_NBR = RL.RQ_LINE_NBR
   and RES.SRC_SYS_ID = RQ.SRC_SYS_ID
  left outer join UM_D_ACAD_PROG PR
    on REP.INSTITUTION = PR.INSTITUTION_CD
   and RES.ACAD_PROG = PR.ACAD_PROG_CD
   and RES.SRC_SYS_ID = PR.SRC_SYS_ID
   and PR.EFFDT_ORDER = 1 
  left outer join UM_D_ACAD_PLAN PL
    on REP.INSTITUTION = PL.INSTITUTION_CD
   and RES.ACAD_PLAN = PL.ACAD_PLAN_CD
   and RES.SRC_SYS_ID = PL.SRC_SYS_ID
   and PL.EFFDT_ORDER = 1 
  left outer join UM_D_ACAD_SPLAN SP
    on REP.INSTITUTION = SP.INSTITUTION_CD
   and RES.ACAD_PLAN = SP.ACAD_PLAN_CD
   and RES.ACAD_SUB_PLAN = SP.ACAD_SPLAN_CD
   and RES.SRC_SYS_ID = SP.SRC_SYS_ID
   and SP.EFFDT_ORDER = 1 
  left outer join XL X1
    on X1.FIELDNAME = 'RPT_TYPE'
   and X1.FIELDVALUE = REP.RPT_TYPE 
   and X1.SRC_SYS_ID = REP.SRC_SYS_ID
  left outer join XL X2
    on X2.FIELDNAME = 'REQ_LINE_TYPE'
   and X2.FIELDVALUE = RES.REQ_LINE_TYPE 
   and X2.SRC_SYS_ID = RES.SRC_SYS_ID
  left outer join XL X3
    on X3.FIELDNAME = 'ITEM_R_STATUS'
   and X3.FIELDVALUE = RES.ITEM_R_STATUS 
   and X3.SRC_SYS_ID = RES.SRC_SYS_ID
 where trunc(REP.SAA_RPT_DTTM_STAMP) >= SYSDATE-90
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SAA_ADB_RESULTS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_RESULTS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_RESULTS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_RESULTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_RESULTS enable constraint PK_UM_F_SAA_ADB_RESULTS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SAA_ADB_RESULTS');

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

END UM_F_SAA_ADB_RESULTS_P;
/
