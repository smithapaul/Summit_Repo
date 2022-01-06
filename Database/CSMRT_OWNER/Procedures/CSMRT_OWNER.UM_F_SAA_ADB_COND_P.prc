CREATE OR REPLACE PROCEDURE             "UM_F_SAA_ADB_COND_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_F_SAA_ADB_COND from PeopleSoft table UM_F_SAA_ADB_COND.
--
 --V01  SMT-xxxx 06/19/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SAA_ADB_COND';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_COND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SAA_ADB_COND', TRUE);


strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_COND disable constraint PK_UM_F_SAA_ADB_COND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SAA_ADB_COND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_SAA_ADB_COND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SAA_ADB_COND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SAA_ADB_COND';				
insert /*+ append */ into UM_F_SAA_ADB_COND
with RES as (
select /*+ parallel(8) inline */ distinct
       R.EMPLID, R.ANALYSIS_DB_SEQ, R.SAA_CAREER_RPT, R.SRC_SYS_ID, 
       R.INSTITUTION_CD, R.INSTITUTION_SID, R.ACAD_CAR_SID, R.TERM_SID, R.PERSON_SID, RPT_DATE
  from UM_F_SAA_ADB_RESULTS R
 where R.CRSES_REQUIRED < 999
   and R.ITEM_R_STATUS in ('FAIL')
   and R.TSCRPT_TYPE in ('DADV','LADV')), 
TERM as (
select /*+ parallel(8) inline */
       INSTITUTION_CD, ACAD_CAR_CD, TERM_CD, EFF_START_DT, EFF_END_DT, TERM_SID 
  from UM_D_TERM_VW
 where TERM_CD between '1010' and '9000'
   and substr(TERM_CD,-2,2) not in ('50','90')
   and EFF_START_DT < '01-JAN-9000'),
RES2 as (
select /*+ parallel(8) inline */
       RES.EMPLID, RES.ANALYSIS_DB_SEQ, RES.SAA_CAREER_RPT, RES.SRC_SYS_ID, 
       RES.INSTITUTION_CD, RES.INSTITUTION_SID, RES.ACAD_CAR_SID, RES.TERM_SID, RES.PERSON_SID, RPT_DATE,
       TERM.TERM_CD
  from RES
  join TERM
    on RES.TERM_SID = TERM.TERM_SID),
COURSES as (
select /*+ parallel(8) inline */ distinct 
       EMPLID, ANALYSIS_DB_SEQ, SAA_CAREER_RPT, SRC_SYS_ID, 
       CRSE_ID,
       EARN_CREDIT      -- Need???    
  from CSSTG_OWNER.PS_SAA_ADB_COURSES
 where nvl(trim(EARN_CREDIT),'-') = 'Y'
   and nvl(trim(VALID_ATTEMPT),'-') = 'Y'
--   and nvl(trim(CRSE_GRADE_OFF),'-') not in ('-','INC','W')     -- Should we exlcude any grades??? 
),
GRP as (
select /*+ parallel(8) inline */
       EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, STDNT_GROUP STDNT_GRP_CD, EFFDT, SRC_SYS_ID,
       EFF_STATUS EFF_STAT_CD,
       nvl(min(EFFDT-1) over (partition by STDNT_GROUP, EMPLID, SRC_SYS_ID
                                  order by EFFDT
                              rows between 1 following and unbounded following),to_date('31-DEC-9999','DD-MON-YYYY')) EFF_END_DT
  from CSSTG_OWNER.PS_STDNT_GRPS_HIST
 where DATA_ORIGIN <> 'D'
),
COND as (
select /*+ parallel(8) inline */ distinct 
       C.EMPLID, 
       C.ANALYSIS_DB_SEQ, 
       C.SAA_CAREER_RPT, 
       C.SRC_SYS_ID, 
       'CRSE' COND_CODE,  
       C.CRSE_ID COND_DATA  
  from COURSES C
  join RES2
    on C.EMPLID = RES2.EMPLID
   and C.ANALYSIS_DB_SEQ = RES2.ANALYSIS_DB_SEQ
   and C.SAA_CAREER_RPT = RES2.SAA_CAREER_RPT
   and C.SRC_SYS_ID = RES2.SRC_SYS_ID
 union all
select /*+ parallel(8) inline */ distinct 
       T.EMPLID, 
       RES2.ANALYSIS_DB_SEQ, 
       RES2.SAA_CAREER_RPT, 
       T.SRC_SYS_ID, 
       'LVL' COND_CODE,  
       L.ACAD_LVL_CD COND_DATA
  from CSSTG_OWNER.PS_STDNT_CAR_TERM T
  join RES2 
    on T.INSTITUTION = RES2.INSTITUTION_CD
   and T.ACAD_CAREER = RES2.SAA_CAREER_RPT
   and T.EMPLID = RES2.EMPLID
   and T.SRC_SYS_ID = RES2.SRC_SYS_ID
   and T.STRM <= RES2.TERM_CD
   and T.DATA_ORIGIN <> 'D'
   and T.ACAD_LEVEL_EOT <> '-'
  join PS_D_ACAD_LVL L
    on L.ACAD_LVL_CD in ('10','20','30','40','GR')
   and L.ACAD_LVL_CD <= T.ACAD_LEVEL_EOT
   and T.SRC_SYS_ID = L.SRC_SYS_ID
 union all
select /*+ parallel(8) inline */ distinct 
       RES2.EMPLID, 
       RES2.ANALYSIS_DB_SEQ, 
       RES2.SAA_CAREER_RPT, 
       RES2.SRC_SYS_ID, 
       'GRP' COND_CODE,  
       GRP.STDNT_GRP_CD COND_DATA
  from GRP
  join RES2 
    on GRP.INSTITUTION_CD = RES2.INSTITUTION_CD
   and GRP.PERSON_ID = RES2.EMPLID
   and GRP.SRC_SYS_ID = RES2.SRC_SYS_ID
   and RES2.RPT_DATE between GRP.EFFDT and GRP.EFF_END_DT 
   and GRP.STDNT_GRP_CD <> '-'
   and GRP.EFF_STAT_CD = 'A'
 union all
select /*+ parallel(8) inline */ distinct 
       G.PERSON_ID, 
       RES2.ANALYSIS_DB_SEQ, 
       RES2.SAA_CAREER_RPT, 
       G.SRC_SYS_ID, 
       'PR' COND_CODE,  
       G.ACAD_PROG_CD COND_DATA
  from CSMRT_OWNER.UM_F_ACAD_PROG G
  join RES2 
    on G.INSTITUTION_CD = RES2.INSTITUTION_CD
   and G.ACAD_CAR_CD = RES2.SAA_CAREER_RPT
   and G.PERSON_ID = RES2.EMPLID
   and G.SRC_SYS_ID = RES2.SRC_SYS_ID
   and G.TERM_CD = RES2.TERM_CD
 union all
select /*+ parallel(8) inline */ distinct 
       L.PERSON_ID, 
       RES2.ANALYSIS_DB_SEQ, 
       RES2.SAA_CAREER_RPT, 
       L.SRC_SYS_ID, 
       'PL' COND_CODE,  
       L.ACAD_PLAN_CD COND_DATA
  from CSMRT_OWNER.UM_F_ACAD_PLAN L
  join RES2 
    on L.INSTITUTION_CD = RES2.INSTITUTION_CD
   and L.ACAD_CAR_CD = RES2.SAA_CAREER_RPT
   and L.PERSON_ID = RES2.EMPLID
   and L.SRC_SYS_ID = RES2.SRC_SYS_ID
   and L.TERM_CD = RES2.TERM_CD
   and L.ACAD_PLAN_CD <> '-'
 union all
select /*+ parallel(8) inline */ distinct 
       S.PERSON_ID, 
       RES2.ANALYSIS_DB_SEQ, 
       RES2.SAA_CAREER_RPT, 
       S.SRC_SYS_ID, 
       'SPL' COND_CODE,  
       S.ACAD_SPLAN_CD COND_DATA
  from CSMRT_OWNER.UM_F_ACAD_PLAN S
  join RES2 
    on S.INSTITUTION_CD = RES2.INSTITUTION_CD
   and S.ACAD_CAR_CD = RES2.SAA_CAREER_RPT
   and S.PERSON_ID = RES2.EMPLID
   and S.SRC_SYS_ID = RES2.SRC_SYS_ID
   and S.TERM_CD = RES2.TERM_CD
   and S.ACAD_SPLAN_CD <> '-')
select /*+ parallel(8) inline */
       RES.EMPLID, RES.ANALYSIS_DB_SEQ, RES.SAA_CAREER_RPT, COND.COND_CODE, COND.COND_DATA, RES.SRC_SYS_ID, 
       RES.INSTITUTION_CD, --RES.INSTITUTION_SID, RES.ACAD_CAR_SID, RES.TERM_SID, RES.PERSON_SID, 
       SYSDATE CREATED_EW_DTTM
  from RES
  join COND
    on RES.EMPLID = COND.EMPLID
   and RES.ANALYSIS_DB_SEQ = COND.ANALYSIS_DB_SEQ 
   and RES.SAA_CAREER_RPT = COND.SAA_CAREER_RPT
   and RES.SRC_SYS_ID = COND.SRC_SYS_ID
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SAA_ADB_COND rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_COND',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );



strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SAA_ADB_COND',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SAA_ADB_COND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SAA_ADB_COND enable constraint PK_UM_F_SAA_ADB_COND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SAA_ADB_COND');

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

END UM_F_SAA_ADB_COND_P;
/
