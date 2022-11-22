DROP PROCEDURE CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR_P
/

--
-- UM_F_STDNT_GRAD_TRACK_HONOR_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_STDNT_GRAD_TRACK_HONOR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_STDNT_GRAD_TRACK_HONOR.
--
 --V01  Case: 80656  11/23/2020  James Doucette
--
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_GRAD_TRACK_HONOR';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_GRAD_TRACK_HONOR');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR disable constraint PK_UM_F_STDNT_GRAD_TRACK_HONOR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR';

insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR
  with Q1 as (
select /*+ inline */ INSTITUTION, HONORS_TYPE, HONORS_CODE, EFFDT, SRC_SYS_ID,
       EFF_STATUS, DESCR,
       row_number() over (partition by INSTITUTION, HONORS_TYPE, HONORS_CODE, SRC_SYS_ID
                              order by EFFDT desc) Q1_ORDER
  from CSSTG_OWNER.PS_DEGR_HONORS_TBL
 where DATA_ORIGIN <> 'D')
select /*+ inline parallel(16) */ HONR.EMPLID PERSON_ID, HONR.INSTITUTION INSTITUTION_CD, HONR.ACAD_CAREER ACAD_CAR_CD, HONR.STDNT_CAR_NBR STDNT_CAR_NUM, HONR.ACAD_PROG ACAD_PROG_CD, HONR.EXP_GRAD_TERM, HONR.DEGREE DEG_CD, HONR.SEQNUM, HONR.SRC_SYS_ID,
       nvl(trim(HONR.HONORS_CODE),'-') HONORS_CODE, nvl(Q1.DESCR,'-') DESCR,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from CSSTG_OWNER.PS_SSR_STDGRD_HONR HONR
  left outer join Q1
    on HONR.INSTITUTION = Q1.INSTITUTION
   and Q1.HONORS_TYPE = 'DH'
   and HONR.HONORS_CODE = Q1.HONORS_CODE
   and HONR.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q1_ORDER = 1
 where HONR.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_GRAD_TRACK_HONOR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_GRAD_TRACK_HONOR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_GRAD_TRACK_HONOR',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK_HONOR enable constraint PK_UM_F_STDNT_GRAD_TRACK_HONOR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_GRAD_TRACK_HONOR');

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

END UM_F_STDNT_GRAD_TRACK_HONOR_P;
/
