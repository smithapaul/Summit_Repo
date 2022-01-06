CREATE OR REPLACE PROCEDURE             "UM_F_STDNT_GRAD_TRACK_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_STDNT_GRAD_TRACK.
--
 --V01  Case: 80656  11/23/2020  James Doucette
--   
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_GRAD_TRACK';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_GRAD_TRACK');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK disable constraint PK_UM_F_STDNT_GRAD_TRACK';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK';				
insert /*+ append */ into UM_F_STDNT_GRAD_TRACK 
  with Q1 as (
select /*+ inline */ INSTITUTION, SSR_GRAD_STATUS, EFFDT, SRC_SYS_ID,
       EFF_STATUS, DESCR,
       row_number() over (partition by INSTITUTION, SSR_GRAD_STATUS, SRC_SYS_ID
                              order by EFFDT desc) Q1_ORDER
  from CSSTG_OWNER.PS_SSR_GRADSTATTBL
 where DATA_ORIGIN <> 'D'),
       Q2 as (
select /*+ inline parallel(16) */ HIST.EMPLID, HIST.INSTITUTION, HIST.ACAD_CAREER, HIST.STDNT_CAR_NBR, HIST.ACAD_PROG, HIST.EXP_GRAD_TERM, HIST.DEGREE, HIST.SSR_GRAD_REV_DTTM, HIST.SRC_SYS_ID,
       HIST.SSR_GRAD_STATUS, nvl(Q1.DESCR,'-') DESCR, HIST.STATUS_DT, HIST.OPRID_LAST_UPDT,
       row_number() over (partition by HIST.EMPLID, HIST.INSTITUTION, HIST.ACAD_CAREER, HIST.STDNT_CAR_NBR, HIST.ACAD_PROG, HIST.EXP_GRAD_TERM, HIST.DEGREE, HIST.SRC_SYS_ID
                              order by HIST.SSR_GRAD_REV_DTTM desc) Q2_ORDER
  from CSSTG_OWNER.PS_SSR_STDGRD_HIST HIST
  left outer join Q1
    on HIST.INSTITUTION = Q1.INSTITUTION
   and HIST.SSR_GRAD_STATUS = Q1.SSR_GRAD_STATUS
   and HIST.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q1_ORDER = 1
 where HIST.DATA_ORIGIN <> 'D'),
       Q3 as (
select /*+ inline parallel(16) */ EMPLID PERSON_ID, INSTITUTION INSTITUTION_CD, ACAD_CAREER ACAD_CAR_CD, STDNT_CAR_NBR STDNT_CAR_NUM, ACAD_PROG ACAD_PROG_CD, EXP_GRAD_TERM, DEGREE DEG_CD, SSR_GRAD_REV_DTTM, SRC_SYS_ID,
       SSR_GRAD_STATUS, DESCR, STATUS_DT, OPRID_LAST_UPDT, Q2_ORDER HIST_ORDER,
       decode(max(case when SSR_GRAD_STATUS not in ('VGRD','VOID') AND Q2_ORDER = 1 then EXP_GRAD_TERM else '0' end)
                  over (partition by EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, DEGREE, SRC_SYS_ID), EXP_GRAD_TERM,'Y','N') LATEST_TERM_FLG,
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from Q2)
select PERSON_ID,
       INSTITUTION_CD,
	   ACAD_CAR_CD,
	   STDNT_CAR_NUM,
	   ACAD_PROG_CD,
	   EXP_GRAD_TERM,
	   DEG_CD,
	   SSR_GRAD_REV_DTTM,
	   SRC_SYS_ID,
	   SSR_GRAD_STATUS,
	   DESCR,
	   STATUS_DT,
	   OPRID_LAST_UPDT,
	   HIST_ORDER,
	   LATEST_TERM_FLG,
	   DATA_ORIGIN,
	   CREATED_EW_DTTM,
	   LASTUPD_EW_DTTM
  from Q3
-- where Q2_ORDER = 1
;
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_GRAD_TRACK rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_GRAD_TRACK',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_GRAD_TRACK',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_GRAD_TRACK enable constraint PK_UM_F_STDNT_GRAD_TRACK';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_GRAD_TRACK');

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

END UM_F_STDNT_GRAD_TRACK_P;
/
